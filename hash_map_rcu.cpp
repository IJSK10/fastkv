#include "hash_map_rcu.h"
#include <iostream>
#include <algorithm>
#include <vector>
#include "persistence.h"

inline void HashMap::incrementRefCount(Node* node) const{
    if (node) {
        node->refCount.fetch_add(1, std::memory_order_relaxed);
    }
}

inline void HashMap::decrementRefCount(Node* node) const{
    if (node) {
        node->refCount.fetch_sub(1, std::memory_order_release);
    }
}

HashMap:: HashMap(const std::string &persistenceFile) : capacity(INITIAL_CAPACITY), size(0), running(true), lruRunning(true), workersRunning(true)
{
    table = std::vector<std::atomic<Node*>>(this->capacity);

    unsigned int num_workers = std::thread::hardware_concurrency();
    if (num_workers == 0) num_workers = 2;

    for (int i=0;i<std::thread::hardware_concurrency();++i)
    {
        workerThread.emplace_back(&HashMap::workerFunction,this);
    }
    
    cleanupThread = std::thread(&HashMap::cleanupExpired, this);
    lruThread = std::thread(&HashMap::lruMonitor,this);
    if (!persistenceFile.empty())
    {
        try{
            Persistence::loadFromFile(*this,persistenceFile);
        }
        catch (const std::exception& e)
        {
            std::cerr<<"Failed to load from persistence file" <<e.what() <<std::endl;
        }
    }
}

HashMap::~HashMap()
{

    running = false;
    lruRunning = false;
    workersRunning = false;

    taskCV.notify_all();
    expiryGlobalCV.notify_all();
    lruCV.notify_all();
    
    for (auto &t: workerThread)
    {
        if (t.joinable()) t.join();
    }
    if (cleanupThread.joinable()) cleanupThread.join();

    if (lruThread.joinable()) lruThread.join();


    try {
        Persistence::saveToFile(*this, "hashmap.json");
    }
    catch (const std::exception& e)
    {
        std::cerr << "Falied to save to persistence file:"<<e.what() <<std::endl;
    }

    processDeferredDeletes(true);

    for (size_t i = 0; i < table.size(); ++i) {
        deleteList(table[i].load(std::memory_order_relaxed));
        table[i].store(nullptr, std::memory_order_relaxed);
    }
}

void HashMap::deleteList(Node* head) {
    Node* current = head;
    while (current) {
        Node* temp = current;
        current = current->next.load(std::memory_order_relaxed);
        delete temp;
    }
}

void HashMap::scheduleDeferredDelete(Node* node) {
    if (!node) return;
    std::lock_guard<std::mutex> lock(deferredDeleteMutex);
    deferredDeleteQueue.push_back(node);
}

void HashMap::processDeferredDeletes(bool forceAll) {
    std::vector<Node*> toDeleteNow;
    {
        std::lock_guard<std::mutex> lock(deferredDeleteMutex);
        if (deferredDeleteQueue.empty()) return;

        deferredDeleteQueue.erase(
            std::remove_if(deferredDeleteQueue.begin(), deferredDeleteQueue.end(),
                           [&](Node* node) {
                               if (node && node->refCount.load(std::memory_order_acquire) == 0) {
                                   toDeleteNow.push_back(node);
                                   return true;
                               }
                               if (forceAll && node && node->refCount.load(std::memory_order_acquire) > 0) {
                                   std::cerr << "Warning: Node " << node->key << " has refCount "
                                            << node->refCount.load() << " during forced delete processing." << std::endl;
                               }
                               return false;
                           }),
            deferredDeleteQueue.end());
    }

    for (Node* node : toDeleteNow) {
        delete node;
    }
}

void HashMap::workerFunction() {
    while (workersRunning.load(std::memory_order_relaxed)) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(taskMutex);
            taskCV.wait(lock, [&]() {
                return !taskQueue.empty() || !workersRunning;
            });
            if (!workersRunning.load(std::memory_order_relaxed) && taskQueue.empty()) {
                break;
            }
            if (taskQueue.empty()) { 
                continue;
            }
            task = taskQueue.front();
            taskQueue.pop();
        }
        if (task)
        {
            try {
                switch (task->type) {
                    case TaskType::SET:
                        setInternal(task->key, task->value, task->ttl);
                        // task->result.set_value("OK");
                        break;
                    case TaskType::GET:
                        task->result.set_value(getInternal(task->key));
                        break;
                    case TaskType::REMOVE:
                        task->result.set_value(removeInternal(task->key) ? "true" : "false");
                        break;
                }
            } catch (const std::exception& e) {
                try {
                    task->result.set_exception(std::current_exception());
                } catch (...) {

                }
            }
        }
    }
}

void HashMap::set(const std::string &key, const std::string &value, int ttl)
{
    auto task=std::make_shared<Task>(TaskType::SET, key, value, ttl);
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();
}

std::string HashMap::get(const std::string &key)
{
    auto task = std::make_shared<Task>(TaskType::GET, key);
    std::future<std::string> future = task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();
    try {
        if (future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {

            return "Error: Timeout";
        }
        return future.get();
    } catch (const std::exception& e) { 
        return "Error: " + std::string(e.what());
    }
}

bool HashMap::remove(const std::string &key)
{
    auto task=std::make_shared<Task>(TaskType::REMOVE, key);
    std::future<std::string> future = task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();
    try {
        if (future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
            return false;
        }
        return future.get() == "true";
    } catch (const std::exception& e) {
        return false;
    }
}

void HashMap::setInternal(const std::string &key, const std::string &val,int ttl)
{
    size_t index=hashFunction(key,capacity);
    time_t expiry = ttl ? time(nullptr) + ttl : 0;

    Node* new_node = new Node(key, val, expiry);

    Node* current_head;
    Node* old_node_to_retire = nullptr;
    
    do {
        current_head = table[index].load(std::memory_order_acquire);
        Node* current = current_head;
        Node* prev = nullptr;
        old_node_to_retire = nullptr;

        while (current != nullptr) {
            if (current->key == key) {
                old_node_to_retire = current; 
                break;
            }
            prev = current;
            current = current->next.load(std::memory_order_acquire);
        }

        if (old_node_to_retire) {
            new_node->next.store(old_node_to_retire->next.load(std::memory_order_relaxed), std::memory_order_relaxed);
        } else {
            new_node->next.store(current_head, std::memory_order_relaxed);
        }

        if (old_node_to_retire == current_head) {
            if (table[index].compare_exchange_weak(current_head, new_node,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
                if (old_node_to_retire) {
                     scheduleDeferredDelete(old_node_to_retire);
                }
                updateLRU(key);
                return;
            }
        }

        else if (old_node_to_retire == nullptr) {
            if (table[index].compare_exchange_weak(current_head, new_node,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
                size.fetch_add(1, std::memory_order_relaxed);
                updateLRU(key);
                return;
            }
        }

        else { 

            if (prev && prev->next.compare_exchange_weak(old_node_to_retire, new_node,
                                                        std::memory_order_release, std::memory_order_relaxed)) {
                scheduleDeferredDelete(old_node_to_retire);
                updateLRU(key);
                return;
            }
        }

        } while (true);
}

std::string HashMap::getInternal(const std::string &key) const
{
    size_t index=hashFunction(key, capacity);
    std::string result_value = "Key not found";
    time_t now = 0;
    Node* current = table[index].load(std::memory_order_acquire);
    incrementRefCount(current);

    while (current!=nullptr)
    {
        if (current->key == key)
        {
            if (now == 0) now = time(nullptr);
            if (current->expiry!=0 && now> current->expiry)
            {
                result_value = "Key expired";
            }
            else {
                current->lastAccessed.store(now, std::memory_order_relaxed);
                result_value = current->value;
            }
            decrementRefCount(current);
            return result_value;
        }

        Node* nextNode = current->next.load();
        incrementRefCount(nextNode); 
        decrementRefCount(current); 
        current = nextNode;
    }
    return result_value;
}

bool HashMap::removeInternal(const std::string & key)
{
    size_t index = hashFunction(key,capacity);

    Node* node_to_retire = nullptr;
    Node* current_head;

    do {
        current_head = table[index].load(std::memory_order_acquire);
        Node* current = current_head;
        Node* prev = nullptr;
        node_to_retire = nullptr;

        while (current != nullptr) {
            if (current->key == key) {
                node_to_retire = current;
                break;
            }
            prev = current;
            current = current->next.load(std::memory_order_acquire);

        }

        if (node_to_retire == nullptr) {
            return false;
        }

        Node* next_after_removed = node_to_retire->next.load(std::memory_order_relaxed);
        if (node_to_retire == current_head) { 
            if (table[index].compare_exchange_weak(current_head, next_after_removed,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {

                scheduleDeferredDelete(node_to_retire);
                size.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        } else {
            if (prev && prev->next.compare_exchange_weak(node_to_retire, next_after_removed,
                                                       std::memory_order_release,
                                                       std::memory_order_relaxed)) {                               
                scheduleDeferredDelete(node_to_retire);
                size.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        }
    } while (true); 
    return false;                                     
}

void HashMap::cleanupExpired() {
    while (running.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lock(expiryGlobalMutex);
            if (expiryGlobalCV.wait_for(lock, std::chrono::seconds(10), [this] { return !running.load(std::memory_order_relaxed); })) {
                if (!running.load(std::memory_order_relaxed)) break;
            }
        } 

        if (!running.load(std::memory_order_relaxed)) break;
        
        time_t now = time(nullptr);
        std::vector<std::string> expired_keys;
        
        for (size_t i = 0; i < capacity; ++i) {
            Node* current = table[i].load(std::memory_order_acquire);
            incrementRefCount(current);
            
            Node* node_iter = current;
            
            
            while (node_iter != nullptr) {
                
                if (node_iter->expiry != 0 && now > node_iter->expiry) {
                    expired_keys.push_back(node_iter->key); // Copy key
                }
                Node* next_node = node_iter->next.load(std::memory_order_acquire);
                incrementRefCount(next_node);
                decrementRefCount(node_iter); 
                node_iter = next_node;
            }
            if (node_iter) decrementRefCount(node_iter); 
            else if (current) decrementRefCount(current);
        }
        
        for (const auto& key : expired_keys) {
            this->remove(key);
        }
        processDeferredDeletes();
    }
}


void HashMap::updateLRU(const std::string &key) {
    std::lock_guard<std::mutex> lock(lruMutex);
    auto it = lruMap.find(key);
    if (it != lruMap.end()) {
        lruList.erase(it->second);
    }
    lruList.push_front(key);
    lruMap[key] = lruList.begin();
}


void HashMap::lruMonitor() {
    while (lruRunning.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lock(lruMutex);
            if(lruCV.wait_for(lock, std::chrono::seconds(5), [this] { return !lruRunning.load(std::memory_order_relaxed); })) {
                 if (!lruRunning.load(std::memory_order_relaxed)) break;
            }
        }

        if (!lruRunning.load(std::memory_order_relaxed)) break;
        
        if (size.load(std::memory_order_acquire) > MAX_CAPACITY) {
            std::string key_to_evict;
            {
                std::lock_guard<std::mutex> lock(lruMutex);
                if (!lruList.empty()) {
                    key_to_evict = lruList.back();
                    lruList.pop_back();
                    lruMap.erase(key_to_evict);
                }
            }
            if (!key_to_evict.empty()) {
                this->remove(key_to_evict);
            }
        }
        processDeferredDeletes();
    }
}

void HashMap::print_map() const{
    std::cout << "--- HashMap RCU (Size: " << size.load() << ", Capacity: " << capacity << ") ---" << std::endl;
    for (size_t i = 0; i < capacity; ++i) {
        Node* current = table[i].load(std::memory_order_acquire);
        if (current) {
            std::cout << "Bucket[" << i << "]: ";
            incrementRefCount(current);
            Node* p_iter = current;
            while (p_iter) {
                std::cout << "{" << p_iter->key << ":" << p_iter->value 
                          << " (Refs: " << p_iter->refCount.load() 
                          << ", Exp: " << p_iter->expiry << ")} -> ";
                Node* next_p = p_iter->next.load(std::memory_order_acquire);
                incrementRefCount(next_p);
                decrementRefCount(p_iter);
                p_iter = next_p;
            }
            if(p_iter) decrementRefCount(p_iter); 
            else if(current) decrementRefCount(current); 
            std::cout << "NULL" << std::endl;
        }
    }
    std::cout << "--- End HashMap RCU ---" << std::endl;
}

std::vector<HashMap::NodeData> HashMap::getAllForPersistence() const{
    std::vector<HashMap::NodeData> data;
    data.reserve(size.load(std::memory_order_relaxed));

    for (size_t i = 0; i < capacity; ++i) {
        Node* current = table[i].load(std::memory_order_acquire);
        incrementRefCount(current); 

        Node* node_iter = current;
        while (node_iter) {
            NodeData nd = {
                node_iter->key,
                node_iter->value,
                node_iter->expiry,
                node_iter->lastAccessed.load(std::memory_order_relaxed)
            };
            data.push_back(nd);

            Node* next_node = node_iter->next.load(std::memory_order_acquire);
            incrementRefCount(next_node);
            decrementRefCount(node_iter); 
            node_iter = next_node;     
        }
        if (node_iter) decrementRefCount(node_iter); 
        else if(current) decrementRefCount(current); 

    }
    return data;
}