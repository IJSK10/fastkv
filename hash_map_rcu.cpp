#include "hash_map_rcu.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <thread>

using namespace std::chrono;

HashMap:: HashMap(const std::string &persistenceFile) : capacity(INITIAL_CAPACITY), size(0), table(INTIAL_CAPACITY), running(true), lruRunning(true), workersRunning(true)
{
    cleanupThread = std::thread(&HashMap::cleanupExpired, this);
    lruThread = std::thread(&HashMap::lruMonitor,this);
    for (int i=0;i<std::thread::hardware_concurrency();++i)
    {
        workerThread.emplace_back(&HashMap::workerFunction,this);
    }

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
    expiryCV.notify_all();
    if (cleanupThread.joinable()) cleanupThread.join();

    lruRunning = false;
    lruCV.notify_all();
    if (lruThread.joinable()) lruThread.join();

    wokersRunning = false;
    taskCV.notify_all();
    for (auto &t: workerThread)
    {
        if (t.joinable()) t.join();
    }

    try {
        Persistence::saveToFile(*this, "hashmap.json");
    }
    catch (const std::exception& e)
    {
        std::cerr << "Falied to save to persistence file:"<<e.what() <<std::endl;
    }

    for (auto &bucket: table)
    {
        deleteList(bucket.load());
    }
}

void HashMap::set(const std::string &key, const std::string &value, int ttl)
{
    auto task=std::make_shared<Task>(TaskType::SET, key, value, ttl);
    std::future<std::string> future=task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();
    future.get();
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
    return future.get();
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
    return future.get() == "OK";
}

void HashMap::safeDelete(Node* node)
{
    if (!node) return;

    while (node->refCount > 0)
    {
        std::this_thread::yield();
    }
    delete node;
}

void HashMap::setInternal(const std::string &key, const std::string &val,int ttl)
{
    size_t index=hashFunction(key,capacity);

    Node *head=table[index].load();
    Node *prev=nullptr;
    Node* current =head;
    Node* nodeToDelete = nullptr;

    if (head)
    {
        indrementRefCount(head);
    }

    while (current)
    {
        Node* nextNode = current->next.load();

        if (nextNode)
        {
            incrementRefCount(nextNode);
        }

        if (current->key==key)
        {
            time_t expiryTime == tll ? time(nullptr) + ttl : 0;
            Node* newNode = new Node(key,val,expiryTime);

            newNode->next.store(nextNode);

            if (prev)
            {
                prev->next.store(newNode);
            }
            else
            {
                table[index].store(newNode);
            }

            nodeToDelete = current;

            updateLRU(key);
            if (nextNode)
            {
                decrementRefCount(nextNode);
            }
            break;
        }

        if (prev)
        {
            decrementRefCount(prev);
        }

        prev=current;
        current = nextNode;
    }

    if (!nodeToDelete)
    {
        time_t expiryTime = ttl ? time(nullptr) + ttl : 0;
        Node* newNode = new Node(key,val,expiryTime);
        newNode->next.store(head);
        table[index].store(newNode);

        size++;

        updateLRU(key);

        decrementRefCount(prev);
    }
    else
    {
        if (prev)
        {
            decrementRefCount(prev);
        }
        safeDelete(nodeToDelete);

    }
}

std::string HashMap::getInternal(const std::string &key)
{
    size_t index=hashFunction(key, capacity);
    Node* current = table[index].load();
    if (current)
    {
        incrementRefCount(current);
    }

    while (current)
    {
        if (current->key == key)
        {
            if (current->expiry && time(nullptr)> current->expiry)
            {
                decrementRefCount(current);
                return "";
            }
            current->lastAccessed= time(nullptr);
            updateLRU(key);
            decrementRefCount(current);
            return current->value;
        }

        Node* nextNode = current->next.load();
        if (nextNode)
        {
            incrementRefCount(nextNode);
        }
        decrementRefCount(current);
        current=nextNode;
    }

    if (current)
    {
        decrementRefCount(current);
    }
    return "";
}

bool HashMap::removeInternal(const std::string & key)
{
    size_t index = hashFunction(key,capacity);

    Node* head=table[index].load();
    Node* prev=nullptr;
    Node* current = head;
    Node* nodeToDelete = nullptr;

    if (current)
    {
        incrementRefCount(current);
    }

    while (current)
    {
        Node* nextNode = current->next.load();
        incrementRefCount(nextNode);

        if (current->key == key)
        {
            if (prev)
            {
                prev->next.store(nextNode);
            }
            else
            {
                table[index].store(nextNode);
            }
            NodeToDelete = current;
            removeLRUKey(key);
    
            size--;
    
            decrementRefCount(nextNode);
            break;
        }
    
        if (prev)
        {
            decrementRefCount(prev);
        }
    
        prev=current;
        current=nextNode;
        }
    if (nodeToDelete) {
        safeDelete(nodeToDelete);

        if (prev)
        {
            decrementRefCount(prev);
        }
        return true;
    }
    else{

        if (current)
        {
            decrementRefCount(current);
        }
        if (prev)
        {
            decrementRefCount(prev);
        }
        return false;
    }
}

void HashMap::deleteList(Node *head)
{
    while (head)
    {
        Node *temp=head;
        head = head->next.load();
        delete temp;
    }
}

void HashMap::cleanupExpired() {
    while (running) {
        {
            std::unique_lock<std::mutex> lock(expiryMutex);
            expiryCV.wait_for(lock, std::chrono::seconds(2), [this]{ return !running; });
            if (!running) break;
        }
        
        time_t now = time(nullptr);
        std::vector<std::string> keysToRemove;
        
        for (size_t i = 0; i < capacity; ++i) {
            Node* prev = nullptr;
            Node* current = table[i].load();
            
            // Protect current node
            incrementRefCount(current);
            
            while (current) {
                // Protect next node
                Node* nextNode = current->next.load();
                incrementRefCount(nextNode);
                
                if (current->expiry && now > current->expiry) {
                    // Collect key to remove from LRU later
                    keysToRemove.push_back(current->key);
                    
                    // Remove expired node
                    if (prev) {
                        prev->next.store(nextNode);
                    } else {
                        table[i].store(nextNode);
                    }
                    
                    // Schedule node for deletion
                    Node* nodeToDelete = current;
                    
                    // Decrement size atomically
                    size--;
                    
                    // Move to next node
                    current = nextNode;
                    
                    // Safe delete after releasing reference
                    safeDelete(nodeToDelete);
                    
                    // Release previous node if we have one
                    if (prev) {
                        decrementRefCount(prev);
                    }
                } else {
                    // Release previous node if it exists
                    if (prev) {
                        decrementRefCount(prev);
                    }
                    
                    // Move to next node
                    prev = current;
                    current = nextNode;
                }
            }
            
            // Release final node if we have one
            if (prev) {
                decrementRefCount(prev);
            }
        }
        
        // Remove expired keys from LRU
        for (const auto& key : keysToRemove) {
            removeLRUKey(key);
        }
    }
}

void HashMap::removeLRUKey(const std::string& key)
{
    std::lock_guard<std::mutex> lock(lruMutex);
    auto it = lruMap.find(key);
    if (it!=lruMap.end())
    {
        lruList.erase(it->second);
        lruMap.erase(it);
    }
}

void HashMap::workerFunction() {
    while (workersRunning) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(taskMutex);
            taskCV.wait(lock, [&]() {
                return !taskQueue.empty() || !workersRunning;
            });
            if (!workersRunning && taskQueue.empty()) break;
            if (!taskQueue.empty()) {
                task = taskQueue.front();
                taskQueue.pop();
            } else {
                continue;
            }
        }

        try {
            switch (task->type) {
                case TaskType::SET:
                    setInternal(task->key, task->value, task->ttl);
                    task->result.set_value("OK");
                    break;
                case TaskType::GET:
                    task->result.set_value(getInternal(task->key));
                    break;
                case TaskType::REMOVE:
                    task->result.set_value(removeInternal(task->key) ? "OK" : "NOT_FOUND");
                    break;
            }
        } catch (const std::exception& e) {
            try {
                task->result.set_exception(std::current_exception());
            } catch (...) {
                // Promise might be already satisfied
            }
        }
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
    
    // Check if we need to remove LRU items
    if (lruList.size() > capacity) {
        std::string lruKey = lruList.back();
        lruList.pop_back();
        lruMap.erase(lruKey);
        
        // Schedule removal in a separate task to avoid deadlock
        auto task = std::make_shared<Task>(TaskType::REMOVE, lruKey);
        {
            std::lock_guard<std::mutex> lock(taskMutex);
            taskQueue.push(task);
        }
        taskCV.notify_one();
    }
}

void HashMap::removeLRUItem()
{
    std::string lruKey;
    {
        std::lock_guard<std::mutex> lock(lruMutex);
        if (lruList.empty()) return;
        lruKey = lruList.back();
        lruList.pop_back();
        lruMap.erase(lruKey);
    }
    removeInternal(lruKey);
}

void HashMap::lruMonitor() {
    while (lruRunning) {
        {
            std::unique_lock<std::mutex> lock(lruMutex);
            lruCV.wait_for(lock, std::chrono::seconds(5), [this]{ return !lruRunning; });
            if (!lruRunning) break;
        }
        
        // Check if we need to evict items based on capacity
        if (size > capacity * 0.9) {
            std::string lruKey;
            {
                std::lock_guard<std::mutex> lock(lruMutex);
                if (!lruList.empty()) {
                    lruKey = lruList.back();
                    lruList.pop_back();
                    lruMap.erase(lruKey);
                }
            }
            
            // If we have an LRU key to remove, schedule it
            if (!lruKey.empty()) {
                auto task = std::make_shared<Task>(TaskType::REMOVE, lruKey);
                {
                    std::lock_guard<std::mutex> lock(taskMutex);
                    taskQueue.push(task);
                }
                taskCV.notify_one();
            }
        }
    }
}

void HashMap::print_map()
{
    for (size_t i = 0; i < capacity; ++i)
    {
        for (Node *node = table[i].load(); node; node = node->next)
        {
            std::cout << node->key << ": " << node->value << std::endl;
        }
    }
}

std::vector<std::pair<std::string, HashMap::Node>> HashMap::getAll() const
{
    std::vector<std::pair<std::string, Node>> result;
    for (size_t i = 0; i < capacity; ++i)
    {
        for (Node *node = table[i].load(); node; node = node->next)
        {
            result.emplace_back(node->key, *node);
        }
    }
    return result;
}