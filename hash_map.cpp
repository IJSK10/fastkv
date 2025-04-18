#include "hash_map.h"
#include <iostream>
#include <chrono>
#include <thread>
#include "persistence.h"

HashMap::HashMap(const std::string& persistenceFile) : capacity(INITIAL_CAPACITY), size(0), running(true), lruRunning(true), workersRunning(true)
{
    table.resize(capacity, nullptr);
    bucketLocks = std::vector<std::shared_mutex>(capacity);

    //start cleanupthread
    cleanupThread = std::thread(&HashMap::cleanupExpired, this);

    //start LRU Thread
    lruThread = std::thread(&HashMap::lruMonitor,this);

    for (int i=0;i<3;i++)
    {
        workerThreads.emplace_back(&HashMap::workerFunction, this);
    }

    // if (!persistenceFile.empty())
    // {
    //     try{
    //         Persistence::loadFromFile(*this,persistenceFile);
    //     }
    //     catch (const std::exception& e)
    //     {
    //         std::cerr<<"Failed to load from persistence file" <<e.what() <<std::endl;
    //     }
    // }
}

HashMap::~HashMap()
{
    try {
        running = false;
        lruRunning = false;
        workersRunning = false;
        
        expiryCV.notify_all();
        lruCV.notify_all();
        taskCV.notify_all();

        if (cleanupThread.joinable()) cleanupThread.join();
        if (lruThread.joinable()) lruThread.join();
        
        for (auto& thread : workerThreads) {
            if (thread.joinable()) thread.join();
        }
        try {
            Persistence::saveToFile(*this, "hashmap.json");
        }
        catch (const std::exception& e)
        {
            std::cerr << "Falied to save to persistence file:"<<e.what() <<std::endl;
        }

        for (auto* head : table) {
            deleteList(head);
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception in HashMap destructor: " << e.what() << std::endl;
    }
}

void HashMap::workerFunction()
{
    while(workersRunning)
    {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(taskMutex);
            if (taskCV.wait_for(lock,std::chrono::seconds(1),[this]{
                return !taskQueue.empty() || !workersRunning;
            }))
            {
                if (!workersRunning) break;
                task=taskQueue.front();
                taskQueue.pop();
            }
        }
        if (!task) continue;

        switch(task->type)
        {
            case TaskType::SET:
            {
                setInternal(task->key,task->value,task->ttl);
                break;
            }
            case TaskType::GET:
            {
                task->result.set_value(getInternal(task->key));
                break;
            }
            case TaskType::REMOVE:
            {
                bool result = removeInternal(task->key);
                task->result.set_value(result? "true" : "false");
                break;
            }
        }
    }
}




void HashMap::set(const std::string& key, const std::string &value,int ttl)
{
    auto task = std::make_shared<Task>(TaskType::SET,key,value,ttl);
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();
}

std::string HashMap::get(const std::string &key)
{
    auto task = std::make_shared<Task>(TaskType::GET,key);
    std::future<std::string> future=task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();

    try{
        auto status = future.wait_for(std::chrono::milliseconds(500));
        if (status == std::future_status::timeout)
        {
            return "Operation Timed out";
        }
        return future.get();
    }
    catch (const std::exception &e)
    {
        return "Error: "+std::string(e.what());
    }
}

bool HashMap::remove(const std::string &key)
{
    auto task = std::make_shared<Task>(TaskType::REMOVE,key);
    std::future<std::string> future = task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();

    try{
        auto status = future.wait_for(std::chrono::milliseconds(500));
        if (status==std::future_status::timeout)
        {
            return false;
        }
        return future.get()=="true";
    }
    catch(const std::exception& e)
    {
        std::cerr<<"Error in remove:" << e.what() <<std::endl;
        return false;
    }
}


//Internal Implementation
void HashMap::setInternal(const std::string &key, const std::string &value, int ttl)
{
    //std::cout<<"Insert Key:"<<key<<", Value:"<<value<<std::endl;   
    size_t index = hashFunction(key,capacity);
    time_t expiry = ttl > 0 ? time(nullptr) + ttl : 0;

    if (size>=MAX_CAPACITY)
    {
        removeLRUItem();
        size--;
    }

    {
        std::unique_lock<std::shared_mutex> lock(bucketLocks[index]);
        
        Node *head = table[index];
        Node *prev = nullptr;
        
        while (head)
        {
            if (head->key == key)
            {
                head->value = value;
                head->expiry = expiry;

                head->lastAccessed = time(nullptr);
                updateLRU(key);

                if (ttl>0) {
                    updateExpiryQueue(key,expiry);
                }
                return;
            }
            prev=head;
            head = head->next;
        }
        
        Node *new_node = new Node(key, value, expiry);
        if (prev)
        {
            prev->next=new_node;
        }
        else
        {
            table[index]=new_node;
        }
        size++;
        updateLRU(key);
    }

    if (ttl>0)
    {
        updateExpiryQueue(key,expiry);
    }
    
    if (static_cast<float>(size) / capacity > 0.75 && capacity*2<=MAX_CAPACITY)
    {
        resize(capacity * 2);
    }
}

void HashMap::updateExpiryQueue(const std::string& key, time_t expiry)
{
    std::lock_guard<std::mutex> lock(expiryMutex);

    auto current =expiryQueue;

    while (!current.empty())
    {
        if (current.top().second==key)
        {
            current.pop();
        }
    }

    bool shouldNotify = expiryQueue.empty() || expiry < expiryQueue.top().first;
    expiryQueue.emplace(expiry,key);

    if (shouldNotify){
        expiryCV.notify_all();
    }
}


std::string HashMap::getInternal(const std::string &key)
{
    size_t index = hashFunction(key,capacity);
    
    std::shared_lock<std::shared_mutex> lock(bucketLocks[index]);
    
    Node *head = table[index];
    
    while (head)
    {
        if (head->key == key){
            if (head->expiry>0 && time(nullptr)>head->expiry)
            {
                lock.unlock();
                remove(key);
                return "Key expired";
            }
            else
            {
                head->lastAccessed=time(nullptr);
                updateLRU(key);
                return head->value;
            }
        }
        head = head->next;
    }
    return "Key not found";
}

bool HashMap::removeInternal(const std::string &key)
{
    size_t index = hashFunction(key,capacity);
    std::unique_lock<std::shared_mutex> lock(bucketLocks[index]);

    Node *head = table[index];
    Node *prev = nullptr;
    while (head)
    {
        if (head->key == key)
        {
            if (prev)
            {
                prev->next = head->next;
            }
            else
            {
                table[index] = head->next;
            }
            {
                std::lock_guard<std::mutex> lruLock(lruMutex);
                auto it = lruMap.find(key);
                if (it!=lruMap.end())
                {
                    lruList.erase(it->second);
                    lruMap.erase(it);
                }
            }
            delete head;
            size--;

            if ((float)size / capacity < 0.25 && capacity > MIN_CAPACITY)
            {
                lock.unlock();
                resize(capacity / 2);
            }
            
            return true;
        }
        prev = head;
        head = head->next;
    }
    return false;
}

void HashMap::resize(size_t new_capacity)
{
    if (new_capacity < MIN_CAPACITY || new_capacity>MAX_CAPACITY)
        return;

    std::lock_guard<std::mutex> lock(writelock);

    std::vector<Node *> new_table(new_capacity, nullptr);
    std::vector<std::shared_mutex> new_bucketLocks(new_capacity);
    

    for (size_t i = 0; i < capacity; i++)
    {
        Node *head = table[i];
        while (head)
        {
            Node *next = head->next;
            size_t new_index = hashFunction(head->key,new_capacity);

            head->next = new_table[new_index];
            new_table[new_index] = head;
            head = next;
        }
    }

    table =std::move(new_table);
    bucketLocks=std::move(new_bucketLocks);
    capacity = new_capacity;
}

void HashMap::cleanupExpired() {
    while (running) {
        std::unique_lock<std::mutex> lock(expiryMutex);

        while (!expiryQueue.empty()) {
            auto now = std::time(nullptr);
            auto nextExpiry = expiryQueue.top().first;

            if (nextExpiry > now) {
                expiryCV.wait_until(lock, std::chrono::system_clock::from_time_t(nextExpiry));
            }

            if (!running) return;

            if (!expiryQueue.empty() && expiryQueue.top().first <= std::time(nullptr)) {
                std::string key = expiryQueue.top().second;
                expiryQueue.pop();

                lock.unlock();  // Unlock expiryMutex before calling remove()
                remove(key);
                lock.lock();    // Re-acquire expiryMutex for next iteration
            }
        }

        expiryCV.wait(lock);  // Wait for next expiry signal
    }
}


void HashMap::deleteList(Node* head)
{
    while (head)
        {
            Node* temp=head;
            head=head->next;
            delete temp;
        }
}

void HashMap::print_map()
{
    for (size_t i = 0; i < capacity; i++)
    {
        std::shared_lock lock(bucketLocks[i]);
        std::cout << "[" << i << "]";
        Node *head = table[i];

        while (head)
        {
            std::cout << "{" << head->key << ":" << head->value << "}->";
            head = head->next;
        }
        std::cout << "NULL\n";
    }
}

std::vector<std::pair<std::string, HashMap::Node>> HashMap::getAll() const {
    std::vector<std::pair<std::string, Node>> allData;
    allData.reserve(std::min(size.load(), static_cast<size_t>(10000))); // Limit size
    
    for (size_t i = 0; i < capacity; i++) {
        Node* current = table[i];
        while (current) {
            allData.push_back({current->key, *current});
            current = current->next;
        }
    }
    return allData;
}

void HashMap::updateLRU(const std::string& key)
{
    std::lock_guard<std::mutex> lock(lruMutex);

    auto it = lruMap.find(key);
    if (it!=lruMap.end())
    {
        lruList.erase(it->second);
    }

    lruList.push_front(key);
    lruMap[key]=lruList.begin();
}

void HashMap::removeLRUItem()
{
    std::string lruKey;
    {
        std::lock_guard<std::mutex> lock(lruMutex);
        if (lruList.empty()) return;

        lruKey=lruList.back();
        lruList.pop_back();
        lruMap.erase(lruKey);
    }

    removeInternal(lruKey);
}

void HashMap::lruMonitor()
{
    while (lruRunning)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}