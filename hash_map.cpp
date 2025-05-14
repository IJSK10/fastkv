#include "hash_map.h"
#include <iostream>
#include <chrono>
#include <thread>
#include "persistence.h"

HashMap::HashMap(const std::string &persistenceFile) : capacity(INITIAL_CAPACITY), size(0), running(true), lruRunning(true), workersRunning(true)
{
    table.resize(capacity, nullptr);
    bucketLocks = std::vector<std::shared_mutex>(capacity);

    // start cleanupthread
    cleanupThread = std::thread(&HashMap::cleanupExpired, this);

    // start LRU Thread
    lruThread = std::thread(&HashMap::lruMonitor, this);

    for (int i = 0; i < 3; i++)
    {
        workerThreads.emplace_back(&HashMap::workerFunction, this);
    }

    if (!persistenceFile.empty())
    {
        Persistence::loadFromFile(*this, persistenceFile);
    }
}

HashMap::~HashMap()
{
    try
    {
        running = false;
        lruRunning = false;
        workersRunning = false;

        expiryCV.notify_all();
        lruCV.notify_all();
        taskCV.notify_all();

        if (cleanupThread.joinable())
            cleanupThread.join();
        if (lruThread.joinable())
            lruThread.join();

        for (auto &thread : workerThreads)
        {
            if (thread.joinable())
                thread.join();
        }
        try
        {
            Persistence::saveToFile(*this, "hashmap.json");
        }
        catch (const std::exception &e)
        {
            std::cerr << "Falied to save to persistence file:" << e.what() << std::endl;
        }

        for (size_t i = 0; i < table.size(); ++i) {
            deleteList(table[i]);
            table[i] = nullptr;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in HashMap destructor: " << e.what() << std::endl;
    }
}

void HashMap::workerFunction()
{
    while (workersRunning)
    {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(taskMutex);
            if (taskCV.wait_for(lock, std::chrono::seconds(1), [this]
                                { return !taskQueue.empty() || !workersRunning; }))
            {
                if (!workersRunning && taskQueue.empty())
                {
                    break; // Exit if shutting down and no more tasks
                }
                if (!taskQueue.empty())
                {
                    task = taskQueue.front();
                    taskQueue.pop();
                }
                else
                {
                    continue;
                }
            }
            else
            {
                if (!workersRunning)
                {
                    break;
                }
                continue;
            }
        }
        if (task)
        {
            switch (task->type)
            {
            case TaskType::SET:
            {
                setInternal(task->key, task->value, task->ttl);
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
                task->result.set_value(result ? "true" : "false");
                break;
            }
            }
        }
    }
}

void HashMap::set(const std::string &key, const std::string &value, int ttl)
{
    auto task = std::make_shared<Task>(TaskType::SET, key, value, ttl);
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

    try
    {
        return future.get(); // Blocks until worker sets the promise
    }
    catch (const std::future_error &e)
    {
        std::cerr << "Future error in get for key '" << key << "': " << e.what() << std::endl;
        return "Error: Future error"; // Or throw, or return specific error string
    }

}

bool HashMap::remove(const std::string &key)
{
    auto task = std::make_shared<Task>(TaskType::REMOVE, key);
    std::future<std::string> future = task->result.get_future();
    {
        std::lock_guard<std::mutex> lock(taskMutex);
        taskQueue.push(task);
    }
    taskCV.notify_one();


    try
    {
        // Using the wait_for logic:
        auto status = future.wait_for(std::chrono::milliseconds(1000)); // Increased timeout slightly
        if (status == std::future_status::timeout)
        {
            std::cerr << "Timeout waiting for remove operation on key: " << key << std::endl;
            return false;
        }
        return future.get() == "true";
    }
    catch (const std::future_error &e)
    {
        std::cerr << "Future error in remove for key '" << key << "': " << e.what() << std::endl;
        return false;
    }
    catch (const std::exception &e) // Catches other exceptions (e.g., if promise set with exception)
    {
        std::cerr << "Error in remove for key '" << key << "': " << e.what() << std::endl;
        return false;
    }
}

// Internal Implementation
void HashMap::setInternal(const std::string &key, const std::string &value, int ttl)
{
    size_t index = hashFunction(key, capacity);
    time_t expiry = ttl > 0 ? time(nullptr) + ttl : 0;
    bool new_key_added=false;

    std::unique_lock<std::shared_mutex> lock(bucketLocks[index]);

    Node *head = table[index];
    Node *prev = nullptr;
    bool key_found = false;

    while (head)
    {
        if (head->key == key)
        {
            head->value = value;
            head->expiry = expiry;
            head->lastAccessed = time(nullptr);
            lock.unlock();
            updateLRU(key);
            if (ttl > 0) {
                updateExpiryQueue(key, expiry);
            }
            key_found = true;
            return;
        }
        prev = head;
        head = head->next;
    }

    if (size.load(std::memory_order_acquire) >= MAX_CAPACITY)
    {
        lock.unlock();
        removeLRUItem();
        lock.lock();
        head = table[index];
        prev = nullptr; 
        while(head) {
            if (head->key == key) {
                head->value = value; head->expiry = expiry; head->lastAccessed = time(nullptr);
                updateLRU(key);
                if (ttl > 0) updateExpiryQueue(key, expiry);
                return;
            }
            prev = head;
            head = head->next;
        }

    }
    Node *new_node = new Node(key, value, expiry);
    new_node->lastAccessed = time(nullptr);
    if (prev)
    {
        prev->next = new_node;
    }
    else
    {
        table[index] = new_node;
    }
    size.fetch_add(1, std::memory_order_release);
    new_key_added = true;
    updateLRU(key);
    if (ttl > 0)
    {
        updateExpiryQueue(key,expiry);
    }
}

void HashMap::updateExpiryQueue(const std::string &key, time_t expiry)
{
    std::lock_guard<std::mutex> lock(expiryMutex);


    bool shouldNotify = expiryQueue.empty() || expiry < expiryQueue.top().first;
    expiryQueue.emplace(expiry, key);

    if (shouldNotify)
    {
        expiryCV.notify_all();
    }
}

std::string HashMap::getInternal(const std::string &key)
{
    size_t index = hashFunction(key, capacity);

    std::shared_lock<std::shared_mutex> lock(bucketLocks[index]);

    Node *head = table[index];

    while (head)
    {
        if (head->key == key)
        {
            if (head->expiry > 0 && time(nullptr) > head->expiry)
            {
                lock.unlock();
                remove(key);
                return "Key expired";
            }
            else
            {
                head->lastAccessed = time(nullptr);
                std::string val = head->value;
                updateLRU(key);
                return val;
            }
        }
        head = head->next;
    }
    return "Key not found";
}

bool HashMap::removeInternal(const std::string &key)
{
    size_t index = hashFunction(key, capacity);
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
                if (it != lruMap.end())
                {
                    lruList.erase(it->second);
                    lruMap.erase(it);
                }
            }
            delete head;
            size.fetch_sub(1, std::memory_order_release);

            return true;
        }
        prev = head;
        head = head->next;
    }
    return false;
}

void HashMap::resize(size_t new_capacity)
{
    if (new_capacity < MIN_CAPACITY || new_capacity > MAX_CAPACITY)
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
            size_t new_index = hashFunction(head->key, new_capacity);

            head->next = new_table[new_index];
            new_table[new_index] = head;
            head = next;
        }
    }

    table = std::move(new_table);
    bucketLocks = std::move(new_bucketLocks);
    capacity = new_capacity;
}

void HashMap::cleanupExpired()
{
    while (running)
    {
        std::unique_lock<std::mutex> lock(expiryMutex);

        if (expiryQueue.empty() && running)
        {
            expiryCV.wait(lock, [this]
                          { return !expiryQueue.empty() || !running; });
        }

        if (!running && expiryQueue.empty())
            return;

        if (!running && !expiryQueue.empty())
        {
            while (!expiryQueue.empty())
            {
                std::string key_to_remove = expiryQueue.top().second;
                expiryQueue.pop();
                lock.unlock();
                remove(key_to_remove); // Queue removal task
                lock.lock();
                if (!running && expiryQueue.empty())
                    return; // check again
            }
            if (!running)
                return;
        }

        while (!expiryQueue.empty() && running)
        {
            auto now = std::time(nullptr);
            auto nextExpiry = expiryQueue.top().first;
            std::string key = expiryQueue.top().second;

            if (nextExpiry > now)
            {
                auto wait_time = std::chrono::system_clock::from_time_t(nextExpiry);
                if (expiryCV.wait_until(lock, wait_time, [this, nextExpiry]
                                        { return !running || expiryQueue.empty() || expiryQueue.top().first < nextExpiry || expiryQueue.top().first <= std::time(nullptr); }))
                {
                }
            }

            if (!running)
                break;

            if (!expiryQueue.empty() && expiryQueue.top().first <= std::time(nullptr))
            {
                std::string key_to_remove = expiryQueue.top().second;
                expiryQueue.pop();

                lock.unlock();
                remove(key_to_remove);
                lock.lock();
            }
            else if (!expiryQueue.empty() && expiryQueue.top().first > std::time(nullptr))
            {
                continue;
            }
            else if (expiryQueue.empty())
            {
                break;
            }
        }

    }
}

void HashMap::deleteList(Node *head)
{
    while (head)
    {
        Node *temp = head;
        head = head->next;
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

std::vector<std::pair<std::string, HashMap::Node>> HashMap::getAll() const
{
    std::vector<std::pair<std::string, Node>> allData;
    allData.reserve(std::min(size.load(), static_cast<size_t>(10000)));

    for (size_t i = 0; i < capacity; i++)
    {
        Node *current = table[i];
        while (current)
        {
            allData.push_back({current->key, *current});
            current = current->next;
        }
    }
    return allData;
}

void HashMap::updateLRU(const std::string &key)
{
    std::lock_guard<std::mutex> lock(lruMutex);

    auto it = lruMap.find(key);
    if (it != lruMap.end())
    {
        lruList.erase(it->second);
    }

    lruList.push_front(key);
    lruMap[key] = lruList.begin();
}

void HashMap::removeLRUItem()
{
    std::string lruKey;
    bool item_to_remove = false;
    {
        std::lock_guard<std::mutex> lock(lruMutex);
        if (!lruList.empty())
        {
            lruKey = lruList.back();
            lruList.pop_back();
            lruMap.erase(lruKey);
            item_to_remove = true;
        }
    }

    if (item_to_remove)
    {
        removeInternal(lruKey);
    }
}

void HashMap::lruMonitor()
{
    while (lruRunning)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (!lruRunning)
            break;
    }
}