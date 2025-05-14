#ifndef HASH_MAP_H
#define HASH_MAP_H

#include "fnv_hash.h"
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <ctime>
#include <queue>
#include <condition_variable>
#include <nlohmann/json.hpp>
#include <functional>
#include <future>
#include <list>
#include <unordered_map>
#include <memory>
#include <mutex>

const size_t INITIAL_CAPACITY = 2048;
const size_t MIN_CAPACITY = 2048;
const size_t MAX_CAPACITY = 2048;

class HashMap
{
    private:
    struct Node{
        std::string key;
        std::string value;
        time_t expiry;
        std::atomic<time_t> lastAccessed;
        std::atomic<Node*> next;
        std::atomic<int> refCount;

        Node(std::string k, std::string v, time_t exp=0) : key(std::move(k)), value(std::move(v)), expiry(exp), lastAccessed(time(nullptr)), next(nullptr), refCount(0) {}

        Node(const Node&) =delete;
        Node& operator = (const Node&)=delete;
        Node(Node&&) = delete;
        Node& operator=(Node&&)=delete;
    };

    inline void incrementRefCount(Node* node) const;
    inline void decrementRefCount(Node* node) const;


    enum class TaskType{SET,GET,REMOVE};

    struct Task{
        TaskType type;
        std::string key;
        std::string value;
        int ttl;
        std::promise<std::string> result;

        Task(TaskType t, std::string k)
            : type(t), key(std::move(k)), ttl(0) {}
        
        Task(TaskType t, std::string k, std::string v, int timeToLive)
            : type(t), key(std::move(k)), value(std::move(v)), ttl(timeToLive) {}
    };

    std::vector<std::atomic<Node*>> table;
    size_t capacity;
    std::atomic<size_t> size;
    std::atomic<bool> running;


    std::vector<Node*> deferredDeleteQueue;
    std::mutex deferredDeleteMutex;
    void scheduleDeferredDelete(Node* node);
    void processDeferredDeletes(bool forceAll = false);


    std::list<std::string> lruList;
    std::unordered_map<std::string, std::list<std::string> ::iterator> lruMap;
    std::mutex lruMutex;
    std::thread lruThread;
    std::atomic<bool> lruRunning;
    std::condition_variable lruCV;

    //Expiry Management
    std::mutex expiryGlobalMutex;
    std::condition_variable expiryGlobalCV;

    //Worker pool
    std::atomic<bool> workersRunning;
    std::vector<std::thread> workerThread;
    std::queue<std::shared_ptr<Task>> taskQueue;
    std::mutex taskMutex;
    std::condition_variable taskCV;

    //
    std::string PersistenceFileName;

    
    size_t hashFunction(const std::string &key,size_t cap) const{
        return fnv1a_hash(key)%cap;
    }

    std::thread cleanupThread;
    void cleanupExpired();
    

    //LRU Management
    void updateLRU(const std::string& key);
    //void removeLRUItem();
    void lruMonitor();

    //worker
    void workerFunction();

    //Internal (RCU-based) operations
    void setInternal(const std::string &key, const std::string & value, int ttl=0);
    std::string getInternal(const std::string &key) const;
    bool removeInternal(const std::string &key);

    void deleteList(Node* head);

    public:
    explicit HashMap(const std::string &persistenceFile = "hashmap.json");
    ~HashMap();

    // Public thread-safe API
    void set(const std::string &key, const std::string &value, int ttl = 0);
    std::string get(const std::string &key);
    bool remove(const std::string &key);

    void print_map() const;

    size_t current_size() const { return size.load(std::memory_order_relaxed); }
    size_t current_capacity() const { return capacity; }

    struct NodeData { 
        std::string key;
        std::string value;
        time_t expiry;
        time_t lastAccessed;
    };
    std::vector<NodeData> getAllForPersistence() const;

};

#endif
