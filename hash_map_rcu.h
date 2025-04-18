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
#include <shared_mutex>

const size_t INTIAL_CAPACITY = 2048;
const size_t MIN_CAPACITY = 16;
const size_t MAX_CAPACITY = 2048;

class HashMap
{
    private:
    struct Node{
        std::string key;
        std::string value;
        time_t expiry;
        time_t lastAccessed;
        std::atomic<Node*> next;
        std::atomic<int> refCount;

        Node(std::string k, std::string v, time_t exp=0) : key(std::move(k)), value(std::move(v)), expiry(exp), lastAccessed(time(nullptr)), next(nullptr) {}

    };

    inline void incrementRefCount(HashMap::Node* node)
    {
        if (node)
        {
            node->refCount++;
        }
    }

    inline void decrementRefCount(HashMap::Node* node)
    {
        if (node)
        {
            node->refCount--;
        }
    }

    enum class TaskType{SET,GET,REMOVE};

    struct Task{
        TaskType type;
        std::string key;
        std::string value;
        int ttl;
        std::promise<std::string> result;

        Task(TaskType t, std::string k, std::string v=" ",int ttl_val=0) : type(t), key(std::move(k)), value(std::move(v)), ttl(ttl_val) {}
    };

    std::vector<std::atomic<Node*>> table;
    size_t capacity;
    std::atomic<size_t> size;

    //RCU Tracking
    std::atomic<bool> running;
    std::thread cleanupThread;

    //LRU
    std::list<std::string> lruList;
    std::unordered_map<std::string, std::list<std::string> ::iterator> lruMap;
    std::mutex lruMutex;
    std::thread lruThread;
    std::atomic<bool> lruRunning;
    std::condition_variable lruCV;

    //Expiry Management
    std::priority_queue<std::pair<time_t,std::string>, std::vector<std::pair<time_t,std::string>>,std::greater<>> expiryQueue;
    std::mutex expiryMutex;
    std::condition_variable_any expiryCV;

    //Worker pool
    std::vector<std::thread> workerThread;
    std::queue<std::shared_ptr<Task>> taskQueue;
    std::mutex taskMutex;
    std::condition_variable taskCV;
    std::atomic<bool> wokersRunning;

    //
    std::string PersistenceFileName;

    //Hash and Resize
    size_t hashFunction(const std::string &key,size_t cap) const{
        return fnv1a_hash(key)%cap;
    }

    void resize(size_t new_capacity);
    void cleanupExpired();
    void scheduleDefferedDelete(Node* oldNode);
    void deleteList(Node* head);

    //LRU Management
    void updateLRU(const std::string& key);
    void removeLRUItem();
    void lruMonitor();

    //worker
    void workerFunction();

    //Internal (RCU-based) operations
    void setInternal(const std::string &key, const std::string & value, int ttl=0);
    std::string getInternal(const std::string &key);
    bool removeInternal(const std::string &key);

    public:
    HashMap(const std::string &persistenceFile = "hashmap.json");
    ~HashMap();

    // Public thread-safe API
    void set(const std::string &key, const std::string &value, int ttl = 0);
    std::string get(const std::string &key);
    bool remove(const std::string &key);

    void print_map();
    std::vector<std::pair<std::string, Node>> getAll() const;
};

#endif
