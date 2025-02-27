#ifndef HASH_MAP_H
#define HASH_MAP_H

#include "fnv_hash.h"
#include <vector>
#include <string>
#include <shared_mutex>
#include <mutex>
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

const size_t INITIAL_CAPACITY = 16;
const size_t MIN_CAPACITY = 16;
const size_t MAX_CAPACITY = 2048;

class HashMap
{
private:
    struct Node
    {
        std::string key;
        std::string value;
        time_t expiry;
        time_t lastAccessed;
        Node *next;
        Node(std::string k, std::string v, time_t exp = 0) : key(k), value(v), expiry(exp), lastAccessed(time(nullptr)),next(nullptr) {}
    };


    // Task Structure for worker threads
    enum class TaskType {SET , GET, REMOVE};
    struct Task{
        TaskType type;
        std::string key;
        std::string value;
        int ttl;
        std::promise<std::string> result;

        Task(TaskType t,std::string k, std::string v="",int ttl_val=0) : type(t), key(k), value(v), ttl(ttl_val) {}
    };


    //HashMap componenets
    std::vector<Node *> table;
    size_t capacity;
    std::atomic<size_t> size;
    std::vector<std::shared_mutex> bucketLocks;
    std::mutex writelock;

    //LRU tracking

    std::list<std::string> lruList;
    std::unordered_map<std::string,std::list<std::string>::iterator> lruMap;
    std::mutex lruMutex;
    std::thread lruThread;
    std::atomic<bool> lruRunning;
    std::condition_variable lruCV;

    //Expiry Queue
    std::priority_queue<std::pair<time_t, std::string>, std::vector<std::pair<time_t, std::string>>, std::greater<>> expiryQueue;
    std::mutex expiryMutex;
    std::condition_variable_any expiryCV;

    //Worker Threads
    std::vector<std::thread> workerThreads;
    std::queue<std::shared_ptr<Task>> taskQueue;
    std::mutex taskMutex;
    std::condition_variable taskCV;
    std::atomic<bool> workersRunning;

    //Cleanup Thread
    std::atomic<bool> running;
    std::thread cleanupThread;

    void resize(size_t new_capacity);
    void cleanupExpired();
    void deleteList(Node *head);
    size_t hashFunction(const std::string& key,size_t cap) const {
        return fnv1a_hash(key)%cap;
    }

    //LRU functions
    void updateLRU(const std::string &key);
    void removeLRUItem();
    void lruMonitor();

    //Worker Thread Function
    void workerFunction();
    
    //Internal implementations
    void setInternal(const std::string &key,const std::string &value,int ttl=0);
    std::string getInternal(const std::string &key);
    bool removeInternal(const std::string &key);


public:
    HashMap(const std::string &persistenceFile = "hashmap.json");
    ;
    ~HashMap();

    //Thread safe
    void set(const std::string &key, const std::string &value, int ttl = 0);
    std::string get(const std::string &key);
    bool remove(const std::string &key);


    void print_map();
    std::vector<std::pair<std::string, Node>> getAll() const;
    
};

#endif