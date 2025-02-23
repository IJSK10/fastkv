#ifndef HASH_MAP_H
#define HASH_MAP_H

#include "fnv_hash.h"
#include <vector>
#include <string>
#include <mutex>

const size_t INITIAL_CAPACITY = 16;
const size_t MIN_CAPACITY = 16;

class HashMap{
    private:
    struct Node{
        std::string key;
        std::string value;
        Node* next;
        Node(std::string k,std::string v): key(k),value(v), next(nullptr) {}
    };
    std::vector<Node*> table;
    size_t capacity;
    size_t size;
    std::mutex mtx;

    void resize(size_t new_capacity);

    public:
    HashMap();
    ~HashMap();

    void set(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    bool remove(const std::string& key);
    void print_map();
};

#endif