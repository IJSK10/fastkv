#include "hash_map.h"
#include <iostream>

HashMap::HashMap() : capacity(INITIAL_CAPACITY), size(0){
    table.resize(capacity,nullptr);
}

HashMap::~HashMap(){
    for (size_t i=0;i<capacity;i++)
    {
        Node* entry = table[i];
        while (entry)
        {
            Node* temp=entry;
            entry=entry->next;
            delete temp;
        }
    }
}

void HashMap::resize(size_t new_capacity)
{
    if (new_capacity<MIN_CAPACITY) return;

    std::vector<Node*> new_table(new_capacity,nullptr);

    for (size_t i=0;i<capacity;i++)
    {
        Node* entry = table[i];
        while(entry){
            Node* next = entry->next;
            size_t new_index= fnv1a_hash(entry->key) % new_capacity;

            entry->next = new_table[new_index];
            new_table[new_index] = entry;
            entry=next;
        }
    }

    table =std::move(new_table);
    capacity=new_capacity;
}

void HashMap::set(const std::string& key, const std::string&value){
    std::lock_guard<std::mutex> lock(mtx);

    size_t index = fnv1a_hash(key) % capacity;
    Node* entry=table[index];

    while (entry)
    {
        if (entry->key==key){
            entry->value=value;
            return;
        }
        entry=entry->next;
    }

    Node* new_node=new Node(key,value);
    new_node->next=table[index];
    table[index]=new_node;
    size++;

    if ((float)size / capacity > 0.75)
    {
        resize(capacity*2);
    }
}

std::string HashMap::get(const std::string&key)
{
    std::lock_guard<std::mutex> lock(mtx);

    size_t index=fnv1a_hash(key) % capacity;
    Node* entry=table[index];

    while (entry)
    {
        if(entry->key==key) return entry->key;
        entry=entry->next;
    }
    return "Key not found";
}

bool HashMap::remove(const std::string& key)
{
    std::lock_guard<std::mutex> lock(mtx);

    size_t index=fnv1a_hash(key)%capacity;
    Node* entry=table[index];
    Node* prev=nullptr;
    while (entry)
    {
        if(entry->key==key)
        {
            if (prev)
            {
                prev->next=entry->next;
            }
            else
            {
                table[index]=entry->next;
            }
            delete entry;
            size--;

            if ((float)size/capacity < 0.25 && capacity>MIN_CAPACITY){
                resize(capacity/2);
            }

            return true;
        }
        prev=entry;
        entry=entry->next;
    }
    return false;
}

void HashMap::print_map(){
    for (size_t i=0;i<capacity;i++)
    {
        std::cout<<"["<<i<<"]";
        Node* entry = table[i];

        while(entry)
        {
            std::cout<<"{"<<entry->key<<":"<<entry->value<<"}->";
            entry=entry->next;
        }
        std::cout<<"NULL\n";
    }
}