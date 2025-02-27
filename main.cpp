#include "hash_map.h"
#include "server.h"
// #include <iostream>
// #include <chrono>
// #include <thread>
// #include <vector>
// #include <random>
// #include <string>

// void testHashMap() {
//     std::cout << "Starting HashMap test..." << std::endl;
    
//     // Create a HashMap instance
//     HashMap map;
    
//     // Test basic set and get operations
//     std::cout << "\n--- Basic Operations ---" << std::endl;
//     map.set("key1", "value1");
//     map.set("key2", "value2");
//     map.set("key3", "value3");
    
//     std::cout << "key1: " << map.get("key1") << std::endl;
//     std::cout << "key2: " << map.get("key2") << std::endl;
//     std::cout << "key3: " << map.get("key3") << std::endl;
//     std::cout << "nonexistent: " << map.get("nonexistent") << std::endl;
    
//     // Test TTL (Time To Live)
//     std::cout << "\n--- TTL Test ---" << std::endl;
//     map.set("expiring", "This will expire", 2); // Expires in 2 seconds
//     std::cout << "expiring (before): " << map.get("expiring") << std::endl;
//     std::cout << "Waiting for expiration..." << std::endl;
//     std::this_thread::sleep_for(std::chrono::seconds(3));
//     std::cout << "expiring (after): " << map.get("expiring") << std::endl;
    
//     // Test removal
//     std::cout << "\n--- Removal Test ---" << std::endl;
//     std::cout << "Removing key2: " << (map.remove("key2") ? "success" : "failed") << std::endl;
//     std::cout << "key2 after removal: " << map.get("key2") << std::endl;
    
//     // Test LRU eviction by filling up to MAX_CAPACITY
//     std::cout << "\n--- LRU Eviction Test ---" << std::endl;
//     std::cout << "Adding " << (MAX_CAPACITY + 10) << " items to trigger LRU eviction..." << std::endl;
    
//     // Generate random keys and values
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_int_distribution<> dis(1000, 9999);
    
//     // Add items to fill beyond capacity
//     for (int i = 0; i < MAX_CAPACITY + 10; i++) {
//         std::string key = "lru_key_" + std::to_string(i);
//         std::string value = "value_" + std::to_string(dis(gen));
//         map.set(key, value);
        
//         // Access some keys more frequently to affect LRU order
//         if (i % 100 == 0) {
//             for (int j = 0; j < 5; j++) {
//                 map.get(key); // Access this key multiple times
//             }
//         }
//     }
    
//     // Check if early keys were evicted
//     std::cout << "Checking if early keys were evicted by LRU..." << std::endl;
//     int evicted = 0;
//     int retained = 0;
    
//     for (int i = 0; i < MAX_CAPACITY + 10; i++) {
//         std::string key = "lru_key_" + std::to_string(i);
//         std::string result = map.get(key);
        
//         if (result == "Key not found") {
//             evicted++;
//         } else {
//             retained++;
//         }
//     }
    
//     std::cout << "Evicted keys: " << evicted << std::endl;
//     std::cout << "Retained keys: " << retained << std::endl;
    
//     // Test concurrent operations
//     std::cout << "\n--- Concurrent Operations Test ---" << std::endl;
//     std::vector<std::thread> threads;
    
//     for (int i = 0; i < 5; i++) {
//         threads.emplace_back([&map, i]() {
//             for (int j = 0; j < 100; j++) {
//                 std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
//                 map.set(key, "thread_value_" + std::to_string(j));
//                 map.get(key);
                
//                 if (j % 10 == 0) {
//                     map.remove(key);
//                 }
//             }
//         });
//     }
    
//     for (auto& t : threads) {
//         t.join();
//     }
    
//     std::cout << "Concurrent operations completed successfully" << std::endl;
    
//     std::cout << "\nHashMap test completed!" << std::endl;
// }

int main() {
    HashMap hashmap;

    start_server(hashmap);

    return 0;
}
