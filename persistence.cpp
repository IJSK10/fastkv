#include "persistence.h"
#include "hash_map_rcu.h"     
#include <nlohmann/json.hpp>  
#include <fstream>            
#include <iostream>          
#include <vector>
#include <string>
#include <ctime>              
.
bool Persistence::saveToFile(const HashMap& map, const std::string& fileName) {
    nlohmann::json j;
    try {
        std::vector<HashMap::NodeData> allData = map.getAllForPersistence();

        for (const auto& itemData : allData) {
            // Store data in the JSON object
            j[itemData.key] = {
                {"value", itemData.value},
                {"expiry", itemData.expiry},
                {"lastAccessed", itemData.lastAccessed}
            };
        }

        std::ofstream outfile(fileName);
        if (!outfile.is_open()) {
            std::cerr << "Persistence Error: Failed to open file for writing: " << fileName << std::endl;
            return false;
        }

        outfile << j.dump(4); 
        outfile.close();

        if (outfile.fail()) { 
            std::cerr << "Persistence Error: Failed to write all data to file: " << fileName << std::endl;
            return false;
        }

        
        return true;

    } catch (const nlohmann::json::exception& e) {
        std::cerr << "Persistence JSON Error during saveToFile: " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Persistence System Error during saveToFile: " << e.what() << std::endl;
        return false;
    }
}


bool Persistence::loadFromFile(HashMap& map, const std::string& fileName) {
    std::ifstream infile(fileName);
    if (!infile.is_open()) {
        
        return true; 
    }

    nlohmann::json j;
    try {
        infile >> j;
        infile.close();

        for (auto it = j.begin(); it != j.end(); ++it) {
            const std::string& key = it.key();
            const auto& itemDataJson = it.value(); 


            if (!itemDataJson.is_object()) {
                std::cerr << "Persistence Warning: Skipping non-object item for key '" << key << "' in " << fileName << std::endl;
                continue;
            }

            std::string value = itemDataJson.value("value", ""); 
            time_t expiry_timestamp = itemDataJson.value("expiry", (time_t)0); 
            int ttl = 0;
            if (expiry_timestamp > 0) {
                time_t now = time(nullptr);
                if (expiry_timestamp > now) {
                    ttl = static_cast<int>(expiry_timestamp - now);
                } else {
                    continue;
                }
            }
            map.set(key, value, ttl); 
        }
        
        return true;

    } catch (const nlohmann::json::exception& e) {
        std::cerr << "Persistence JSON Error during loadFromFile: " << e.what() << std::endl;
        if (infile.is_open()) infile.close();
        return false; 
    } catch (const std::exception& e) {
        std::cerr << "Persistence System Error during loadFromFile: " << e.what() << std::endl;
        if (infile.is_open()) infile.close();
        return false; 
    }
}
