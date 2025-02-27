#include "persistence.h"
#include <fstream>
#include <iostream>

bool Persistence::saveToFile(const HashMap& map, const std::string& fileName)
{
    nlohmann::json j;
    try {
        // Limit how many items we save
        // const size_t MAX_ITEMS_TO_SAVE = 10000;
        auto allData = map.getAll();
        // if (allData.size() > MAX_ITEMS_TO_SAVE) {
        //     allData.resize(MAX_ITEMS_TO_SAVE);
        // }
        for (const auto&pair : allData)
        {
            j[pair.first]["value"]=pair.second.value;
            j[pair.first]["expiry"]=pair.second.expiry;
        }

        std::ofstream outfile(fileName);
        if (!outfile.is_open())
        {
            std::cerr<<"Failed to open file for writing"<<std::endl;
            return false;
        }
        outfile<<j.dump(4);
        outfile.close();
    } catch (const std::exception& e) {
        std::cerr << "Error saving to file: " << e.what() << std::endl;
    }
    return true;
}

bool Persistence::loadFromFile(HashMap& map,const std::string& fileName)
{
    std::ifstream infile(fileName);
    if(!infile.is_open())
    {
        std::cerr<<"falied to open file for reading"<<std::endl;
        return false;
    }
    nlohmann::json j;
    infile >> j;
    for (auto& [key,value]:j.items())
    {
        std::string val=value["value"];
        time_t expiry = value.contains("expiry") ? static_cast<time_t>(value["expiry"].get<std::int64_t>()) : 0;
        map.set(key, val, static_cast<int>(expiry));
    }

    infile.close();
    return true;
}

