#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include "hash_map.h"
#include <nlohmann/json.hpp>

class Persistence{
    public:
        static bool saveToFile(const HashMap& map,const std::string& fileName);
        static bool loadFromFile(HashMap& map,const std::string& fileName);
};

#endif