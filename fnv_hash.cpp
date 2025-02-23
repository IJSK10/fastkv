#include "fnv_hash.h"

const size_t FNV_PRIME = 16777619ULL;
const size_t OFFSET_BASIS = 2166136261ULL;

size_t fnv1a_hash(const std::string& key)
{
    size_t hash = OFFSET_BASIS;
    for (char c: key)
    {
        hash ^=c;
        hash *=FNV_PRIME;
    }
    return hash;
}