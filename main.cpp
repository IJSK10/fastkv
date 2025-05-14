#include "hash_map_rcu.h"
#include "server.h"

int main() {
    HashMap hashmap;

    start_server(hashmap);

    return 0;
}
