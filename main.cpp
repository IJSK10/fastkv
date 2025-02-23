#include "hash_map.h"
#include <iostream>

int main(){
    HashMap mp;

    mp.set("name","Alice");
    mp.set("Age","23");
    mp.set("city","New York");

    std::cout<<mp.get("name")<<std::endl;
    std::cout<<mp.get("ans")<<std::endl;

    mp.print_map();

    return 0;
}