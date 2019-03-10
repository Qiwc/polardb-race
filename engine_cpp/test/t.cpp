//
// Created by parallels on 11/16/18.
//

#include<iostream>
#include<stdio.h>
using namespace std;
unsigned int swapInt32(unsigned int value) {
    return ((value & 0x000000FF) << 24) |
           ((value & 0x0000FF00) << 8) |
           ((value & 0x00FF0000) >> 8) |
           ((value & 0xFF000000) >> 24);
}
int main()
{

    char*test1="12345678";
    u_int8_t* test = (u_int8_t*)test1;
    uint_fast64_t * uint1;
    uint1=(uint_fast64_t *)test;
    cout<<*uint1<<"\n";
    cout<<((*uint1)>>56)<<"\t"<<((uint64_t)test[0])<<"\n";

    uint64_t gai;
    gai = (((uint64_t)test[0])<<56) |
            (((uint64_t)test[1])<<48)|
            (((uint64_t)test[2])<<40)|
            (((uint64_t)test[3])<<32)|
            (((uint64_t)test[4])<<24)|
            (((uint64_t)test[5])<<16)|
            (((uint64_t)test[6])<<8)|
            (((uint64_t)test[7]));
    cout<<gai<<"\n";
    cout<<(gai>>56)<<"\t"<<((uint64_t)test[0])<<"\n";
}