/*
    param
*/

#ifndef ENGINE_PARAMS_H
#define ENGINE_PARAMS_H

#include <string>
#include <mutex>

using namespace std;
//range次数
const int MAX_RANGE_COUNT = 2;

//Log数量,大小以及每个Log的键值容量
const int LOG_NUM = 4096;
const int NUM_PER_SLOT = 128;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT << 12;  //128mb
const size_t KEY_LOG_SIZE = NUM_PER_SLOT << 3;

//文件数量
const int FILE_NUM = 64;

//每个sortlog的容量
const int SORT_LOG_SIZE = NUM_PER_SLOT;

//文件自动扩充后的大小
const int KEY_ENLARGE_SIZE = 20010 * 8;
const int VALUE_ENLARGE_SIZE = 20010 * 4096;
const int SORT_ENLARGE_SIZE = 20010;

//range阶段value缓存的相关参数
const size_t CACHE_SIZE = VALUE_LOG_SIZE;
const int CACHE_NUM = 16;
const int ACTIVE_CACHE_NUM = 8;
const int RESERVE_CACHE_NUM = CACHE_NUM - ACTIVE_CACHE_NUM;
const int PREPARE_CACHE_NUM = 4;

//写入阶段的Block参数
const int PAGE_PER_BLOCK = 4;
const size_t BLOCK_SIZE = PAGE_PER_BLOCK << 12;

//open阶段恢复内存的线程数以及range阶段读磁盘的线程数
const int RECOVER_THREAD = 64;
const int READDISK_THREAD = 2;

//排序阶段阈值，将快排变为插入排序
const int MAX_LENGTH_INSERT_SORT = 12;

//用来判断第三阶段range的阈值
const int RANGE_THRESHOLD = 10000000;

//文件自动扩容的锁
std::mutex sortLogEnlargeMtx;
std::mutex valueLogEnlargeMtx;

//锁
struct PMutex {
    pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
    void lock() { pthread_mutex_lock(&m); }
    void unlock() { pthread_mutex_unlock(&m); }
};

//条件变量
struct PCond {
    PMutex pm;
    pthread_cond_t c = PTHREAD_COND_INITIALIZER;
    void lock() { pm.lock(); }
    void unlock() { pm.unlock(); }
    void wait() {
        pthread_cond_wait(&c, &pm.m);
    }
    void notify_all() {
        pthread_cond_broadcast(&c);
    }
    void notify_one() {
        pthread_cond_signal(&c);
    }
};

#endif //ENGINE_PARAMS_H
