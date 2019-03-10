/*
    Key Value Log
*/

#ifndef ENGINE_KEYVALUELOG_H
#define ENGINE_KEYVALUELOG_H

#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "../include/polar_string.h"
#include "../include/engine.h"
#include "params.h"

class KeyValueLog {
private:
    int id;
    string path;

    //对应ValueFile
    int fd;
    size_t filePosition;
    size_t globalOffset;

    //对应KeyFile
    size_t cacheBufferPosition;
    char *cacheBuffer;
    size_t keyBufferPosition;
    u_int64_t *keyBuffer;

    //是否扩容
    bool enlarge;

public:
    KeyValueLog(const std::string &path, const int &id, const int &fd, const size_t &globalOffset, char *cacheBuffer, u_int64_t *keyBuffer) :
        id(id), path(path), fd(fd), filePosition(0), globalOffset(globalOffset), cacheBufferPosition(0),
        cacheBuffer(cacheBuffer), keyBufferPosition(0), keyBuffer(keyBuffer), enlarge(false){

        std::ostringstream fp;
        fp << path << "/enlarge-" << id;
        string filePath = fp.str();

        //如果存在扩容文件
        if (access(filePath.data(), 0) != -1) {
            valueLogEnlargeMtx.lock();
            this->enlarge = true;
            this->fd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            fallocate(this->fd, 0, 0, VALUE_ENLARGE_SIZE + KEY_ENLARGE_SIZE);
            this->globalOffset = 0;
            this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, KEY_ENLARGE_SIZE, PROT_READ | PROT_WRITE,
                                                            MAP_SHARED | MAP_POPULATE, this->fd,
                                                            (off_t) VALUE_ENLARGE_SIZE));
            valueLogEnlargeMtx.unlock();
        }
    }

    ~KeyValueLog() {

        //close时，将block中未写入valueFile部分全部刷盘
        if (this->cacheBufferPosition != 0) {
            auto remainSize = cacheBufferPosition << 12;
            pwrite(this->fd, cacheBuffer, remainSize, globalOffset + filePosition);
        }
        if (this->enlarge) {
            munmap(keyBuffer, KEY_ENLARGE_SIZE);
            close(this->fd);
        }
    }

    size_t size() {
        return filePosition + (cacheBufferPosition << 12);
    }

    inline void putValueKey(const char *value, const char * key) {
        memcpy(cacheBuffer + (cacheBufferPosition << 12), value, 4096);
        cacheBufferPosition++;

        //value缓存达到缓存块大小就刷盘
        if (cacheBufferPosition == PAGE_PER_BLOCK) {
            pwrite(this->fd, cacheBuffer, BLOCK_SIZE, globalOffset + filePosition);
            filePosition += BLOCK_SIZE;
            cacheBufferPosition = 0;

            //自动扩容
            if (filePosition + BLOCK_SIZE > VALUE_LOG_SIZE && filePosition <= VALUE_LOG_SIZE) {
                valueLogEnlargeMtx.lock();
                this->enlarge = true;
                std::ostringstream fp;
                fp << path << "/enlarge-" << id;
                int fdEnlarge = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
                fallocate(fdEnlarge, 0, 0, VALUE_ENLARGE_SIZE + KEY_ENLARGE_SIZE);
                auto * keyBufferEnlarge = static_cast<u_int64_t *>(mmap(nullptr, KEY_ENLARGE_SIZE, PROT_READ | PROT_WRITE,
                                                                MAP_SHARED | MAP_POPULATE, fdEnlarge,
                                                                (off_t) VALUE_ENLARGE_SIZE));
                // 复制原来的log内容到新的文件
                for (int pos = 0; pos < filePosition; pos += BLOCK_SIZE) {
                    pread(this->fd, cacheBuffer, BLOCK_SIZE, globalOffset + pos);
                    pwrite(fdEnlarge, cacheBuffer, BLOCK_SIZE, pos);
                }
                memcpy(keyBufferEnlarge, keyBuffer, KEY_LOG_SIZE);
                this->fd = fdEnlarge;
                this->globalOffset = 0;
                this->keyBuffer = keyBufferEnlarge;
                valueLogEnlargeMtx.unlock();
            }
        }

        //写入key
        *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
        keyBufferPosition++;
    }

    inline void readValue(int index, char *value) {
        pread(this->fd, value, 4096, globalOffset + (index << 12));
    }

    inline void readValue(size_t offset, char *value, size_t size) {
        pread(this->fd, value, size, globalOffset + offset);
    }

    //再次open时恢复写的位置
    void recover(size_t sum) {
        this->keyBufferPosition = sum;
        this->filePosition = sum << 12;
    }

    inline bool getKey(u_int64_t & key) {
        key = *(keyBuffer + keyBufferPosition);
        keyBufferPosition++;
        return key != 0;
    }
};


#endif //ENGINE_KEYVALUELOG_H
