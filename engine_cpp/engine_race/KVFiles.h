/*
    Key Value File
*/

#ifndef ENGINE_VALUELOGFILE_H
#define ENGINE_VALUELOGFILE_H


#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "../include/engine.h"

namespace polar_race {

    class KVFiles {
    private:
        //value文件
        int valueFd;
        size_t valueFileSize;

        //key文件以及写入缓存
        int mapFd;
        u_int64_t * keyBuffer;
        size_t keyFileSize;
        char * blockBuffer;
        size_t blockFileSize;

    public:
        KVFiles(const std::string &path, const int &id, const bool &exist, const size_t &valueFileSize, const size_t &keyFileSize, const size_t &blockFileSize)
            : valueFileSize(valueFileSize), keyFileSize(keyFileSize), blockFileSize(blockFileSize){
            //Value Log, DIO
            std::ostringstream fp;
            fp << path << "/value-" << id;
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            if (exist)
                fallocate(this->valueFd, 0, 0, valueFileSize);

            //Key Log and BlockBuffer, MMAP
            std::ostringstream mp;
            mp << path << "/map-" << id;
            this->mapFd = open(mp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            ftruncate(this->mapFd, keyFileSize + blockFileSize);
            this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, keyFileSize, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK, this->mapFd,
                                                            0));
            this->blockBuffer = static_cast<char *>(mmap(nullptr, blockFileSize, PROT_READ | PROT_WRITE,
                                                            MAP_PRIVATE | MAP_POPULATE | MAP_NONBLOCK, this->mapFd,
                                                            keyFileSize));
        }

        ~KVFiles() {
            munmap(blockBuffer, this->blockFileSize);
            munmap(keyBuffer, this->keyFileSize);
            close(this->mapFd);
            close(this->valueFd);
        }


        int getValueFd() const {
            return valueFd;
        }

        u_int64_t *getKeyBuffer() const {
            return keyBuffer;
        }

        char *getBlockBuffer() const {
            return blockBuffer;
        }
    };
}


#endif //ENGINE_VALUELOGFILE_H
