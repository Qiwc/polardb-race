// Copyright [2018] Alibaba Cloud All rights reserved
#include <sys/stat.h>
#include "engine_race.h"

namespace polar_race {

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        struct stat st = {0};
        if(stat(name.data(), &st) == -1) {
            mkdir(name.data(), 0777);
        }

        fprintf(stderr, "Opening database!\n");

        auto *engine_race = new EngineRace(name);

        *eptr = engine_race;
        return kSucc;
    }

// 2. Close engine
    EngineRace::~EngineRace() {
        delete pEngine;
        fprintf(stderr, "closing database\n");
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        pEngine->put(key, value);
        return kSucc;
    }

// 4. Read value of a key
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        return pEngine->read(key, value);
    }

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {
        return pEngine->range(lower, upper, visitor);
    }

}  // namespace polar_race
