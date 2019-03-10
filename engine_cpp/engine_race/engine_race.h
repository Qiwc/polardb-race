// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <string>
#include <sys/stat.h>
#include "PEngine.h"
#include "../include/engine.h"

namespace polar_race {

    class EngineRace : public Engine {
    public:
        static RetCode Open(const std::string &name, Engine **eptr);

        explicit EngineRace(const std::string &dir) {
           this->pEngine = new PEngine(dir);
        }

        ~EngineRace();

        RetCode Write(const PolarString &key,
                      const PolarString &value) override;

        RetCode Read(const PolarString &key,
                     std::string *value) override;

        /*
         * NOTICE: Implement 'Range' in quarter-final,
         *         you can skip it in preliminary.
         */
        RetCode Range(const PolarString &lower,
                      const PolarString &upper,
                      Visitor &visitor) override;

    private:
        PEngine *pEngine;

    };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
