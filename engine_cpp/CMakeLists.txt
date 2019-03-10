cmake_minimum_required(VERSION 3.12)
project(engine_race)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)
include_directories(..)
include_directories(./engine_race/sparsepp)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(engine_race
#        test/benchmark.cc
        #        main.cc
#        test/t.cpp
        engine_race/engine_race.cc
        engine_race/engine_race.h
        engine_race/PEngine.h
#        engine_race/KeyLog.h
        engine_race/KeyValueLog.h
        engine_race/KVFiles.h
        engine_race/params.h
        engine_race/SortLog.h
        #        engine_race/SortArray.h
#        engine_race/CacheQueueCondition.h
#        engine_race/FrameCache.h
#        engine_race/HppcHashMap.h
#        engine_race/LongIntMapForRace.h
        CMakeLists.txt
        test/histogram.cc
        test/histogram.h
        test/likely.h
        test/random.cc
        test/random.h
        test/testutil.cc
        test/testutil.h
        test/test.cc

#        engine_race/SortLog.h
#        engine_race/sparsepp/spp.h
#        engine_race/sparsepp/spp_config.h
#        engine_race/sparsepp/spp_dlalloc.h
#        engine_race/sparsepp/spp_memory.h
#        engine_race/sparsepp/spp_smartptr.h
#        engine_race/sparsepp/spp_stdint.h
#        engine_race/sparsepp/spp_timer.h
#        engine_race/sparsepp/spp_traits.h
#        engine_race/sparsepp/spp_utils.h
        )
