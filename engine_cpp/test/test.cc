#include <assert.h>
#include <stdio.h>
#include <string>
#include "../include/engine.h"

#include <thread>
#include <mutex>
#include <random>
#include <iostream>
#include <algorithm>
#include <set>

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

class DumpVisitor : public Visitor {
public:
    DumpVisitor(int* kcnt)
            : key_cnt_(kcnt) {}

    ~DumpVisitor() {}

    void Visit(const PolarString& key, const PolarString& value) {
        printf("Visit %s --> %s\n", key.data(), value.data());
        (*key_cnt_)++;
    }

private:
    int* key_cnt_;
};


template <typename T>
class threadsafe_vector : public std::vector<T>
{
public:
    void add(const T& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->push_back(val);
    }

    void add(T&& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->emplace_back(val);
    }

private:
    mutable std::mutex mMutex;
};

class RandNum_generator
{
private:
    RandNum_generator(const RandNum_generator&) = delete;
    RandNum_generator& operator=(const RandNum_generator&) = delete;
    std::uniform_int_distribution<unsigned> u;
    std::default_random_engine e;
    int mStart, mEnd;
public:
    // [start, end], inclusive, uniformally distributed
    RandNum_generator(int start, int end)
            : u(start, end)
            , e(std::hash<std::thread::id>()(std::this_thread::get_id())
                + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count())
            , mStart(start), mEnd(end)
    {}

    // [mStart, mEnd], inclusive
    unsigned nextNum()
    {
        return u(e);
    }

    // [0, max], inclusive
    unsigned nextNum(unsigned max)
    {
        return unsigned((u(e) - mStart) / float(mEnd - mStart) * max);
    }
};

std::string random_str(RandNum_generator& rng, std::size_t strLen)
{
    std::string rs(strLen, ' ');
    for (auto& ch : rs) {
        ch = rng.nextNum();
    }
    return rs;
}

typedef unsigned long long hash64_t;
hash64_t fnv1_hash_64(const std::string& str)
{
    static const hash64_t fnv_offset_basis = 14695981039346656037u;
    static const hash64_t fnv_prime = 1099511628211u;
    hash64_t hv = fnv_offset_basis;
    for (auto ch : str) {
        hv *= fnv_prime;
        hv ^= ch;
    }
    return hv;
}

std::string hash_to_str(hash64_t hash)
{
    const int cnt = 8;
    char val[cnt];
    for (int i = 0; i < cnt; ++i) {
        val[cnt - i - 1] = hash % 256;
        hash /= 256;
    }
    return std::string(val, cnt);
}

std::string key_from_value(const std::string& val)
{
    std::string key(8, ' ');

    key[0] = val[729];
    key[1] = val[839];
    key[2] = val[25];
    key[3] = val[202];
    key[4] = val[579];
    key[5] = val[1826];
    key[6] = val[369];
    key[7] = val[2903];

    return key;
}

void write(Engine* engine, threadsafe_vector<std::string>& keys, unsigned numWrite)
{
    RandNum_generator rng(0, 255);
    for (unsigned i = 0; i < numWrite; ++i) {
        std::string val(random_str(rng, 4096));

        //std::string key = hash_to_str(fnv1_hash_64(val)); // strong hash, slow but barely any chance to duplicate
        std::string key(key_from_value(val)); // random positions, faster but tiny chance to duplicate

        engine->Write(key, val);
        keys.add(key);
    }
}

void randomRead(Engine* engine, const threadsafe_vector<std::string>& keys, unsigned numRead)
{
    RandNum_generator rng(0, keys.size() - 1);
    for (unsigned i = 0; i < numRead; ++i) {
        auto& key = keys[rng.nextNum()];
        std::string val;
        engine->Read(key, &val);
        //if (key != hash_to_str(fnv1_hash_64(val))) {
        if (key != key_from_value(val)) {
            std::cout << "Random Read error: key and value not match" << std::endl;
            std::cout << key << std::endl;
            std::cout << key_from_value(val) << std::endl;

            exit(-1);
        }
    }
}

class MyVisitor : public Visitor
{
public:
    MyVisitor(const threadsafe_vector<std::string>& keys, unsigned start, unsigned& cnt)
            : mKeys(keys), mStart(start), mCnt(cnt)
    {}

    ~MyVisitor() {}

    void Visit(const PolarString& key, const PolarString& value)
    {
        //if (key != hash_to_str(fnv1_hash_64(value.ToString()))) {
        if (key != key_from_value(value.ToString())) {
            std::cout << "Sequential Read error: key and value not match" << std::endl;
            exit(-1);
        }
        if (key != mKeys[mStart + mCnt]) {
            std::cout << "Sequential Read error: not an expected key" << std::endl;
            exit(-1);
        }
        mCnt += 1;
    }

private:
    const threadsafe_vector<std::string>& mKeys;
    unsigned mStart;
    unsigned& mCnt;
};

void sequentialRead(Engine* engine, const threadsafe_vector<std::string>& keys)
{
//    RandNum_generator rng(0, keys.size() - 1);
//    RandNum_generator rng1(10, 100);

    unsigned lenKeys = keys.size();
    // Random ranges
//    unsigned lenAccu = 0;
//    while (lenAccu < lenKeys) {
//        std::string lower, upper;
//
//        unsigned start = rng.nextNum();
//        lower = keys[start];
//
//        unsigned len = rng1.nextNum();
//        if (start + len >= lenKeys) {
//            len = lenKeys - start;
//        }
//        if (start + len == lenKeys) {
//            upper = "";
//        } else {
//            upper = keys[start + len];
//        }
//
//        unsigned keyCnt = 0;
//        MyVisitor visitor(keys, start, keyCnt);
//        engine->Range(lower, upper, visitor);
//        if (keyCnt != len) {
//            std::cout << "Range size not match, expected: " << len
//                      << " actual: " << keyCnt << std::endl;
//            exit(-1);
//        }
//
//        lenAccu += len;
//    }

    // Whole range traversal
    unsigned keyCnt = 0;
    MyVisitor visitor(keys, 0, keyCnt);
    engine->Range("", "", visitor);
    if (keyCnt != lenKeys) {
        std::cout << "Range size not match, expected: " << lenKeys
                  << " actual: " << keyCnt << std::endl;
        exit(-1);
    }

    unsigned keyCnt2 = 0;
    MyVisitor visitor2(keys, 0, keyCnt2);
    engine->Range("", "", visitor2);
    if (keyCnt2 != lenKeys) {
        std::cout << "Range size not match, expected: " << lenKeys
                  << " actual: " << keyCnt2 << std::endl;
        exit(-1);
    }
}

int main()
{
    auto numThreads = std::thread::hardware_concurrency();
    numThreads=64;
    std::cout << numThreads << std::endl;

    Engine *engine = NULL;

    threadsafe_vector<std::string> keys;

    // Write
    unsigned numWrite = 1000, numKills = 1;
    double duration = 0;
    for (int nk = 0; nk < numKills; ++nk) {
        RetCode ret = Engine::Open(kEnginePath, &engine);
        assert (ret == kSucc);

        auto writeStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> writers;
        for (int i = 0; i < numThreads; ++i) {
            writers.emplace_back(std::thread(write, engine, std::ref(keys), numWrite / numKills));
        }
        for (auto& th : writers) {
            th.join();
        }
        writers.clear();

        auto writeEnd = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();

        delete engine;
    }

    std::cout << "Writing takes: "
              << duration
              << " milliseconds" << std::endl;


    RetCode ret = Engine::Open(kEnginePath, &engine);
    assert (ret == kSucc);

    std::cout << keys.size() << std::endl;
    std::sort(keys.begin(), keys.end());
    auto last = std::unique(keys.begin(), keys.end());
    keys.erase(last, keys.end());
    //std::cout << engine->size() << " == " << keys.size() << std::endl;

    // Random Read
    auto rreadStart = std::chrono::high_resolution_clock::now();

    unsigned numRead = 1000;
    std::vector<std::thread> rreaders;
    for (int i = 0; i < numThreads; ++i) {
        rreaders.emplace_back(std::thread(randomRead, engine, std::cref(keys), numRead));
    }
    for (auto& th : rreaders) {
        th.join();
    }
    rreaders.clear();

    auto rreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Random read takes: "
              << std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count()
              << " milliseconds" << std::endl;


    // Sequential Read
    auto sreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> sreaders;
    for (int i = 0; i < numThreads; ++i) {
        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
    }
    for (auto& th : sreaders) {
        th.join();
    }
    sreaders.clear();
//    for (int i = 0; i < numThreads; ++i) {
//        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
//    }
//    for (auto& th : sreaders) {
//        th.join();
//    }
//    sreaders.clear();

    auto sreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Sequential read takes: "
              << std::chrono::duration<double, std::milli>(sreadEnd - sreadStart).count()
              << " milliseconds" << std::endl;

    delete engine;

    return 0;
}
