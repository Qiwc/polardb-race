/*
    Sortlog in Memory
*/

#ifndef ENGINE_SORT_LOG_H
#define ENGINE_SORT_LOG_H

#include <iostream>
#include "params.h"
#include <vector>

using namespace std;

namespace polar_race {
    class SortLog {

    private:
        u_int64_t * keys;
        u_int16_t * values;
        u_int16_t nums;
        bool enlarge;
        bool isSwap;

    public:

        SortLog(u_int64_t * keys, u_int16_t * values) : nums(0), enlarge(false), isSwap(false), keys(keys), values(values) {
        }

        ~SortLog() {
            if (enlarge) {
                free(keys);
                free(values);
            }
        }

        int size() {
            return nums;
        }

        void swap() {
            if (!isSwap) {
                for (int i = 0; i < nums; i++) {
                    keys[i] = __builtin_bswap64(keys[i]);
                }
                isSwap = true;
            }
        }

        bool hasGreaterEqualKey(u_int64_t key) {
            return __builtin_bswap64(key) <= keys[nums - 1];
        }

        bool hasLessKey(u_int64_t key) {
            return __builtin_bswap64(key) > keys[0];
        }

        void put(u_int64_t &bigEndkey) {
            keys[nums] = __builtin_bswap64(bigEndkey);
            values[nums] = nums;
            nums++;

            if (nums == SORT_LOG_SIZE) {
                this->enlarge = true;
                sortLogEnlargeMtx.lock();
                auto * keysEnlarge = static_cast<u_int64_t *>(malloc(SORT_ENLARGE_SIZE * sizeof(u_int64_t)));
                auto * valuesEnlarge = static_cast<u_int16_t *>(malloc(SORT_ENLARGE_SIZE * sizeof(u_int16_t)));
                for (int i = 0; i < nums; i++) {
                    keysEnlarge[i] = keys[i];
                    valuesEnlarge[i] = values[i];
                }
                keys = keysEnlarge;
                values = valuesEnlarge;
                sortLogEnlargeMtx.unlock();
            }
        };

        void quicksort() {
            if (nums > 0) {
                quicksort(0, nums - 1);

                    u_int16_t k = 0;
                    for (int i = 0; i < nums; i++)
                        if (i == nums - 1 || keys[i] != keys[i + 1]) {
                            keys[k] = keys[i];
                            values[k] = values[i];
                            k++;
                        }
                    nums = k;

            }
        }

        void quicksort(int low, int high) {

            if ((high - low + 1) > MAX_LENGTH_INSERT_SORT) {
                // 待排序数组长度大于临界值，则进行快速排序
                int pivotLoc = partition(low, high);

                quicksort(low, pivotLoc - 1);
                quicksort(pivotLoc + 1, high);
            } else {
                // 2. 优化小数组时的排序方案，将快速排序改为插入排序
                insertSort(low, high);
            }
        }

        inline bool lessThan(int &a, int &b) {
            return (keys[a] < keys[b] || (keys[a] == keys[b] && values[a] < values[b]));
        }

        inline bool lessThanOrEqual(u_int64_t &key1, u_int16_t &value1, u_int64_t &key2, u_int16_t &value2) {
            return (key1 < key2 || (key1 == key2 && value1 <= value2));
        }

        inline bool lessThan(u_int64_t &key1, u_int16_t &value1, u_int64_t &key2, u_int16_t &value2) {
            return (key1 < key2 || (key1 == key2 && value1 < value2));
        }

        int partition(int low, int high) {

            // 1. 优化排序基准，使用三值取中获取中值
            medianOfThree(low, high);
            u_int64_t pivotKey = keys[low];
            u_int16_t pivotValue = values[low];

            while (low < high) { // 从数组的两端向中间扫描 // A
                while (low < high && lessThanOrEqual(pivotKey, pivotValue, keys[high], values[high])) { // B
                    high--;
                }

                keys[low] = keys[high];
                values[low] = values[high];
                while (low < high && lessThanOrEqual(keys[low], values[low], pivotKey, pivotValue)) { // D
                    low++;
                }

                keys[high] = keys[low];
                values[high] = values[low];
            }
            keys[low] = pivotKey;
            values[low] = pivotValue;

            return low; // 返回一趟下来后枢轴pivot所在的位置
        }


        inline void medianOfThree(int low, int high) {
            int mid = low + ((high - low) >> 1); // mid = low + (high-low)/2, 中间元素下标

            if (lessThan(high, mid))
                swap(mid, high);

            if (lessThan(mid, low))
                swap(low, mid);
        }

        inline void swap(int left, int right) {
            if (left == right) return;
            keys[left] ^= keys[right] ^= keys[left] ^= keys[right];
            values[left] ^= values[right] ^= values[left] ^= values[right];
        }

        void insertSort(int low, int high) {
            int i, j;
            for (i = low + 1; i <= high; i++) { // 从下标low+1开始遍历,因为下标为low的已经排好序
                if (lessThan(keys[i], values[i], keys[i - 1], values[i - 1])) {
                    // 如果当前下标对应的记录小于前一位记录,则需要插入,否则不需要插入，直接将记录数增加1
                    u_int64_t tmpKey = keys[i];
                    u_int16_t tmpValue = values[i];
                    for (j = i - 1; j >= low && lessThan(tmpKey, tmpValue, keys[j], values[j]); j--) {
                        keys[j + 1] = keys[j];
                        values[j + 1] = values[j];
                    }
                    // 插入正确位置
                    keys[j + 1] = tmpKey;
                    values[j + 1] = tmpValue;
                }
            }
        }


        int find(u_int64_t &bigEndkey) {

            u_int64_t key = __builtin_bswap64(bigEndkey);
            u_int64_t kk = key + 1;

            int left = 0;
            int right = nums;
            int middle;
            while (left < right) {
                middle = left + ((right - left) >> 1);
                if (keys[middle] < kk)
                    left = middle + 1;
                else
                    right = middle;
            }
            auto res = -1;
            if (left > 0 && keys[left - 1] == key)
                res = values[left - 1];
            return res;
        }

        int findValueByIndex(int index) {
            return values[index];
        }

        u_int64_t findKeyByIndex(int index) {
            return keys[index];
//            return __builtin_bswap64(keys[index]);
        }

        int getMinIndexGreaterEqualThan(u_int64_t key) {
            key = __builtin_bswap64(key);

            int left = 0;
            int right = nums - 1;
            int middle;
            while (left < right) {
                middle = (left + right) >> 1;
                if (key > keys[middle])
                    left = middle + 1;
                else
                    right = middle;
            }
            return right;
        }


        int getMaxIndexLessThan(u_int64_t key) {
            key = __builtin_bswap64(key);

            int left = 0;
            int right = nums - 1;
            while (left <= right) {
                int middle = (left + right) >> 1;
                if (key > keys[middle])
                    left = middle + 1;
                else
                    right = middle - 1;
            }
            return right;
        }

    };
}


#endif //ENGINE_SORT_LOG_H