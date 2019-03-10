#ifndef BENCHMARK_TESTUTIL_H_
#define BENCHMARK_TESTUTIL_H_

#include "random.h"
#include "include/polar_string.h"

namespace benchmark {

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern polar_race::PolarString RandomString(Random* rnd, int len, std::string* dst);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern polar_race::PolarString CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst);

}  // namespace benchmark

#endif  // BENCHMARK_TESTUTIL_H_
