#include "random.h"

#include <thread>

#include "likely.h"

namespace benchmark {

Random* Random::GetTLSInstance() {
  static __thread Random* tls_instance;
  static __thread std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

  auto rv = tls_instance;
  if (UNLIKELY(rv == nullptr)) {
    size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    rv = new (&tls_instance_bytes) Random((uint32_t)seed);
    tls_instance = rv;
  }
  return rv;
}

}  // namespace benchmark
