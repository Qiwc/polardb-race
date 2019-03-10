#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <thread>
#include <sstream>
#include <chrono>
#include <sys/time.h>
#include <unordered_map>
#include <algorithm>
#include <cinttypes>
#include <dirent.h>
#include <cstdlib>
#include <sys/types.h>
#include <signal.h>
#include <cstring>
#include <mutex>
#include <condition_variable>

#include "testutil.h"
#include "histogram.h"
#include "include/engine.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillrandom    -- write N values in random key order in async mode
//      readseq       -- read N times sequentially
//      readrandom    -- read N times in random order
static const char* FLAGS_benchmarks =
    "fillrandom,"
    "readrandom,"
    "readseq"
    ;
// Use the db with the following name.
static const char* FLAGS_db = nullptr;

// Size of each key
static int32_t FLAGS_key_size = 8;

// Size of each value
static int32_t FLAGS_value_size = 4096;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 1;

// Seed base for random number generators. When 0 it is deterministic
static int32_t FLAGS_seed = 0;

// Number of key/values to place in database
static int32_t FLAGS_num = 100;

// Number of concurrent threads to run
static int32_t FLAGS_threads = 64;

// Time in seconds for the random-ops tests to run. When 0 then num
// determine the test duration
static int32_t FLAGS_duration = 0;

// Print histogram of operation timings
static int32_t FLAGS_histogram = 1;

// Drop page caches before each benchmark case;
static int32_t FLAGS_drop_caches = 1;

// For competion
static bool g_competion_mode = true;
static std::unordered_map<std::string, int32_t> g_opspersec_map;
static uint64_t g_max_memory = 0;
static uint64_t g_db_size = 0;
static float g_time_taken = 0;

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    benchmark::Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, FLAGS_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      benchmark::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  polar_race::PolarString Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return polar_race::PolarString(data_.data() + pos_ - len, len);
  }
};

static void AppendWithSpace(std::string* str, polar_race::PolarString msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

static uint64_t NowMicros() {
   struct timeval tv;
   gettimeofday(&tv, nullptr);
   return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

uint64_t GetRss() {
  int32_t page = sysconf(_SC_PAGESIZE);
  uint64_t rss;
  char buf[4096];
  char filename[256];
  int fd, count;
  char *p, *x;

  snprintf(filename, 256, "/proc/%d/stat", getpid());
  if ((fd = open(filename,O_RDONLY)) == -1) return 0;
  if (read(fd,buf,4096) <= 0) {
    close(fd);
    return 0;
  }
  close(fd);

  p = buf;
  count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
  while(p && count--) {
    p = strchr(p,' ');
    if (p) p++;
  }
  if (!p) return 0;
  x = strchr(p,' ');
  if (!x) return 0;
  *x = '\0';

  rss = strtoll(p,NULL,10);
  rss *= page;
  return rss;
}

// a class that reports max memory usage
class MemoryChecker {
 public:
  MemoryChecker(uint64_t report_interval_millisecs)
      : total_ops_done_(0),
        max_memory_rss_(0),
        report_interval_millisecs_(report_interval_millisecs),
        stop_(false) {

    reporting_thread_ = std::thread([&]() { SleepAndReport(); });
  }

  ~MemoryChecker() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
    g_max_memory = (g_max_memory >= max_memory_rss_ / (1024 * 1024)) ?
                    g_max_memory : max_memory_rss_ / (1024 * 1024);
    fprintf(stderr,
             "MemoryChecker report: TotalOpsDone: %lu, MaxMemoryRss: %lu KB\n", 
             total_ops_done_.load(), max_memory_rss_ / 1024);
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  void SleepAndReport() {
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk,
                       std::chrono::milliseconds(report_interval_millisecs_),
                       [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
        uint64_t cur_rss = GetRss();
        if (max_memory_rss_ < cur_rss) {
          max_memory_rss_ = cur_rss;
        }
      }
    }
  }

  std::atomic<int64_t> total_ops_done_;
  uint64_t max_memory_rss_;
  const uint64_t report_interval_millisecs_;
  std::thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kWrite, "write"},
  {kOthers, "op"}
};

class Stats {
 private:
  int id_;
  uint64_t start_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<benchmark::HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  std::string message_;
  bool exclude_from_merge_;
  MemoryChecker* memory_checker_;  // does not own

 public:
  Stats() { Start(-1); }

  void SetMemoryChecker(MemoryChecker* memory_checker) {
    memory_checker_ = memory_checker;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_)
      return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({ it->first, it->second });
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(polar_race::PolarString msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = NowMicros();
  }

  void FinishedOps(int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    memory_checker_->ReportFinishedOps(num_ops);

    uint64_t now = NowMicros();
    uint64_t micros = now - last_op_finish_;

    if (hist_.find(op_type) == hist_.end())
    {
      auto hist_temp = std::make_shared<benchmark::HistogramImpl>();
      hist_.insert({op_type, std::move(hist_temp)});
    }
    hist_[op_type]->Add(micros);

//    if (micros > 20000 && !FLAGS_stats_interval) {
//      fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
//      fflush(stderr);
//    }
    last_op_finish_ = now;

    done_ += num_ops;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const polar_race::PolarString& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_/elapsed;

    fprintf(stderr, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(),
            elapsed * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stderr, "Microseconds per %s:\n%s\n",
            OperationTypeString[it->first].c_str(),
            it->second->ToString().c_str());
      }
    }
    if (g_competion_mode) {
      g_opspersec_map[name.ToString()] = static_cast<int32_t>(throughput);
    }
    fflush(stderr);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  std::mutex mu;
  std::condition_variable cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  benchmark::Random64 rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  /* implicit */
  ThreadState(int index)
      : tid(index),
        rand((FLAGS_seed ? FLAGS_seed : 1000) + index) {
  }
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_ = 0;
    start_at_ = NowMicros();
  }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = 100;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_;
  uint64_t start_at_;
};

static bool DeleteDir(const char* path) {
  std::vector<std::string> files;
  DIR* d = opendir(path);
  if (d == nullptr) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        break;
      default:
        fprintf(stderr, "opendir(%s) error: %d\n", path, errno);
        return false;
    }
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != nullptr) {
    files.push_back(entry->d_name);
  }
  closedir(d);
  std::string file;
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i] == "." || files[i] == "..") {
      continue;
    }
    file = std::string(path) + "/" + files[i];
    if (unlink(file.c_str()) != 0) {
        fprintf(stderr, "unlink file(%s) error: %d\n", file.c_str(), errno);
        return false;
    }
  }

  if (rmdir(path) != 0) {
    fprintf(stderr, "rmdir path(%s) error\n", path);
    return false;
  }

  return true;
}

class Benchmark {
 private:
  int64_t num_;
  int32_t value_size_;
  int32_t key_size_;
  polar_race::Engine* db_;

  bool SanityCheck() {
    if (FLAGS_compression_ratio > 1) {
      fprintf(stderr, "compression_ratio should be between 0 and 1\n");
      return false;
    }
    return true;
  }

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stderr, "Keys:       %d bytes each\n", FLAGS_key_size);
    fprintf(stderr, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stderr, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stderr, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stderr, "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + FLAGS_value_size * FLAGS_compression_ratio)
              * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stderr, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stderr,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stderr,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static polar_race::PolarString TrimSpace(polar_race::PolarString s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit-1])) {
      limit--;
    }
    return polar_race::PolarString(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
#if defined(__linux)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        polar_race::PolarString key = TrimSpace(polar_race::PolarString(line,
                                                sep - 1 - line));
        polar_race::PolarString val = TrimSpace(polar_race::PolarString(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
      : num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        key_size_(FLAGS_key_size),
        db_(nullptr) {
  }

  ~Benchmark() {
    delete db_;
  }

  polar_race::PolarString AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return polar_race::PolarString(key_guard->get(), key_size_);
  }

  void GenerateKeyFromInt(uint64_t v, polar_race::PolarString* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
#if __BYTE_ORDER__==__ORDER_LITTLE_ENDIAN__
    for (int i = 0; i < bytes_to_fill; ++i) {
      pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
    }
#else
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
#endif
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }

  void Run() {
    if (!SanityCheck()) {
      exit(1);
    }

    polar_race::RetCode ret;
    PrintHeader();
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
      fprintf(stderr, "%s\n", name.c_str());
      // Sanitize parameters
      num_ = FLAGS_num;
      value_size_ = FLAGS_value_size;
      key_size_ = FLAGS_key_size;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      int num_threads = FLAGS_threads;
      bool fresh_db = false;

      if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readrandom") {
        method = &Benchmark::ReadRandom;
      } else if (!name.empty()) {  // No error message for empty name
        fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
        exit(1);
      }

      if (db_ != nullptr) {
        delete db_;
        db_ = nullptr;
      }
      if (fresh_db) {
        if (!DeleteDir(FLAGS_db)) {
          fprintf(stderr, "fresh db(%s) error\n", FLAGS_db);
          exit(1);
        }
      }
      ret = polar_race::Engine::Open(FLAGS_db, &db_);
      if (ret != polar_race::RetCode::kSucc) {
        fprintf(stderr, "open DB error: %d\n", ret);
        exit(1);
      }

      if (method != nullptr) {
        if (FLAGS_drop_caches) {
          fprintf(stderr, "Droping caches...\n");
          uint64_t start = NowMicros();
          std::system("echo 3 >/proc/sys/vm/drop_caches");
          fprintf(stderr, "Drop caches done, used: %lu us\n", NowMicros() - start);
        }

        fprintf(stderr, "DB path: [%s]\n", FLAGS_db);

        Stats stats = RunBenchmark(num_threads, name, method);
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    std::thread* thread_ptr;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      std::unique_lock<std::mutex> lk(shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.notify_all();
      }
      while (!shared->start) {
        shared->cv.wait(lk);
      }
    }

    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      std::unique_lock<std::mutex> lk(shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.notify_all();
      }
    }
  }

  Stats RunBenchmark(int n, polar_race::PolarString name,
                     void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    std::unique_ptr<MemoryChecker> memory_checker;
    memory_checker.reset(new MemoryChecker(100));

    ThreadArg* arg = new ThreadArg[n];

    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->stats.SetMemoryChecker(memory_checker.get());
      arg[i].thread->shared = &shared;
      arg[i].thread_ptr = new std::thread([&x = arg[i]]() { ThreadBody(&x); });
    }

    {
    std::unique_lock<std::mutex> lk(shared.mu);
    while (shared.num_initialized < n) {
      shared.cv.wait(lk);
    }

    shared.start = true;
    shared.cv.notify_all();
    while (shared.num_done < n) {
      shared.cv.wait(lk);
    }
    }
    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread);
  }

  class KeyGenerator {
   public:
    KeyGenerator(benchmark::Random64* rand,
        uint64_t num)
      : rand_(rand),
        num_(num),
        next_(0) {
    }

    uint64_t Next() {
      return rand_->Next() % num_;
    }

   private:
    benchmark::Random64* rand_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
  };

  void DoWrite(ThreadState* thread) {
    RandomGenerator gen;
    const int test_duration = 0;
    const int64_t num_ops = num_;

    std::unique_ptr<KeyGenerator> key_gens;
    int64_t max_ops = num_ops;

    Duration duration(test_duration, max_ops);
    key_gens.reset(new KeyGenerator(&(thread->rand), INT64_MAX));


    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    polar_race::PolarString key = AllocateKey(&key_guard);

    int64_t num_written = 0;
    polar_race::RetCode ret;


    while (!duration.Done(1)) {

      int64_t rand_num = key_gens->Next();
      GenerateKeyFromInt(rand_num, &key);
      ret = db_->Write(polar_race::PolarString(key.data(), key.size()),
          gen.Generate(value_size_));
      if (ret != polar_race::RetCode::kSucc) {
        fprintf(stderr, "write error: %d\n", ret);
        exit(1);
      }

      bytes += value_size_ + key_size_;
      ++num_written;

      thread->stats.FinishedOps(1, kWrite);
    }
    thread->stats.AddBytes(bytes);
  }

  class TestVisitor : public polar_race::Visitor {
   public:
    TestVisitor(ThreadState* thread) : num_(0), bytes_(0), thread_(thread) {
    };
    int64_t num() {
      return num_;
    }
    int64_t bytes() {
      return bytes_;
    }
    virtual void Visit(const polar_race::PolarString &key,
        const polar_race::PolarString &value) override {
      bytes_ += key.size() + value.size();
      num_++;
      thread_->stats.FinishedOps(1, kRead);
    }
   private:
    int64_t num_;
    int64_t bytes_;
    ThreadState* thread_;
  };

  void ReadSequential(ThreadState* thread) {
    TestVisitor test_visitor(thread);
    for (int32_t i = 0; i < 2; i++) {
      polar_race::RetCode ret = db_->Range("", "", test_visitor);
      if (ret != polar_race::RetCode::kSucc) {
          fprintf(stderr, "range error: %d\n", ret);
          exit(1);
      }
      thread->stats.AddBytes(test_visitor.bytes());
    }
  }

  void ReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    std::unique_ptr<KeyGenerator> key_gens;
    key_gens.reset(new KeyGenerator(&(thread->rand), INT64_MAX));

    std::unique_ptr<const char[]> key_guard;
    polar_race::PolarString key = AllocateKey(&key_guard);
    std::string val;
    polar_race::RetCode ret;

    Duration duration(FLAGS_duration, num_);
    while (!duration.Done(1)) {
      int64_t key_rand = key_gens->Next();
      GenerateKeyFromInt(key_rand, &key);
      read++;
      ret = db_->Read(key.ToString(), &val);
      if (ret == polar_race::RetCode::kSucc) {
        found++;
        bytes += key.size() + val.size();
      } else if (ret != polar_race::RetCode::kNotFound) {
        fprintf(stderr, "Read returned an error: %d\n", ret);
        exit(1);
      }

      thread->stats.FinishedOps(1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n",
             found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

};

static int32_t GetTestDirectory(std::string* result) {
  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *result = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/tmp/benchmark-%d", int(geteuid()));
    *result = buf;
  }
  return mkdir(result->c_str(), 0755);
}

void CalculateFile(char* dir) {
  DIR *dp;
  struct stat statbuff;
  struct dirent *entry;

  if ((dp = opendir(dir)) == NULL) {
    return;
  }

  chdir(dir);
  // The root directory
  //lstat(dir, &statbuff);
  //g_db_size += statbuff.st_size;

  while ((entry = readdir(dp)) != NULL) {
    lstat(entry->d_name, &statbuff);
    if (S_ISDIR(statbuff.st_mode)) {
      if (strcmp(".", entry->d_name) == 0 ||
          strcmp("..", entry->d_name) == 0) {
        continue;
      }
      g_db_size += statbuff.st_size;
      CalculateFile(entry->d_name);
    } else {
      g_db_size += statbuff.st_size;
    }
  }
  chdir("..");
  closedir(dp);
}

void CalculateDB() {
  CalculateFile(const_cast<char*>(FLAGS_db));
  if (g_db_size != 0) {
    // humanize
    g_db_size = g_db_size / (1024 * 1024);
  }
}

void CalculateResult() {
  fprintf(stderr, "\n------------------------------------------------\n");
  fprintf(stderr, "!!!Competion Report!!!\n");
  for (auto& ops : g_opspersec_map) {
    fprintf(stderr, "   %16s: %d ops/second\n", ops.first.c_str(), ops.second);
  }

  fprintf(stderr,"time taken: %f s\n", g_time_taken);
  fprintf(stderr,"max memory usage: %lu MB\n", g_max_memory);
  fprintf(stderr,"disk usage: %lu MB\n", g_db_size);
  fprintf(stderr, "------------------------------------------------\n");
}

int main(int argc, char** argv) {
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (polar_race::PolarString(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
      g_competion_mode = false;
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1) {
      FLAGS_key_size = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--stats_seed=%d%c", &n, &junk) == 1) {
      FLAGS_seed = n;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
                (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--drop_caches=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_drop_caches = n;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      GetTestDirectory(&default_db_path);
      FLAGS_db = default_db_path.c_str();
  }

  Benchmark benchmark;
  auto start = std::chrono::system_clock::now();
  benchmark.Run();
  auto end = std::chrono::system_clock::now();
  auto dur = end - start;
  typedef std::chrono::duration<float> float_seconds;
  auto secs = std::chrono::duration_cast<float_seconds>(dur);
  g_time_taken = secs.count();

  if (g_competion_mode) {
    CalculateDB();
    CalculateResult();
  }
  return 0;
}
