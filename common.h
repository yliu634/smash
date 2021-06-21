#pragma once

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <chrono>
#include <cassert>
#include <cinttypes>
#include <sys/types.h>
#include <ctime>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <utility>
#include <sys/prctl.h>
#include <netdb.h>
#include <chrono>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sched.h>
#include <execinfo.h>


#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>

#include <functional>
#include <algorithm>
#include <iterator>
#include <utility>
#include <random>
#include <thread>
#include <mutex>

#include <queue>
#include <stack>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <tuple>
#include <vector>
#include <array>
#include <queue>
#include <set>
#include <list>
#include <forward_list>

#include "utils/disjointset.h"
#include "utils/hash.h"
#include "utils/lfsr64.h"
#include "utils/debugbreak.h"
//#include "utils/json.hpp"
#include "utils/hashutil.h"

//using json = nlohmann::json;

#ifndef NDEBUG
static const bool debugging = true;
#ifdef FULL_DEBUG
static const bool full_debug = true;
#else
static const bool full_debug = false;
#endif
#else
static const bool debugging =  false, full_debug = false;
#endif

inline uint64_t diff_us(timeval t1, timeval t2) {
  return ((t1.tv_sec - t2.tv_sec) * 1000000ULL + (t1.tv_usec - t2.tv_usec));
}

inline uint64_t diff_ms(timeval t1, timeval t2) {
  return diff_us(t1, t2) / 1000ULL;
}

std::string human(uint64_t word);

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#define COMPILER_BARRIER() asm volatile("" ::: "memory")

int stick_this_thread_to_core(int core_id);

void sync_printf(const char *format, ...);

void commonInit();

enum Distribution {
  zipfian,
  uniform
};

string toHex(const void *input, uint64_t size);

/** Zipf-like random distribution.
 *
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 */
template<class IntType = unsigned long, class RealType = double>
class zipf_distribution {
public:
  typedef RealType input_type;
  typedef IntType result_type;
  
  static_assert(std::numeric_limits<IntType>::is_integer, "");
  static_assert(!std::numeric_limits<RealType>::is_integer, "");
  
  zipf_distribution(const IntType n = std::numeric_limits<IntType>::max(),
                    const RealType q = 1.0)
      : n(n), q(q), H_x1(H(1.5) - 1.0), H_n(H(n + 0.5)), dist(H_x1, H_n) {}
  
  IntType operator()(std::default_random_engine &rng) {
    while (true) {
      const RealType u = dist(rng);
      const RealType x = H_inv(u);
      const IntType k = clamp<IntType>(std::round(x), 1, n);
      if (u >= H(k + 0.5) - h(k)) {
        return k;
      }
    }
  }

private:
  /** Clamp x to [min, max]. */
  template<typename T>
  static constexpr T clamp(const T x, const T min, const T max) {
    return std::max(min, std::min(max, x));
  }
  
  /** exp(x) - 1 / x */
  static double
  expxm1bx(const double x) {
    return (std::abs(x) > epsilon)
           ? std::expm1(x) / x
           : (1.0 + x / 2.0 * (1.0 + x / 3.0 * (1.0 + x / 4.0)));
  }
  
  /** H(x) = log(x) if q == 1, (x^(1-q) - 1)/(1 - q) otherwise.
   * H(x) is an integral of h(x).
   *
   * Note the numerator is one less than in the paper order to work with all
   * positive q.
   */
  const RealType H(const RealType x) {
    const RealType log_x = std::log(x);
    return expxm1bx((1.0 - q) * log_x) * log_x;
  }
  
  /** log(1 + x) / x */
  static RealType
  log1pxbx(const RealType x) {
    return (std::abs(x) > epsilon)
           ? std::log1p(x) / x
           : 1.0 - x * ((1 / 2.0) - x * ((1 / 3.0) - x * (1 / 4.0)));
  }
  
  /** The inverse function of H(x) */
  const RealType H_inv(const RealType x) {
    const RealType t = std::max(-1.0, x * (1.0 - q));
    return std::exp(log1pxbx(t) * x);
  }
  
  /** That hat function h(x) = 1 / (x ^ q) */
  const RealType h(const RealType x) {
    return std::exp(-q * std::log(x));
  }
  
  static constexpr RealType epsilon = 1e-8;
  
  IntType n;     ///< Number of elements
  RealType q;     ///< Exponent
  RealType H_x1;  ///< H(x_1)
  RealType H_n;   ///< H(n)
  std::uniform_real_distribution<RealType> dist;  ///< [H(x_1), H(n)]
};

class InputBase {
public:
  static Distribution distribution;
  static std::default_random_engine generator;
  static int bound;
  static zipf_distribution<int, double> expo;
  static std::uniform_int_distribution<int> unif;
  
  inline static void setSeed(uint32_t seed) {
    generator.seed(seed);
  }
  
  inline static uint32_t rand() {
    if (distribution == uniform) {
      return static_cast<uint32_t>(unif(generator)) % bound;
    } else {
      return static_cast<uint32_t>(expo(generator)) % bound;
    }
  }
};

template<typename InType,
    template<typename U, typename alloc = allocator<U>> class InContainer,
    typename OutType = InType,
    template<typename V, typename alloc = allocator<V>> class OutContainer = InContainer>
OutContainer<OutType>
mapf(const InContainer<InType> &input, function<OutType(const InType &)> func) {
  OutContainer<OutType> output;
  output.resize(input.size());
  transform(input.begin(), input.end(), output.begin(), func);
  return output;
}

template<typename InType,
    template<typename U, typename alloc = allocator<U>> class InContainer,
    typename OutType = InType,
    template<typename V, typename alloc = allocator<V>> class OutContainer = InContainer>
OutContainer<OutType>
mapf(const InContainer<InType> &input, function<OutType(const InType)> func) {
  OutContainer<OutType> output;
  output.resize(input.size());
  transform(input.begin(), input.end(), output.begin(), func);
  return output;
}

#define RESET   "\033[0m"
#define BLACK   "\033[30m"      /* Black */
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */
#define YELLOW  "\033[33m"      /* Yellow */
#define BLUE    "\033[34m"      /* Blue */
#define MAGENTA "\033[35m"      /* Magenta */
#define CYAN    "\033[36m"      /* Cyan */
#define WHITE   "\033[37m"      /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */
#define BOLDRED     "\033[1m\033[31m"      /* Bold Red */
#define BOLDGREEN   "\033[1m\033[32m"      /* Bold Green */
#define BOLDYELLOW  "\033[1m\033[33m"      /* Bold Yellow */
#define BOLDBLUE    "\033[1m\033[34m"      /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m"      /* Bold Magenta */
#define BOLDCYAN    "\033[1m\033[36m"      /* Bold Cyan */
#define BOLDWHITE   "\033[1m\033[37m"      /* Bold White */

const vector<const char *> colors = {
    GREEN,
    RED,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    WHITE,
//  BLACK,
//  BOLDBLACK,
    BOLDRED,
    BOLDGREEN,
    BOLDYELLOW,
    BOLDBLUE,
    BOLDMAGENTA,
    BOLDCYAN,
    BOLDWHITE,
};

class TeeOstream {
public:
  TeeOstream(string name = "/dev/null", const char *color = nullptr) : my_fstream(name), c(color) {}
  
  // for regular output of variables and stuff
  template<typename T>
  TeeOstream &operator<<(const T &something) {
    if (c) {
      std::cout << c << something << RESET;
    } else {
      std::cout << something;
    }
    cout.flush();
    my_fstream << something;
    return *this;
  }
  
  // for manipulators like std::endl
  typedef std::ostream &(*stream_function)(std::ostream &);
  
  TeeOstream &operator<<(stream_function func) {
    func(std::cout);
    func(my_fstream);
    return *this;
  }
  
  void flush() {
    my_fstream.flush();
    std::cout.flush();
  }

private:
  std::ofstream my_fstream;
  const char *c;
};

extern TeeOstream root;
extern vector<TeeOstream> ccouts;

template<typename K, typename V>
V &getWithDef(std::unordered_map<K, V> &m, const K &key, const V &defval) {
  typename std::unordered_map<K, V>::iterator it = m.find(key);
  if (it == m.end()) {
    m.insert(make_pair(key, defval));
    it = m.find(key);
  }
  
  return it->second;
}

class LockGetter {
public:
  recursive_mutex &lock;
  
  LockGetter(recursive_mutex &lock) : lock(lock) {
    lock.lock();
  }
  
  void release() {
    lock.unlock();
  }
};

class Clocker {
  static recursive_mutex lock;
  
  LockGetter lockGetter;
  
  thread::id id;
  
  int level;
  struct timeval start;
  string name;
  bool stopped = false;
  
  int laps = 0;
  uint64_t us = 0;
  
  TeeOstream &os;
  unordered_map<string, double> mem;

public:
  static unordered_map<thread::id, vector<Clocker *>> clockers;
  static unordered_map<thread::id, int> currentLevel;
  
  explicit Clocker(const string &name, TeeOstream *os = nullptr)
      : lockGetter(lock), name(name), id(this_thread::get_id()),
        level(getWithDef(currentLevel, id, 0)++),
        os(os ? *os : level ? clockers[id].back()->os : currentLevel.size() - 1 ? ccouts[(currentLevel.size() - 1) % ccouts.size()] : root) {
    if (level) { clockers[id].back()->os.flush(); }
    else { clockers[id] = {}; }
    
    ostringstream oss;
    for (int i = 0; i < level; ++i) oss << "| ";
    oss << "++";
    
    gettimeofday(&start, nullptr);
    oss << " [" << name << "]\n";
    this->os << oss.str();
    
    clockers[id].push_back(this);
    lockGetter.release();
  }
  
  void lap() {
    timeval end;
    gettimeofday(&end, nullptr);
    us += diff_us(end, start);
    
    output();
    
    laps++;
  }
  
  void stop() {
    lap();
    stopped = true;
    
    lock_guard lg(lock);
    Clocker::clockers[id].pop_back();
    currentLevel[id]--;
  }
  
  ~Clocker() {
    if (!stopped) {
      stop();
    }
    
    if (level == 0) {
      lock_guard lg(lock);
      Clocker::clockers.erase(id);
      currentLevel.erase(id);
    }
  }
  
  void output() {
    lock_guard lg(lock);
    ostringstream oss;
#ifdef PROFILE
    for (auto it = mem.begin(); it != mem.end(); ++it) {
      const string &solution = it->first;
      oss << pad() << "->" << "[" << solution << "] " << it->second << "\n";
    }
    os << oss.str();
    mem.clear();
    oss.str("");
    oss.clear();
#endif
    
    for (int i = 0; i < level; ++i) oss << "| ";
    oss << "--" << " [" << name << "]" << (laps ? "@" + to_string(laps) : "") << ": "
        << us / 1000 << "ms or " << us << "us\n";
    os << oss.str();
  }
  
  static inline void count(const string &solution, const string &type, double acc = 1) {
#ifdef PROFILE
    count(solution + " " + type, acc);
#endif
  }
  
  static inline void count(const string &solution, double acc = 1) {
#ifdef PROFILE
    count_(solution, acc);
#endif
  }
  
  static inline void count_(const string &solution, double acc = 1) {
#ifdef PROFILE
    lock_guard lg(lock);
    assertExistence(solution)[solution] += acc;
#endif
  }
  
  inline static unordered_map<string, double> &assertExistence(const string &solution) {
    thread::id id = this_thread::get_id();
    unordered_map<string, double> &mem1 = clockers[id].back()->mem;
    auto it = mem1.find(solution);
    if (it == mem1.end()) {
      mem1.insert(make_pair(solution, 0.0));
    }
    return mem1;
  }
  
  static inline double getCount(const string &solution, const string &type) {
    return getCount(solution + " " + type);
  }
  
  static inline void countMax(const string &solution, const string &type, double number) {
#ifdef PROFILE
    countMax(solution + " " + type, number);
#endif
  }
  
  static inline void countMin(const string &solution, const string &type, double number) {
#ifdef PROFILE
    countMin(solution + " " + type, number);
#endif
  }
  
  static inline double getCount(const string &solution) {
#ifdef PROFILE
    lock_guard lg(lock);
    return assertExistence(solution)[solution];
#else
    return 0;
#endif
  }
  
  static inline void countMax(const string &solution, double number) {
#ifdef PROFILE
    lock_guard lg(lock);
    auto &mem1 = assertExistence(solution);
    mem1[solution] = max(mem1[solution], number);
#endif
  }
  
  static inline void countMin(const string &solution, double number) {
#ifdef PROFILE
    lock_guard lg(lock);
    auto &mem1 = assertExistence(solution);
    mem1[solution] = min(mem1[solution], number);
#endif
  }
  
  string pad() const {
    ostringstream oss;
    for (int i = 0; i < level; ++i) oss << "| ";
    oss << " ";
    return oss.str();
  }
};

/** @brief A simple scoped lock type.
 *
 * A lock_guard controls mutex ownership within a scope, releasing
 * ownership in the destructor.
 */
template<typename Mutex>
class mylock_guard {
public:
  typedef Mutex mutex_type;
  
  inline explicit mylock_guard(mutex_type &_m) : M_device(_m) { M_device.lock(); }
  
  inline mylock_guard(mutex_type &_m, adopt_lock_t) noexcept: M_device(_m) {} // calling thread owns mutex
  
  inline void release() {
    released = true;
    M_device.unlock();
  }
  
  inline ~mylock_guard() {
    if (!released) release();
  }
  
  mylock_guard(const mylock_guard &) = delete;
  
  mylock_guard &operator=(const mylock_guard &) = delete;

private:
  mutex_type &M_device;
  bool released = false;
};


// C++ program to find Current Day, Date
// and Local Time
void printCurrentDateAndTime();

void error(const char *msg);
