#pragma once
#include <chrono>

namespace raft {
namespace util {
using std::chrono::microseconds;
using std::chrono::milliseconds;

using TimePoint = decltype(std::chrono::steady_clock::now());

class Timer {
 public:
  Timer() = default;
  ~Timer() = default;

  void Reset() { start_time_point_ = std::chrono::high_resolution_clock::now(); }

  int64_t ElapseMicroseconds() const {
    auto time_now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<microseconds>(time_now - start_time_point_).count();
  }

  int64_t ElapseMilliseconds() const {
    auto time_now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<milliseconds>(time_now - start_time_point_).count();
  }

 private:
  using TimePoint = decltype(std::chrono::high_resolution_clock::now());
  TimePoint start_time_point_;
};

enum LogMsgType { kRPC = 1, kRaft = 2, kEc = 3 };
class Logger {
  // Enable debug messages or not. False means all types
  // of messages will be ignored
#ifdef ENABLE_LOG
  static const bool debugFlag = true;
#else
  static const bool debugFlag = false;
#endif

  // On debugFlag = true, enable RPC related messages
  static const bool debugRPCFlag = false;

  // On debugRaftFlag = true, enable Raft logic related messages
  static const bool debugRaftFlag = true;

  // On debugECFlag = true, enable EC logic related messages, including
  // the parameter k, m change; the # of live servers change and so on
  static const bool debugECFlag = true;

 public:
  Logger() : startTimePoint_(std::chrono::steady_clock::now()) {}

  void Debug(LogMsgType type, const char* fmt, ...);

  // Reset the start timepoint of this debugger
  void Reset();

 private:
  decltype(std::chrono::steady_clock::now()) startTimePoint_;
  char buf[512]{};
};

inline TimePoint NowTime() {
  return std::chrono::steady_clock::now();
}

inline uint64_t DurationToMicros(TimePoint start, TimePoint end) {
  return std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
}

// Use singleton to access the global-only logger
Logger* LoggerInstance();
}  // namespace util
#define LOG(msg_type, format, ...)                  \
  {                                                 \
    auto logger = raft::util::LoggerInstance();     \
    logger->Debug(msg_type, format, ##__VA_ARGS__); \
  }
}  // namespace raft
