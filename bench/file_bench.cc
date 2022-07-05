#include <sys/fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <vector>

class FileBench {
  static constexpr const int kOpenBaseFlags = O_CLOEXEC;

 public:
  struct BenchmarkParameters {
    int entry_size;
    int entry_cnt;
  };

  struct Slice {
    Slice(char* data, size_t size) : data_(data), size_(size) {}
    char* data_;
    size_t size_;
  };

 public:
  FileBench(const BenchmarkParameters& config) : config_(config) {
    fd_ = ::open("testfile", O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd_ < 0) {
      std::cerr << "Create testfile failed" << std::endl;
      exit(1);
    }
  }

  ~FileBench() { close(fd_); }

  void Write(const Slice& slice) {
    auto write_data = slice.data_;
    auto write_size = slice.size_;
    while (write_size > 0) {
      auto size = ::write(fd_, write_data, write_size);
      write_data += size;
      write_size -= size;
    }
    fsync(fd_);
  }

  void StartBench() {
    PrepareBenchmarkData();
    std::cout << "[Start Running Benchmark]" << std::endl;
    for (const auto& ent : data_) {
      auto start = std::chrono::high_resolution_clock::now();
      Write(ent);
      auto end = std::chrono::high_resolution_clock::now();
      auto dura = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      latency_.push_back(dura.count());
    }
  }

  void ShowBenchResults() {
    std::sort(latency_.begin(), latency_.end());
    uint64_t sum = 0;
    std::for_each(latency_.begin(), latency_.end(), [&sum](const auto& i) { sum += i; });
    std::cout << "[Max Latency]" << latency_.back() << "us\n"
              << "[Min Latency]" << latency_.front() << "us\n"
              << "[Avg Latency]" << sum / latency_.size() << "us" << std::endl;
  }

 private:
  void PrepareBenchmarkData() {
    for (int i = 1; i <= config_.entry_cnt; ++i) {
      data_.push_back(Slice(new char[config_.entry_size], config_.entry_size));
    }
  }

 private:
  std::vector<uint64_t> latency_;
  std::vector<Slice> data_;
  BenchmarkParameters config_;
  int fd_;
};

int main(int argc, char* argv[]) {
  FileBench::BenchmarkParameters config;

  config.entry_size = std::atoi(argv[1]) * 1024;
  config.entry_cnt = std::atoi(argv[2]);

  auto bench = new FileBench(config);
  bench->StartBench();
  bench->ShowBenchResults();
}
