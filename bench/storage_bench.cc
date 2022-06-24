#include <_types/_uint64_t.h>

#include <algorithm>
#include <chrono>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "storage.h"

class StorageBench {
 public:
  struct BenchmarkParameters {
    std::string storage_name;
    int entry_size;
    int entry_cnt;
  };

 public:
  StorageBench(const BenchmarkParameters& config) : config_(config) {
    if (config.storage_name == "raft") {
      storage_ = raft::PersistStorage::Open("raft.log");
    }
    latency_.reserve(config.entry_cnt);
  }

  void StartBench() {
    PrepareBenchmarkData();
    for (const auto& ent : data_) {
      auto start = std::chrono::high_resolution_clock::now();
      storage_->PersistEntries(1, 1, {ent});
      auto end = std::chrono::high_resolution_clock::now();
      auto dura = std::chrono::duration_cast<std::chrono::microseconds>(start - end);
      latency_.push_back(dura.count());
    }
  }

  void ShowBenchResults() {
    std::sort(latency_.begin(), latency_.end());
    uint64_t sum = 0;
    std::for_each(latency_.begin(), latency_.end(), [&sum](const auto& i) { sum += i; });
    std::cout << "[Max Latency]" << latency_.back() << "us\n"
    << "[Min Latency]" << latency_.front() << "us\n"
              << "[Avg Latency]" << sum / latency_.size() << std::endl;
  }

 private:
  void PrepareBenchmarkData() {
    for (int i = 0; i < config_.entry_cnt; ++i) {
      raft::LogEntry ent;
      ent.SetCommandData(raft::Slice(new char[config_.entry_size], config_.entry_size));
      data_.push_back(ent);
    }
  }

 private:
  raft::Storage* storage_;
  std::vector<uint64_t> latency_;
  std::vector<raft::LogEntry> data_;
  BenchmarkParameters config_;
};

int main(int argc, char* argv[]) {
  StorageBench::BenchmarkParameters config;
  if (std::string(argv[1]) == "raft") {
    config.storage_name = "raft";
  } else {
    config.storage_name = "leveldb";
  }
  config.entry_size = 4 * 1024;
  config.entry_cnt = 1000000;

  auto bench = new StorageBench(config);
  bench->StartBench();
  bench->ShowBenchResults();
}
