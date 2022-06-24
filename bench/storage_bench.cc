#include <algorithm>
#include <chrono>
#include <cstdint>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"
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

  ~StorageBench() { delete storage_; }

  void StartBench() {
    PrepareBenchmarkData();
    std::cout << "[Start Running Benchmark]" << std::endl;
    for (const auto& ent : data_) {
      auto start = std::chrono::high_resolution_clock::now();
      storage_->PersistEntries(ent.Index(), ent.Index(), {ent});
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
      raft::LogEntry ent;
      ent.SetType(raft::kNormal);
      ent.SetIndex(static_cast<raft::raft_index_t>(i));
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

  config.entry_size = std::atoi(argv[2]) * 1024;
  config.entry_cnt = std::atoi(argv[3]);

  auto bench = new StorageBench(config);
  bench->StartBench();
  bench->ShowBenchResults();
}
