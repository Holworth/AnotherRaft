#include "storage_engine.h"

#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>

using KvPair = std::pair<std::string, std::string>;

struct BenchConfiguration {
  std::string key_prefix;
  std::string value_prefix;
  int bench_put_cnt;
  int bench_put_size;
};

void BuildBench(const BenchConfiguration& cfg, std::vector<KvPair>* bench) {
  const std::string value_suffix(cfg.bench_put_size, 0);
  for (int i = 1; i <= cfg.bench_put_cnt; ++i) {
    auto key = cfg.key_prefix + std::to_string(i);
    auto val = cfg.value_prefix + std::to_string(i) + value_suffix;
    bench->push_back({key, val});
  }
}

void ExecuteBench(kv::StorageEngine* engine, const std::vector<KvPair>& bench) {
  std::vector<uint64_t> lantency;

  for (const auto& p : bench) {
    auto start = std::chrono::high_resolution_clock::now();
    auto stat = engine->Put(p.first, p.second);
    auto end = std::chrono::high_resolution_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    if (stat) {
      lantency.push_back(dura.count());  // us
    }
  }

  uint64_t latency_sum = 0;
  std::for_each(lantency.begin(), lantency.end(),
                [&latency_sum](uint64_t n) { latency_sum += n; });
  auto avg_lantency = latency_sum / lantency.size();
  auto max_lantency = *std::max_element(lantency.begin(), lantency.end());

  printf("[Results][Succ Cnt=%lu][Average Lantency = %llu us][Max Lantency = %llu us]\n",
         lantency.size(), avg_lantency, max_lantency);

  int succ_cnt = 0;
  // Check if inserted value can be found
  for (const auto& p : bench) {
    std::string get_val;
    auto stat = engine->Get(p.first, &get_val);
    if (stat && get_val == p.second) {
      ++succ_cnt;
    }
  }
  printf("[Get Results][Succ Count=%d]\n", succ_cnt);
}

int main(int argc, char* argv[]) {
  int valsize = std::stoi(argv[1]) * 1024;

  auto db = kv::StorageEngine::Default("./testdb");

  auto cfg = BenchConfiguration{"key-", "value-", 1000, valsize};
  std::vector<KvPair> bench;
  BuildBench(cfg, &bench);
  ExecuteBench(db, bench);
  delete db;
}
