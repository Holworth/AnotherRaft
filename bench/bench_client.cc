#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "client.h"
#include "config.h"
#include "kv_node.h"
#include "log_manager.h"
#include "rpc.h"
#include "util.h"

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

void ExecuteBench(kv::KvServiceClient* client, const std::vector<KvPair>& bench) {
  std::vector<uint64_t> lantency;
  for (const auto& p : bench) {
    auto start = std::chrono::high_resolution_clock::now();
    auto stat = client->Put(p.first, p.second);
    auto end = std::chrono::high_resolution_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    if (stat == kv::kOk) {
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

  int err_cnt = 0;
  // Check if inserted value can be found
  for (const auto& p : bench) {
    std::string get_val;
    auto stat = client->Get(p.first, &get_val);
    if (stat != kv::kOk || get_val != p.second) {
      ++err_cnt;
    }
  }
  printf("[Get Results][Error Count=%d]", err_cnt);
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "[Error] Expect at least one parameter, get" << argc << std::endl;
    return 0;
  }
  auto cluster_cfg = ParseConfigurationFile(std::string(argv[1]));
  int client_id = std::stoi(argv[2]);

  auto key_prefix = "key-" + std::to_string(client_id);
  auto value_prefix = "value-" + std::to_string(client_id) + "-";
  auto put_cnt = ParseCommandSize(std::string(argv[4]));
  auto val_size = ParseCommandSize(std::string(argv[3]));

  std::vector<KvPair> bench;
  auto bench_cfg = BenchConfiguration{key_prefix, value_prefix, put_cnt, val_size};
  BuildBench(bench_cfg, &bench);

  auto client = new kv::KvServiceClient(cluster_cfg, client_id);
  ExecuteBench(client, bench);

  delete client;
  return 0;
}
