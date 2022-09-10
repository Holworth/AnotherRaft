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
#include "type.h"
#include "util.h"

using KvPair = std::pair<std::string, std::string>;
const int kVerboseInterval = 100;

struct BenchConfiguration {
  std::string key_prefix;
  std::string value_prefix;
  int bench_put_cnt;
  int bench_put_size;
};

struct AnalysisResults {
  uint64_t avg;
  uint64_t max;
};

AnalysisResults Analysis(const std::vector<uint64_t>& colleced_data) {
  uint64_t data_sum = 0;
  std::for_each(colleced_data.begin(), colleced_data.end(),
                [&data_sum](uint64_t n) { data_sum += n; });
  auto avg_lantency = data_sum / colleced_data.size();
  auto max_lantency = *std::max_element(colleced_data.begin(), colleced_data.end());

  return {avg_lantency, max_lantency};
}

void BuildBench(const BenchConfiguration& cfg, std::vector<KvPair>* bench) {
  const std::string value_suffix(cfg.bench_put_size, 0);
  for (int i = 1; i <= cfg.bench_put_cnt; ++i) {
    auto key = cfg.key_prefix + std::to_string(i);
    auto val = cfg.value_prefix + std::to_string(i) + value_suffix;
    bench->push_back({key, val});
  }
}

void ExecuteBench(kv::KvServiceClient* client, const std::vector<KvPair>& bench) {
  std::vector<uint64_t> latency;
  std::vector<uint64_t> apply_latency;
  std::vector<uint64_t> commit_latency;

  std::printf("[Execution Process]\n");

  for (int i = 0; i < bench.size(); ++i) {
    const auto& p = bench[i];
    auto start = std::chrono::high_resolution_clock::now();
    auto stat = client->Put(p.first, p.second);
    auto end = std::chrono::high_resolution_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    if (stat.err == kv::kOk) {
      latency.push_back(dura.count());  // us
      apply_latency.push_back(stat.apply_elapse_time);
      if (stat.commit_elapse_time != -1) {
        commit_latency.push_back(stat.commit_elapse_time);
      }
    } else {
      break;
    }
    int done_cnt = i + 1;
    if (done_cnt > 0 && done_cnt % kVerboseInterval == 0) {
      std::cout << "\r[Already Execute " << done_cnt << " Ops]" << std::flush;
    }
  }

  puts("");

  auto [avg_latency, max_latency] = Analysis(latency);
  auto [avg_apply_latency, max_apply_latency] = Analysis(apply_latency);
  auto [avg_commit_latency, max_commit_latency] = Analysis(commit_latency);

  printf("[Client Id %d]\n", client->ClientId());
  printf("[Results][Succ Cnt=%lu][Average Latency = %llu us][Max Latency = %llu us]\n",
         latency.size(), avg_latency, max_latency);
  printf("[Average Apply Latency = %llu us][Max Apply Latency = %llu us]\n",
         avg_apply_latency, max_apply_latency);
  printf("[Average Commit Latency = %llu us][Max Commit Latency = %llu us]\n",
         avg_commit_latency, max_commit_latency);
  fflush(stdout);

  int succ_cnt = 0;
  // Check if inserted value can be found
  for (const auto& p : bench) {
    std::string get_val;
    auto stat = client->Get(p.first, &get_val);
    if (stat.err == kv::kOk && get_val == p.second) {
      ++succ_cnt;
    }
    // No need to continue executing the benchmark
    if (stat.err == kv::kRequestExecTimeout) {
      break;
    }
  }
  printf("[Get Results][Succ Count=%d]\n", succ_cnt);
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
