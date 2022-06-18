#include <_types/_uint64_t.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <unordered_map>

#include "bench_util.h"
#include "gtest/gtest.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rsm.h"
#include "util.h"

using namespace raft;
class CounterStateMachine : public Rsm {
  void ApplyLogEntry(LogEntry ent) override {
    int val = *reinterpret_cast<int*>(ent.CommandData().data());
    counter += val;
  }

 private:
  int counter = 0;
};

class CounterBench {
 public:
  CounterBench(const RaftNode::NodeConfig& config, int cmd_size)
      : node_(new RaftNode(config)), cmd_size_(cmd_size) {}
  ~CounterBench() { delete node_; }

  void StartBench() {
    node_->Init();
    node_->Start();
    int test_cnt = 100000;
    for (int i = 0; i < test_cnt; ++i) {
      bool stat = ProposeOneEntry();
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    node_->Exit();
  }

  void BenchResults() {
    if (propose_cnt_ != 0) {
      std::cout << "[Propose count]" << propose_cnt_ << std::endl;
      std::cout << "[Average Lantency]"
                << static_cast<double>(agreement_time_) / propose_cnt_ << std::endl;
    } else {
      std::cout << "[Propose count]" << 0 << std::endl;
    }
  }

 private:
  bool ProposeOneEntry() {
    auto raft = node_->getRaftState();
    auto cmd = ConstructCommand(1);

    auto start_time = std::chrono::high_resolution_clock::now();
    auto stat = raft->Propose(cmd);
    if (!stat.is_leader) {
      delete cmd.command_data.data();
      return false;
    }

    // Wait for this command to commit, for at most 1000s
    auto raft_index = stat.propose_index;
    while (true) {
      auto end_time = std::chrono::high_resolution_clock::now();
      auto dura =
          std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      if (raft_index <= raft->CommitIndex()) {
        agreement_time_ += dura.count();
        propose_cnt_ += 1;
        return true;
      }
      if (dura.count() >= 1000) {  // timeout
        return false;
      }
    }
  }

  CommandData ConstructCommand(int val) {
    auto cmd_data = new char[cmd_size_];
    *reinterpret_cast<int*>(cmd_data) = 1;
    return CommandData{cmd_size_, Slice(cmd_data, cmd_size_)};
  }

 private:
  RaftNode* node_;
  int cmd_size_;
  uint64_t agreement_time_ = 0;
  uint16_t propose_cnt_ = 0;
};

int main(int argc, char* argv[]) {
  CommandLineParser parser;
  if (!parser.Parse(argc, argv)) {
    parser.ErrorMsg();
    exit(1);
  }

  RaftNode::NodeConfig config;
  config.node_id_me = parser.GetNodeId();
  config.servers = parser.GetNetConfig();
  config.storage_filename = std::string("raft_log") + std::to_string(config.node_id_me);
  config.rsm = new CounterStateMachine();

  CounterBench bench(config, parser.GetCommandSize());
  bench.StartBench();
  bench.BenchResults();
}
