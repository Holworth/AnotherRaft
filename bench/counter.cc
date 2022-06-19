#include <algorithm>
#include <chrono>
#include <cstdio>
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
      : node_(new RaftNode(config)), cmd_size_(cmd_size) {
    std::printf("Finish creating benchmark\n");
  }
  ~CounterBench() { delete node_; }

  void StartBench() {
    std::printf("Start benchmarking\n");
    node_->Init();
    // Wait for other nodes bootsup
    std::this_thread::sleep_for(std::chrono::seconds(3));
    node_->Start();
    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // A thread that periodically reports result
    auto report_results = [this]() {
      while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        this->BenchResults();
      }
    };

    std::thread report_thread(report_results);
    report_thread.detach();

    int test_cnt = 100000;
    int succ_cnt = 0;
    while (true) {
      // Propose at most 100000 entries
      if (succ_cnt >= test_cnt) {
        continue;
      }
      bool stat = ProposeOneEntry();
      if (stat) {
        succ_cnt += 1;
      }
      if (succ_cnt >= test_cnt) {
        node_->Exit();
        return;
      }
    }
  }

  void BenchResults() {
    if (propose_cnt_ != 0) {
      std::cout << "[Propose count]" << propose_cnt_ << std::endl;
      std::cout << "[Average Lantency]"
                << static_cast<double>(agreement_time_) / propose_cnt_ << std::endl;
    } else {
      std::cout << "[Propose count] " << 0 << std::endl;
    }
  }

 private:
  bool ProposeOneEntry() {
    auto raft = node_->getRaftState();
    auto cmd = ConstructCommand(1);

    util::Timer timer;
    timer.Reset();
    auto stat = raft->Propose(cmd);
    if (!stat.is_leader) {
      delete cmd.command_data.data();
      return false;
    }

    // Wait for this command to commit, for at most 1000s
    while (timer.ElapseMilliseconds() < 1000) {
      if (stat.propose_index <= raft->CommitIndex()) {
        agreement_time_ += timer.ElapseMicroseconds();
        propose_cnt_ += 1;
        return true;
      }
    }
    return false;
  }

  CommandData ConstructCommand(int val) {
    auto cmd_data = new char[cmd_size_];
    *reinterpret_cast<int*>(cmd_data) = 1;
    return CommandData{cmd_size_, Slice(cmd_data, cmd_size_)};
  }

 private:
  RaftNode* node_;
  int cmd_size_;
  uint64_t agreement_time_ = 0; // elapse time
  uint16_t propose_cnt_ = 0;
};

int main(int argc, char* argv[]) {
  CommandLineParser parser;
  if (!parser.Parse(argc, argv)) {
    parser.ErrorMsg();
    exit(1);
  }

  std::printf("Finish parsing parameters\n");

  RaftNode::NodeConfig config;
  config.node_id_me = parser.GetNodeId();
  config.servers = parser.GetNetConfig();
  // config.storage_filename = std::string("raft_log") + std::to_string(config.node_id_me);
  config.storage_filename = "";
  config.rsm = new CounterStateMachine();
  // std::filesystem::remove(std::string("raft_log") + std::to_string(config.node_id_me));
  remove((std::string("raft_log") + std::to_string(config.node_id_me)).c_str());

  CounterBench bench(config, parser.GetCommandSize());
  bench.StartBench();
  bench.BenchResults();
}
