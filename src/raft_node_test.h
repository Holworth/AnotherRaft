#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "rsm.h"
#include "storage.h"
#include "util.h"

#define CONFLICT_TERM -2

namespace raft {
// A bunch of basic test framework
class RaftNodeTest : public ::testing::Test {
 public:
  static constexpr int kMaxNodeNum = 9;
  static constexpr raft_node_id_t kNoLeader = -1;

  // Some necessary Test structs
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;
  struct TestNodeConfig {
    rpc::NetAddress ip;
    std::string storage_name;
  };
  using NodesConfig = std::unordered_map<raft_node_id_t, TestNodeConfig>;

  // This is a simple simulated state machine, it assumes that each command is simply
  // an integer and the applier simply records it with associated log index
  class RsmMock : public Rsm {
    // (index, term) uniquely identify an entry
    using CommitResult = std::pair<raft_term_t, int>;

   public:
    void ApplyLogEntry(LogEntry ent) override {
      int val = *reinterpret_cast<int*>(ent.CommandData().data());
      applied_value_.insert({ent.Index(), {ent.Term(), val}});
    }

    int getValue(raft_index_t raft_index, raft_term_t raft_term) {
      if (applied_value_.count(raft_index) == 0) {
        return -1;
      }
      auto commit_res = applied_value_[raft_index];
      if (commit_res.first != raft_term) {
        return CONFLICT_TERM;
      }
      return commit_res.second;
    }

   private:
    std::unordered_map<raft_index_t, CommitResult> applied_value_;
  };


  static NodesConfig ConstructNodesConfig(int server_num, bool with_storage) {
    std::string default_ip = "127.0.0.1";
    uint16_t init_port = 50001;
    NodesConfig ret;
    for (uint16_t i = 0; i < server_num; ++i) {
      TestNodeConfig node_config;
      node_config.ip = {default_ip, static_cast<uint16_t>(init_port + i)};
      node_config.storage_name =
          with_storage ? std::string("test_storage") + std::to_string(i) : "";
      ret.insert({i, node_config});
    }
    return ret;
  }

  void LaunchAllServers(const NetConfig& net_config) {
    node_num_ = net_config.size();
    for (const auto& [id, _] : net_config) {
      LaunchRaftNodeInstance({id, net_config, "", new RsmMock});
    }
  }

  NetConfig GetNetConfigFromNodesConfig(const NodesConfig& nodes_config) {
    NetConfig net_config;
    for (const auto& [id, config] : nodes_config) {
      net_config.insert({id, config.ip});
    }
    return net_config;
  }

  void LaunchAllServers(const NodesConfig& nodes_config) {
    node_num_ = nodes_config.size();
    NetConfig net_config = GetNetConfigFromNodesConfig(nodes_config);
    for (const auto& [id, config] : nodes_config) {
      LaunchRaftNodeInstance({id, net_config, config.storage_name, new RsmMock});
    }
  }

  // Create a thread that holds this raft node, and start running this node immediately
  // returns the pointer to that raft node
  void LaunchRaftNodeInstance(const RaftNode::NodeConfig& config) {
    auto raft_node = new RaftNode(config);
    this->nodes_[config.node_id_me] = raft_node;
    raft_node->Init();
    auto node_thread = std::thread([=]() {
      raft_node->Start();
    });
    node_thread.detach();
  };

  bool CheckNoLeader() {
    bool has_leader = false;
    std::for_each(nodes_, nodes_ + node_num_, [&](RaftNode* node) {
      has_leader |= (node->getRaftState()->Role() == kLeader);
    });
    return has_leader == false;
  }

  CommandData ConstructCommandFromValue(int val) {
    auto data = new char[64];
    *reinterpret_cast<int*>(data) = val;
    return CommandData{sizeof(int), Slice(data, sizeof(int))};
  }

  // Check that at every fixed term, there is and there is only one leader alive
  // NOTE: CheckOneLeader may fail under such condition:
  //   Say there are 3 servers, the old leader and one is alive, but another one is
  //   separate from network and becoming candidate with much higher term.
  bool CheckOneLeader() {
    const int retry_cnt = 5;
    std::unordered_map<raft_term_t, int> leader_cnt;
    auto record = [&](RaftNode* node) {
      if (!node->IsDisconnected()) {
        auto raft_state = node->getRaftState();
        leader_cnt[raft_state->CurrentTerm()] += (raft_state->Role() == kLeader);
      }
    };
    for (int i = 0; i < retry_cnt; ++i) {
      leader_cnt.clear();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      std::for_each(nodes_, nodes_ + node_num_, record);

      raft_term_t lastTerm = 0;
      for (const auto& [term, cnt] : leader_cnt) {
        if (cnt > 2) {
          std::printf("Term %d has more than one leader\n", term);
          return false;
        }
        lastTerm = std::max(lastTerm, term);
      }
      if (lastTerm > 0 && leader_cnt[lastTerm] == 1) {
        return true;
      }
    }
    return false;
  }

  // Where is a majority servers alive in cluster, it's feasible to propose one entry
  // and reach an agreement among these servers
  bool ProposeOneEntry(int value) {
    const int retry_cnt = 20;

    for (int run = 0; run < retry_cnt; ++run) {
      raft_node_id_t leader_id = kNoLeader;
      ProposeResult propose_result;
      auto cmd = ConstructCommandFromValue(value);
      for (int i = 0; i < node_num_; ++i) {  // Search for a leader
        if (!Alive(i)) {
          continue;
        }
        propose_result = nodes_[i]->getRaftState()->Propose(cmd);
        if (propose_result.is_leader) {
          leader_id = i;
          break;
        }
      }

      // This value has been proposed by a leader, however, we can not gurantee it
      // will be committed
      if (leader_id != kNoLeader) {
        // Wait this entry to be committed
        assert(propose_result.propose_index > 0);

        // Retry 10 times
        for (int run2 = 0; run2 < 10; ++run2) {
          int val = checkCommitted(propose_result);
          if (val == CONFLICT_TERM) {
            break;
          } else if (val == -1) {
            sleepMs(20);
          } else {
            LOG(util::kRaft, "Check Propose value: Expect %d Get %d", value, val);
            return val == value;
          }
        }
      }

      // Sleep for 50ms so that the entry will be committed
      sleepMs(500);
    }
    return false;
  }

  void sleepMs(int num) { std::this_thread::sleep_for(std::chrono::milliseconds(num)); }

  int checkCommitted(const ProposeResult& propose_result) {
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i)) {
        auto rsm = reinterpret_cast<RsmMock*>(nodes_[i]->getRsm());
        auto raft = nodes_[i]->getRaftState();
        if (raft->CommitIndex() >= propose_result.propose_index) {
          return rsm->getValue(propose_result.propose_index, propose_result.propose_term);
        }
      }
    }
    return -1;
  }

  bool Alive(int i) {
    return nodes_[i] != nullptr && !nodes_[i]->Exited() && !nodes_[i]->IsDisconnected();
  }

  // Find current leader and returns its associated node id, if there is multiple
  // leader, for example, due to network partition, returns the smallest one
  raft_node_id_t GetLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i) && nodes_[i]->getRaftState()->Role() == kLeader) {
        return i;
      }
    }
    return kNoLeader;
  }

  // TODO: Use pause to replace shut down. A paused node should not respond to any RPC
  // call and not makes progress, as if the server is paused at some point
  // void ShutDown(raft_node_id_t id) {
  //   if (!nodes_[id]->Exited()) {
  //     nodes_[id]->Exit();
  //   }
  // }

  void Disconnect(raft_node_id_t id) {
    // std::cout << "Disconnect " << id << std::endl;
    if (!nodes_[id]->IsDisconnected()) {
      nodes_[id]->Disconnect();
    }
  }

  void Reconnect(raft_node_id_t id) {
    // std::cout << "Reconnect " << id << std::endl;
    if (nodes_[id]->IsDisconnected()) {
      nodes_[id]->Reconnect();
    }
  }

  // Calling end will exits all existed raft node thread, and clear all allocated
  // resources. This should be only called when a test is done
  void TestEnd() {
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode* node) {
      if (!node->Exited()) {
        node->Exit();
      }
    });
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode* node) { delete node; });
  }

  void ClearTestContext(const NodesConfig& nodes_config) {
    auto cmp = [](RaftNode* node) {
      if (!node->Exited()) {
        node->Exit();
        delete node;
      }
    };
    std::for_each(nodes_, nodes_ + node_num_, cmp);

    // Clear created log files
    for (const auto &[_, config] : nodes_config) {
      if (config.storage_name != "") {
        // std::filesystem::remove(config.storage_name);
        remove(config.storage_name.c_str());
      }
    }
  }

 public:
  // Record each nodes and all nodes number
  RaftNode* nodes_[kMaxNodeNum];
  int node_num_;
};
}  // namespace raft
