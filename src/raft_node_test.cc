#include "raft_node.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "rsm.h"
#include "storage.h"

namespace raft {
class RaftNodeTest : public ::testing::Test {
 public:
  static constexpr int kMaxNodeNum = 9;
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

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
        return -1;
      }
      return commit_res.second;
    }

   private:
    std::unordered_map<raft_index_t, CommitResult> applied_value_;
  };

  static constexpr raft_node_id_t kNoLeader = -1;
  // Create a thread that holds this raft node, and start running this node immediately
  // returns the pointer to that raft node
  void LaunchRaftNodeInstance(const RaftNode::NodeConfig& config);

  bool CheckNoLeader() {
    bool has_leader = false;
    std::for_each(nodes_, nodes_ + node_num_, [&](RaftNode* node) {
      has_leader |= (node->getRaftState()->Role() == kLeader);
    });
    return has_leader == false;
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

  bool ProposeOneEntry(int value) {
    auto data = new char[sizeof(int)];
    *reinterpret_cast<int*>(data) = value;
    CommandData cmd{sizeof(int), Slice(data, sizeof(int))};

    const int retry_cnt = 20;
    for (int run = 0; run < retry_cnt; ++run) {
      raft_node_id_t leader_id = kNoLeader;
      ProposeResult propose_result;
      for (int i = 0; i < node_num_; ++i) { // Search for a leader
        if (!Alive(i)) {
          continue;
        }
        propose_result = nodes_[i]->getRaftState()->Propose(cmd);
        if (propose_result.is_leader) {
          leader_id = i;
          break;
        }
      }

      if (leader_id != kNoLeader) {
        // Wait this entry to be committed
        assert(propose_result.propose_index > 0);
        int val;
        while ((val = checkCommitted(propose_result)) == -1) {
          sleepMs(20);
        }
        return val == value;
      }

      // Sleep for 50ms so that the entry will be committed
      sleepMs(500);
    }
    return false;
  }

  void LaunchAllServers(const NetConfig& net_config) {
    node_num_ = net_config.size();
    for (const auto& [id, _] : net_config) {
      LaunchRaftNodeInstance({id, net_config, "", new RsmMock});
    }
  }

  void sleepMs(int num) { std::this_thread::sleep_for(std::chrono::milliseconds(num)); }

  int checkCommitted(const ProposeResult& propose_result) {
    for (int i = 0; i < node_num_; ++i) {
      if (nodes_[i]->getRaftState()->CommitIndex() >= propose_result.propose_index) {
        return reinterpret_cast<RsmMock*>(nodes_[i]->getRsm())
            ->getValue(propose_result.propose_index, propose_result.propose_term);
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

  void Reconnect(const NetConfig& net_config, raft_node_id_t id) { 
    // std::cout << "Reconnect " << id << std::endl;
    nodes_[id]->Reconnect();
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

 public:
  // Record each nodes and all nodes number
  RaftNode* nodes_[kMaxNodeNum];
  int node_num_;
};

void RaftNodeTest::LaunchRaftNodeInstance(const RaftNode::NodeConfig& config) {
  auto node_thread = std::thread([=]() {
    auto raft_node = new RaftNode(config);
    this->nodes_[config.node_id_me] = raft_node;
    raft_node->Init();
    raft_node->Start();
  });

  node_thread.detach();
}

TEST_F(RaftNodeTest, DISABLED_TestRequestVoteHasLeader) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);

  ASSERT_TRUE(CheckOneLeader());

  TestEnd();
}

// NOTE: This test may fail due to RPC , the default RPC uses TCP protocol, which
// requires both sender and receiver maintains their state. However, when we shut
// down the first leader, the rest two may fail to send rpc to the shut-down server
// and causes some exception
TEST_F(RaftNodeTest, DISABLED_TestReElectIfPreviousLeaderExit) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);

  ASSERT_TRUE(CheckOneLeader());

  auto leader_id1 = GetLeaderId();
  ASSERT_NE(leader_id1, kNoLeader);

  Disconnect(leader_id1);

  ASSERT_TRUE(CheckOneLeader());

  auto leader_id2 = GetLeaderId();
  ASSERT_NE(leader_id2, kNoLeader);
  ASSERT_NE(leader_id2, leader_id1);

  TestEnd();
}

TEST_F(RaftNodeTest, TestWithDynamicClusterChanges) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}}, {1, {"127.0.0.1", 50002}}, {2, {"127.0.0.1", 50003}},
      {3, {"127.0.0.1", 50004}}, {4, {"127.0.0.1", 50005}},
  };
  LaunchAllServers(net_config);
  EXPECT_TRUE(CheckOneLeader());

  const int iter_cnt = 10;
  for (int i = 0; i < iter_cnt; ++i) {
    raft_node_id_t i1 = rand() % node_num_;
    raft_node_id_t i2 = (i1 + 1) % node_num_;

    Disconnect(i1);
    Disconnect(i2);

    EXPECT_TRUE(CheckOneLeader());

    Reconnect(net_config, i1);
    Reconnect(net_config, i2);
  }

  TestEnd();
}

TEST_F(RaftNodeTest, TestSimplyProposeEntry) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);
  sleepMs(10);

  // Test propose a few entries
  ASSERT_TRUE(ProposeOneEntry(1));
  ASSERT_TRUE(ProposeOneEntry(2));
  ASSERT_TRUE(ProposeOneEntry(3));

  TestEnd();
}

TEST_F(RaftNodeTest, TestProposeEntryWhenServerShutdown) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader_id1 = GetLeaderId();

  Disconnect(leader_id1);

  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  TestEnd();
}

TEST_F(RaftNodeTest, TestFailReachAgreementIfMajorityShutDown) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}}, {1, {"127.0.0.1", 50002}}, {2, {"127.0.0.1", 50003}},
      {3, {"127.0.0.1", 50004}}, {4, {"127.0.0.1", 50005}},
  };
  LaunchAllServers(net_config);
  sleepMs(10);

  const int iter_cnt = 1;
  for (int i = 0; i < iter_cnt; ++i) {
    raft_node_id_t id1 = rand() % node_num_;
    raft_node_id_t id2 = (id1 + 1) % node_num_;
    raft_node_id_t id3 = (id1 + 2) % node_num_;

    Disconnect(id1);
    Disconnect(id2);
    Disconnect(id3);

    // Can not propose and commit an entry since there is only 2 alive servers
    EXPECT_FALSE(ProposeOneEntry(i + 1));

    Reconnect(net_config, id1);
    Reconnect(net_config, id2);
    Reconnect(net_config, id3);
  }
  TestEnd();
}

}  // namespace raft
