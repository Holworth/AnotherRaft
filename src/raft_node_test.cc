#include "raft_node.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "storage.h"

namespace raft {
class RaftNodeTest : public ::testing::Test {
 public:
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

  static constexpr raft_node_id_t kNoLeader = -1;
  // Create a thread that holds this raft node, and start running this node immediately
  // returns the pointer to that raft node
  void LaunchRaftNodeInstance(const RaftNode::NodeConfig& config);

  // Check that there exists one leader among all nodes
  bool CheckHasLeader() {
    bool has_leader = false;
    std::for_each(nodes_, nodes_ + node_num_, [&](RaftNode* node) {
      has_leader |= (node->getRaftState()->Role() == kLeader);
    });
    return has_leader;
  }

  bool CheckNoLeader() {
    bool has_leader = false;
    std::for_each(nodes_, nodes_ + node_num_, [&](RaftNode* node) {
      has_leader |= (node->getRaftState()->Role() == kLeader);
    });
    return has_leader == false;
  }

  void LaunchAllServers(const NetConfig& net_config) {
    node_num_ = net_config.size();
    for (const auto& [id, _] : net_config) {
      LaunchRaftNodeInstance({id, net_config, ""});
    }
  }

  // Find current leader and returns its associated node id, if there is multiple
  // leader, for example, due to network partition, returns the smallest one
  raft_node_id_t GetLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (!nodes_[i]->Exited() && nodes_[i]->getRaftState()->Role() == kLeader) {
        return i;
      }
    }
    return kNoLeader;
  }

  void ShutDown(raft_node_id_t id) {
    if (!nodes_[id]->Exited()) {
      nodes_[id]->Exit();
    }
  }

  // Let all created raft nodes to exit
  void ExitAll() {
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode* node) {
      if (!node->Exited()) {
        node->Exit();
      }
    });
  }

  void ClearAll() {
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode* node) { delete node; });
  }

 public:
  // Record each nodes and all nodes number
  RaftNode* nodes_[7];
  int node_num_;
};

void RaftNodeTest::LaunchRaftNodeInstance(const RaftNode::NodeConfig& config) {
  auto node_thread = std::thread([=]() {
    auto raft_node = new RaftNode(config);
    this->nodes_[config.node_id_me] = raft_node;
    raft_node->Init();
    raft_node->Start();
    while (!raft_node->Exited())
      ;
  });

  node_thread.detach();
}

TEST_F(RaftNodeTest, TestRequestVoteHasLeader) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(CheckHasLeader());
  ExitAll();
  ClearAll();
}

// NOTE: This test may fail due to RPC , the default RPC uses TCP protocol, which
// requires both sender and receiver maintains their state. However, when we shut
// down the first leader, the rest two may fail to send rpc to the shut-down server
// and causes some exception
TEST_F(RaftNodeTest, TestReElectIfPreviousLeaderExit) {
  NetConfig net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  LaunchAllServers(net_config);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(CheckHasLeader());

  auto leader_id1 = GetLeaderId();
  ASSERT_NE(leader_id1, kNoLeader);

  ShutDown(leader_id1);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(CheckHasLeader());

  auto leader_id2 = GetLeaderId();
  ASSERT_NE(leader_id2, kNoLeader);
  ASSERT_NE(leader_id2, leader_id1);

  ExitAll();
  ClearAll();
}

}  // namespace raft
