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

  // Let all created raft nodes to exit
  void ExitAll() {
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode* node) { node->Exit(); });
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
  std::unordered_map<raft_node_id_t, rpc::NetAddress> net_config = {
      {0, {"127.0.0.1", 50001}},
      {1, {"127.0.0.1", 50002}},
      {2, {"127.0.0.1", 50003}},
  };
  node_num_ = 3;
  for (const auto& [id, _] : net_config) {
    LaunchRaftNodeInstance({id, net_config, ""});
  }
  // ASSERT_TRUE(CheckNoLeader());
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(CheckHasLeader());
  ExitAll();
}

}  // namespace raft
