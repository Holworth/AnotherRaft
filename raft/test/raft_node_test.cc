#include "raft_node_test.h"

#include <string>

namespace raft {
class RaftNodeBasicTest : public RaftNodeTest {
 public:
  static NetConfig ConstructNetConfig(int server_num) {
    std::string default_ip = "127.0.0.1";
    uint16_t init_port = 50001;
    NetConfig ret;
    for (uint16_t i = 0; i < server_num; ++i) {
      ret.insert({i, {default_ip, static_cast<uint16_t>(init_port + i)}});
    }
    return ret;
  }
};

TEST_F(RaftNodeBasicTest, TestRequestVoteHasLeader) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  EXPECT_TRUE(CheckOneLeader());
  ClearTestContext(config);
}

// NOTE: This test may fail due to RPC , the default RPC uses TCP protocol, which
// requires both sender and receiver maintains their state. However, when we shut
// down the first leader, the rest two may fail to send rpc to the shut-down server
// and causes some exception
TEST_F(RaftNodeBasicTest, TestReElectIfPreviousLeaderExit) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);

  EXPECT_TRUE(CheckOneLeader());

  auto leader_id1 = GetLeaderId();
  EXPECT_NE(leader_id1, kNoLeader);

  Disconnect(leader_id1);

  EXPECT_TRUE(CheckOneLeader());

  auto leader_id2 = GetLeaderId();
  EXPECT_NE(leader_id2, kNoLeader);
  EXPECT_NE(leader_id2, leader_id1);

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, TestWithDynamicClusterChanges) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);

  EXPECT_TRUE(CheckOneLeader());

  const int iter_cnt = 10;
  for (int i = 0; i < iter_cnt; ++i) {
    raft_node_id_t i1 = rand() % node_num_;
    raft_node_id_t i2 = (i1 + 1) % node_num_;

    Disconnect(i1);
    Disconnect(i2);

    EXPECT_TRUE(CheckOneLeader());

    Reconnect(i1);
    Reconnect(i2);
  }

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, TestSimplyProposeEntry) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Test propose a few entries
  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestProposeEntryWhenServerShutdown) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader_id1 = GetLeaderId();

  Disconnect(leader_id1);

  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestFailReachAgreementIfMajorityShutDown) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
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

    Reconnect(id1);
    Reconnect(id2);
    Reconnect(id3);
  }

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestOldLeaderRejoin) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(101));

  auto leader1 = GetLeaderId();
  Disconnect(leader1);
  LOG(util::kRaft, "----- S%d disconnect -----", leader1);

  // This value should not be committed
  nodes_[leader1]->Propose(ConstructCommandFromValue(102));
  nodes_[leader1]->Propose(ConstructCommandFromValue(103));
  nodes_[leader1]->Propose(ConstructCommandFromValue(104));

  // New leader
  EXPECT_TRUE(ProposeOneEntry(103));

  // Disconnect new leader
  auto leader2 = GetLeaderId();
  EXPECT_NE(leader2, leader1);
  Disconnect(leader2);
  LOG(util::kRaft, "----- S%d disconnect -----", leader2);

  // Old leader rejoin
  Reconnect(leader1);
  LOG(util::kRaft, "----- S%d reconnect -----", leader1);
  EXPECT_TRUE(ProposeOneEntry(104));

  // New leader rejoin, all servers together
  Reconnect(leader2);
  LOG(util::kRaft, "----- S%d reconnect -----", leader2);
  EXPECT_TRUE(ProposeOneEntry(105));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestRecoverAfterLongIncorrectLogs) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  int init_value = 100;
  EXPECT_TRUE(ProposeOneEntry(init_value));
  // Disconnect another group of servers from the cluster
  auto leader1 = GetLeaderId();
  Disconnect((leader1 + 2) % node_num_);
  Disconnect((leader1 + 3) % node_num_);
  Disconnect((leader1 + 4) % node_num_);
  // LOG(util::kRaft, "----- S%d disconnect -----", leader1);

  // Add a few commands that won't be committed
  for (int i = 1; i <= 50; ++i) {
    nodes_[leader1]->Propose(ConstructCommandFromValue(init_value + i));
  }
  sleepMs(100);

  // Disable old leader and another server, bring the majority into cluster
  Disconnect((leader1 + 0) % node_num_);
  Disconnect((leader1 + 1) % node_num_);

  Reconnect((leader1 + 2) % node_num_);
  Reconnect((leader1 + 3) % node_num_);
  Reconnect((leader1 + 4) % node_num_);

  // These commands should be properly committed
  for (int i = 1; i <= 50; ++i) {
    EXPECT_TRUE(ProposeOneEntry(init_value * 10 + i));
  }

  // Now get new leader and another server partitioned
  auto leader2 = GetLeaderId();
  auto other = (leader1 + 2) % node_num_;
  if (leader2 == other) {
    other = (leader1 + 3) % node_num_;
  }
  Disconnect(other);

  // These commands should not be committed
  for (int i = 1; i <= 50; ++i) {
    nodes_[leader2]->Propose(ConstructCommandFromValue(init_value * 100 + i));
  }
  sleepMs(100);

  // Bring original leader back
  for (int i = 0; i < node_num_; ++i) {
    Disconnect(i);
  }

  Reconnect((leader1 + 0) % node_num_);
  Reconnect((leader1 + 1) % node_num_);
  Reconnect(other);

  // These commands will be properly committed
  for (int i = 1; i <= 50; ++i) {
    EXPECT_TRUE(ProposeOneEntry(init_value * 1000 + i));
  }

  // Bring all servers here
  for (int node = 0; node < node_num_; ++node) {
    Reconnect(node);
  }
  EXPECT_TRUE(ProposeOneEntry(init_value));

  ClearTestContext(config);
}

}  // namespace raft
