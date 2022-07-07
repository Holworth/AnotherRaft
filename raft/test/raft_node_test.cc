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

TEST_F(RaftNodeBasicTest, TestFollowersCrash) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  // EXPECT_TRUE(ProposeOneEntry(2));
  // EXPECT_TRUE(ProposeOneEntry(3));

  auto leader = GetLeaderId();

  // Disconnect a follower
  Disconnect((leader + 1) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  Disconnect((leader + 2) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(7));
  EXPECT_TRUE(ProposeOneEntry(8));
  EXPECT_TRUE(ProposeOneEntry(9));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, TestLeaderCrash) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader1 = GetLeaderId();

  // Disconnect leader
  Disconnect(leader1);

  // The cluster should continue to run despite one server down
  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, TestNewLeaderGetFullLogEntry) {
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader1 = GetLeaderId();

  auto cmd = ConstructCommandFromValue(4);
  auto pr = nodes_[leader1]->Propose(cmd);
  EXPECT_TRUE(pr.is_leader);

  // Disconnect old leader before this entry is committed
  Disconnect(leader1);

  sleepMs(1000);

  EXPECT_TRUE(checkCommitted(pr, 4));

  ClearTestContext(config);
}

}  // namespace raft
