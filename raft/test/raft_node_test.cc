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

}  // namespace raft
