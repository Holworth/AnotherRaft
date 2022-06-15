#include <gtest/gtest.h>

#include <filesystem>
#include <unordered_map>

#include "raft_node_test.h"
#include "raft_type.h"
namespace raft {
class RaftNodeCrashTest : public RaftNodeTest {
 public:
  void Crash(raft_node_id_t id) {
    if (!nodes_[id]->Exited()) {
      nodes_[id]->Exit();
    }
    delete nodes_[id];
    nodes_[id] = nullptr;
  }

  void Reboot(const NodesConfig& config, raft_node_id_t id) {
    auto net_config = GetNetConfigFromNodesConfig(config);
    auto node_config = config.at(id);
    LaunchRaftNodeInstance({id, net_config, node_config.storage_name, new RsmMock});
  }

  void PreRemoveFiles(const NodesConfig& config) {
    for (const auto& [_, config]:config) {
      std::filesystem::remove(config.storage_name);
    }
  }
};

TEST_F(RaftNodeCrashTest, TestSimpleRestartLeader) {
  auto config = ConstructNodesConfig(3, true);
  PreRemoveFiles(config);

  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(101));
  auto leader = GetLeaderId();

  Disconnect((leader + 2) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(102));

  Crash((leader + 0) % node_num_);
  Crash((leader + 1) % node_num_);

  Reconnect((leader + 2) % node_num_);
  // Can not commit one entry since there is no majority
  // EXPECT_FALSE(ProposeOneEntry(103));

  // Able to commit one entry since majority comes back
  Reboot(config, (leader + 0) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(103));

  Reboot(config, (leader + 1) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(104));

  ClearTestContext(config);
}
}  // namespace raft
