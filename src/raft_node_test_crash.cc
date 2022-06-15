#include <gtest/gtest.h>
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
};
}  // namespace raft
