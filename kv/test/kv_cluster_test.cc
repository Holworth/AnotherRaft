#include <thread>

#include "client.h"
#include "config.h"
#include "gtest/gtest.h"
#include "kv_node.h"
#include "raft_type.h"
#include "type.h"
namespace kv {
class KvClusterTest : public ::testing::Test {
  static constexpr int kMaxNodeNum = 10;

 public:
  void LaunchKvServiceNodes(const KvClusterConfig& config) {
    node_num_ = config.size();
    for (const auto& [id, conf] : config) {
      nodes_[id] = KvServiceNode::NewKvServiceNode(config, id);
      auto run_thread = [=](raft::raft_node_id_t id) { 
        nodes_[id]->InitServiceNodeState();
        nodes_[id]->StartServiceNode(); 
      };
      std::thread t(run_thread, id);
      t.detach();
    }
  }

  void ClearTestContext(const KvClusterConfig& config) {
    for (int i = 0; i < node_num_; ++i) {
      nodes_[i]->StopServiceNode();
    }
  }

 private:
  KvServiceNode* nodes_[kMaxNodeNum];
  int node_num_;
};

TEST_F(KvClusterTest, TestSimplePutGetOperation) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 60000}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 60001}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 60002}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);

  auto client = new KvServiceClient(cluster_config);

  EXPECT_EQ(client->Put("key1", "value1"), kOk);
  std::string value;
  EXPECT_EQ(client->Get("key1", &value), kOk);
  EXPECT_EQ(value, "value1");

  ClearTestContext(cluster_config);
}
}  // namespace kv
