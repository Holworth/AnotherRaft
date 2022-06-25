#include <filesystem>
#include <string>
#include <thread>

#include "client.h"
#include "config.h"
#include "gtest/gtest.h"
#include "kv_node.h"
#include "raft_type.h"
#include "type.h"
namespace kv {
// This test should be running on a multi-core machine, so that each thread can run
// smoothly without interpretation
class KvClusterTest : public ::testing::Test {
  static constexpr int kMaxNodeNum = 10;

 public:
  void LaunchKvServiceNodes(const KvClusterConfig& config) {
    node_num_ = config.size();
    for (const auto& [id, conf] : config) {
      auto node = KvServiceNode::NewKvServiceNode(config, id);
      node->InitServiceNodeState();
      nodes_[id] = node;
    }

    for (int i = 0; i < node_num_; ++i) {
      nodes_[i]->StartServiceNode();
    }
  }

  void ClearTestContext(const KvClusterConfig& config) {
    for (int i = 0; i < node_num_; ++i) {
      nodes_[i]->StopServiceNode();
      delete nodes_[i];
    }
    for (const auto& [id, conf] : config) {
      if (conf.raft_log_filename != "") {
        std::filesystem::remove(conf.raft_log_filename);
      }
      if (conf.kv_dbname != "") {
        std::filesystem::remove_all(conf.kv_dbname);
      }
    }
  }

  void sleepMs(int cnt) { std::this_thread::sleep_for(std::chrono::milliseconds(cnt)); }

  raft::raft_node_id_t GetLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (!nodes_[i]->IsDisconnected() && nodes_[i]->IsLeader()) {
        return i;
      }
    }
    return kNoDetectLeader;
  }

  void Disconnect(int i) {
    if (!nodes_[i]->IsDisconnected()) {
      nodes_[i]->Disconnect();
    }
  }

  void Reconnect(int i) {
    if (nodes_[i]->IsDisconnected()) {
      nodes_[i]->Reconnect();
    }
  }

  void CheckBatchPut(KvServiceClient* client, const std::string& key_prefix,
                     const std::string& value_prefix, int key_lo, int key_hi) {
    for (int i = key_lo; i <= key_hi; ++i) {
      auto key = key_prefix + std::to_string(i);
      auto value = value_prefix + std::to_string(i);
      EXPECT_EQ(client->Put(key, value), kOk);
    }
  }

  void CheckBatchGet(KvServiceClient* client, const std::string& key_prefix,
                     const std::string expect_val_prefix, int key_lo, int key_hi) {
    std::string get_val;
    for (int i = key_lo; i <= key_hi; ++i) {
      auto key = key_prefix + std::to_string(i);
      auto expect_value = expect_val_prefix + std::to_string(i);
      EXPECT_EQ(client->Get(key, &get_val), kOk);
      EXPECT_EQ(get_val, expect_value);
    }
  }

 private:
  KvServiceNode* nodes_[kMaxNodeNum];
  static const raft::raft_node_id_t kNoDetectLeader = -1;
  int node_num_;
};

TEST_F(KvClusterTest, DISABLED_TestSimplePutGetOperation) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = new KvServiceClient(cluster_config);
  int put_cnt = 1000;
  CheckBatchPut(client, "key", "value", 1, put_cnt);
  CheckBatchGet(client, "key", "value", 1, put_cnt);
  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, DISABLED_TestDeleteAndOverWriteValue) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = new KvServiceClient(cluster_config);
  int put_cnt = 1000;

  CheckBatchPut(client, "key", "value", 1, put_cnt);
  CheckBatchGet(client, "key", "value", 1, put_cnt);

  // For odd key, delete it, for even key, overwrite them with new value
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    if (i % 2) {
      EXPECT_EQ(client->Delete(key), kOk);
    } else {
      auto value = "value2-" + std::to_string(i);
      EXPECT_EQ(client->Put(key, value), kOk);
    }
  }

  // Check Get
  std::string value;
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    if (i % 2) {
      EXPECT_EQ(client->Get(key, &value), kKeyNotExist);
    } else {
      auto expect_val = "value2-" + std::to_string(i);
      EXPECT_EQ(client->Get(key, &value), kOk);
      EXPECT_EQ(value, expect_val);
    }
  }
  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, TestLeaderTransfer) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };

  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = new KvServiceClient(cluster_config);
  const int put_cnt = 10;
  CheckBatchPut(client, "key1-", "value1-", 1, put_cnt);
  CheckBatchGet(client, "key1-", "value1-", 1, put_cnt);

  // The client returns the leader id exactly the same as the true leader id
  EXPECT_EQ(GetLeaderId(), client->LeaderId());

  auto leader1 = GetLeaderId();
  Disconnect(leader1);

  // Check get after leader transfer
  CheckBatchGet(client, "key1-", "value1-", 1, put_cnt);

  EXPECT_EQ(GetLeaderId(), client->LeaderId());
  auto leader2 = GetLeaderId();
  ASSERT_NE(leader2, leader1);

  // Write some values as new leader
  CheckBatchPut(client, "key2-", "value2-", 1, put_cnt);
  CheckBatchGet(client, "key2-", "value2-", 1, put_cnt);

  // Disable the new leader and bring the old leader back to cluster
  Disconnect(leader2);
  Reconnect(leader1);

  CheckBatchGet(client, "key1-", "value1-", 1, put_cnt);
  CheckBatchGet(client, "key2-", "value2-", 1, put_cnt);

  EXPECT_EQ(GetLeaderId(), client->LeaderId());

  // Leader3 should not be leader1 since it has older log
  // Leader3 should not be leader2 since leader2 has been disabled
  auto leader3 = GetLeaderId();
  ASSERT_NE(leader3, leader1);
  ASSERT_NE(leader3, leader2);

  CheckBatchPut(client, "key3-", "value3-", 1, put_cnt);
  CheckBatchGet(client, "key3-", "value3-", 1, put_cnt);

  ClearTestContext(cluster_config);
}
}  // namespace kv
