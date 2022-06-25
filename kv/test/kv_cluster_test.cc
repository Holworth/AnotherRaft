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

 private:
  KvServiceNode* nodes_[kMaxNodeNum];
  int node_num_;
};

TEST_F(KvClusterTest, TestSimplePutGetOperation) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = new KvServiceClient(cluster_config);
  int put_cnt = 1000;
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(client->Put(key, value), kOk);
  }

  // Check Get
  std::string value;
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto expect_value = "value" + std::to_string(i);
    EXPECT_EQ(client->Get(key, &value), kOk);
    EXPECT_EQ(value, expect_value);
  }

  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, TestDeleteAndOverWriteValue) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = new KvServiceClient(cluster_config);
  int put_cnt = 1000;
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(client->Put(key, value), kOk);
  }

  // Check Get
  std::string value;
  for (int i = 1; i <= put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto expect_value = "value" + std::to_string(i);
    EXPECT_EQ(client->Get(key, &value), kOk);
    EXPECT_EQ(value, expect_value);
  }

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
}  // namespace kv
