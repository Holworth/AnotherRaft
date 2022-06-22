#include "kv_server.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "raft_type.h"
#include "type.h"
#include "util.h"
namespace kv {
class KvServerTest : public ::testing::Test {
  static const int kRequestTimeout = 10;  // A KV request must be done within 10s
 public:
  struct TestNodeConfig {
    raft::rpc::NetAddress addr;
    std::string storage_name;
    std::string dbname;
  };
  using NodesConfig = std::unordered_map<raft::raft_node_id_t, TestNodeConfig>;
  using NetConfig = std::unordered_map<raft::raft_node_id_t, raft::rpc::NetAddress>;

 public:
  static constexpr int kMaxNodeNum = 9;

  void sleepMs(int cnt) { std::this_thread::sleep_for(std::chrono::milliseconds(cnt)); }

 public:
  ErrorType Put(const std::string& key, const std::string& value) {
    Request request = Request{kPut, 0, 0, key, value};
    Response resp;
    return WaitUntilRequestDone(&request, &resp);
  }

  ErrorType Get(const std::string& key, std::string* value) {
    Request request = Request{kGet, 0, 0, key, std::string("")};
    Response resp;
    auto res = WaitUntilRequestDone(&request, &resp);
    *value = resp.value;
    return res;
  }

  ErrorType Delete(const std::string& key) {
    Request request = Request{kDelete, 0, 0, key, std::string("")};
    Response resp;
    return WaitUntilRequestDone(&request, &resp);
  }

  raft::raft_node_id_t DetectLeader() {
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i)) {
        auto req = Request{kDetectLeader, 0, 0, "", ""};
        Response resp;
        servers_[i]->DealWithRequest(&req, &resp);
        if (resp.err == kOk) {
          leader_id_ = i;
          // We might need to also record the term of the "leader" so that we can 
          // know who is the true leader?
          return i;
        }
      }
    }
    return kNoLeader;
  }

  bool Alive(raft::raft_node_id_t id) {
    return !servers_[id]->IsDisconnected() && !servers_[id]->Exited();
  }

  raft::raft_node_id_t GetCurrentLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i) && servers_[i]->IsLeader()) {
        return i;
      }
    }
    return kNoLeader;
  }

 private:
  ErrorType WaitUntilRequestDone(const Request* request, Response* resp) {
    raft::util::Timer timer;
    timer.Reset();
    while (timer.ElapseMilliseconds() < kKVRequestTimesout * 1000) {
      if (leader_id_ == kNoLeader && DetectLeader() == kNoLeader) {
        LOG(raft::util::kRaft, "Detect No Leader");
        sleepMs(300);
        continue;
      }
      servers_[leader_id_]->DealWithRequest(request, resp);
      switch (resp->err) {
        case kOk:
        case kKeyNotExist:
          return resp->err;

        case kEntryDeleted:
          break;

        // The leader might be separated from the cluster
        case kRequestExecTimeout:
        case kNotALeader:
          leader_id_ = kNoLeader;
          break;
        
        default:
          assert(false);
      }
    }
    return kKVRequestTimesout;
  }

 public:
  NetConfig GetNetConfigFromNodesConfig(const NodesConfig& nodes_config) {
    NetConfig net_config;
    std::for_each(nodes_config.begin(), nodes_config.end(),
                  [&net_config](const auto& ent) {
                    net_config.insert({ent.first, ent.second.addr});
                  });
    return net_config;
  }

  void LaunchAllServers(const NodesConfig& nodes_config) {
    node_num_ = nodes_config.size();
    auto net_config = GetNetConfigFromNodesConfig(nodes_config);
    for (const auto& [id, config] : nodes_config) {
      raft::RaftNode::NodeConfig raft_config = {id, net_config, config.storage_name,
                                                nullptr};
      LaunchRaftNodeInstance({raft_config, config.dbname});
    }
  }

  void LaunchRaftNodeInstance(const KvServerConfig& config) {
    auto kv_server = KvServer::NewKvServer(config);
    this->servers_[kv_server->Id()] = kv_server;
    kv_server->Init();
    auto kv_thread = std::thread([=]() { kv_server->Start(); });
    kv_thread.detach();
  };

  void ClearTestContext(const NodesConfig& nodes_config) {
    auto cmp = [](KvServer* node) {
      if (!node->Exited()) {
        node->Exit();
        delete node;
      }
    };
    std::for_each(servers_, servers_ + node_num_, cmp);

    // Clear created log files
    for (const auto& [_, config] : nodes_config) {
      if (config.storage_name != "") {
        remove(config.storage_name.c_str());
      }
      if (config.dbname != "") {
        std::filesystem::remove_all(config.dbname);
      }
    }
  }

 public:
  void Disconnect(raft::raft_node_id_t id) { servers_[id]->Disconnect(); }

 public:
  KvServer* servers_[kMaxNodeNum];
  int node_num_;
  raft::raft_node_id_t leader_id_ = kNoLeader;
  static const raft::raft_node_id_t kNoLeader = -1;
};

TEST_F(KvServerTest, TestSimplePutAndGet) {
  auto servers_config = NodesConfig{
      {0, {{"127.0.0.1", 50001}, "", "./testdb0"}},
      {1, {{"127.0.0.1", 50002}, "", "./testdb1"}},
      {2, {{"127.0.0.1", 50003}, "", "./testdb2"}},
  };

  LaunchAllServers(servers_config);

  std::string value;
  const int test_cnt = 10;
  for (int i = 0; i < test_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(Put(key, value), kOk);
  }

  for (int i = 0; i < test_cnt; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
    EXPECT_EQ(value, "value" + std::to_string(i));
  }

  ClearTestContext(servers_config);
}

TEST_F(KvServerTest, TestDeleteAndOverwrite) {
  auto servers_config = NodesConfig{
      {0, {{"127.0.0.1", 50001}, "", "./testdb0"}},
      {1, {{"127.0.0.1", 50002}, "", "./testdb1"}},
      {2, {{"127.0.0.1", 50003}, "", "./testdb2"}},
  };

  LaunchAllServers(servers_config);

  std::string value;
  const int test_cnt = 10;

  for (int i = 0; i < test_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(Put(key, value), kOk);
  }

  for (int i = 0; i < test_cnt; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
    EXPECT_EQ(value, "value" + std::to_string(i));
  }

  for (int i = 0; i < test_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    if (i % 2 == 0) {
      EXPECT_EQ(Delete(key), kOk);
    } else {
      auto value = "value2-" + std::to_string(i);
      EXPECT_EQ(Put(key, value), kOk);
    }
  }

  for (int i = 0; i < test_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    if (i % 2 == 0) {
      EXPECT_EQ(Get(key, &value), kKeyNotExist);
    } else {
      EXPECT_EQ(Get(key, &value), kOk);
      EXPECT_EQ(value, "value2-" + std::to_string(i));
    }
  }

  ClearTestContext(servers_config);
}

TEST_F(KvServerTest, TestGetPreviousValueAfterLeaderCrashes) {
  auto servers_config = NodesConfig{
      {0, {{"127.0.0.1", 50001}, "", "./testdb0"}},
      {1, {{"127.0.0.1", 50002}, "", "./testdb1"}},
      {2, {{"127.0.0.1", 50003}, "", "./testdb2"}},
  };

  LaunchAllServers(servers_config);

  std::string value;

  const int phase1_put_cnt = 10;
  for (int i = 0; i < phase1_put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(Put(key, value), kOk);
  }

  for (int i = 0; i < phase1_put_cnt; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
    EXPECT_EQ(value, "value" + std::to_string(i));
  }

}

}  // namespace kv
