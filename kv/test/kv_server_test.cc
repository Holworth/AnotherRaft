#include "kv_server.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "kv_format.h"
#include "log_entry.h"
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

  struct DecodedString {
    int k, m;
    raft::raft_frag_id_t frag_id;
    raft::Slice frag;
  };

  DecodedString DecodeString(std::string* str) {
    auto bytes = str->c_str();

    int k = *reinterpret_cast<const int*>(bytes);
    bytes += sizeof(int);

    int m = *reinterpret_cast<const int*>(bytes);
    bytes += sizeof(int);

    auto frag_id = *reinterpret_cast<const raft::raft_frag_id_t*>(bytes);
    bytes += sizeof(raft::raft_frag_id_t);

    auto remaining_size = str->size() - sizeof(int) * 2 - sizeof(raft::raft_frag_id_t);
    return DecodedString{k, m, frag_id,
                         raft::Slice(const_cast<char*>(bytes), remaining_size)};
  }

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

    // The Get operation needs special dealing since it might only receives a fragment
    // value
    if (res != kOk) {
      return res;
    }

    // check if need to search other servers to collect fragments
    auto format = DecodeString(&resp.value);
    if (format.k == 1) {
      GetKeyFromPrefixLengthFormat(format.frag.data(), value);
      return kOk;
    }

    LOG(raft::util::kRaft, "[Get Partial Value: k=%d m=%d], start collecting", format.k,
        format.m);

    int k = format.k, m = format.m;
    raft::Encoder::EncodingResults input;

    // Otherwise we need to collect fragments from all servers and construct the original
    // value based on k, m parameters
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i)) {
        std::string frag_value;
        auto found = servers_[i]->DB()->Get(key, &frag_value);
        if (found) {
          auto format = DecodeString(&frag_value);
          input.insert_or_assign(format.frag_id, raft::Slice::Copy(format.frag));
        }
      }
    }

    raft::Encoder encoder;
    raft::Slice decode_res;
    auto decode_succ = encoder.DecodeSlice(input, k, m, &decode_res);
    if (!decode_succ) {
      return kRPCCallFailed;
    }

    *value = std::string(decode_res.data(), decode_res.size());
    return kOk;
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
          if (resp.raft_term > leader_term) {
            leader_id_ = i;
            leader_term = resp.raft_term;
          }
        }
      }
    }
    return leader_id_;
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
          leader_term = 0;
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
    // std::printf("create server with server=%p db=%p\n", kv_server, kv_server->DB());
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
  raft::raft_term_t leader_term = 0;
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
  // const int test_cnt = 1000;
  // for (int i = 0; i < test_cnt; ++i) {
  //   auto key = "key" + std::to_string(i);
  //   auto value = "value" + std::to_string(i);
  //   EXPECT_EQ(Put(key, value), kOk);
  // }
  //
  // for (int i = 0; i < test_cnt; ++i) {
  //   EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
  //   EXPECT_EQ(value, "value" + std::to_string(i));
  // }
  EXPECT_EQ(Put("key1", "value-abcdefg1"), kOk);

  auto leader1 = GetCurrentLeaderId();
  Disconnect(leader1);

  EXPECT_EQ(Get("key1", &value), kOk);
  EXPECT_EQ(value, "value-abcdefg1");

  ClearTestContext(servers_config);
}

TEST_F(KvServerTest, DISABLED_TestDeleteAndOverwrite) {
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

TEST_F(KvServerTest, DISABLED_TestGetPreviousValueAfterLeaderCrashes) {
  auto servers_config = NodesConfig{
      {0, {{"127.0.0.1", 50001}, "", "./testdb0"}},
      {1, {{"127.0.0.1", 50002}, "", "./testdb1"}},
      {2, {{"127.0.0.1", 50003}, "", "./testdb2"}},
  };

  LaunchAllServers(servers_config);

  std::string value;

  const int phase1_put_cnt = 100;
  for (int i = 0; i < phase1_put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    EXPECT_EQ(Put(key, value), kOk);
  }

  for (int i = 0; i < phase1_put_cnt; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
    EXPECT_EQ(value, "value" + std::to_string(i));
  }

  auto leader1 = GetCurrentLeaderId();
  Disconnect(leader1);

  // Previous value should be read from remaining cluster
  for (int i = 0; i < phase1_put_cnt; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i), &value), kOk);
    EXPECT_EQ(value, "value" + std::to_string(i));
  }
  auto leader2 = GetCurrentLeaderId();
  ASSERT_NE(leader1, leader2);

  // Delete all keys from phase1 and put some new keys in phase2
  const int phase2_put_cnt = 100;

  for (int i = 0; i < phase1_put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    EXPECT_EQ(Delete(key), kOk);
  }

  for (int i = 0; i < phase2_put_cnt; ++i) {
    auto key = "key" + std::to_string(i + phase1_put_cnt);
    auto value = "value" + std::to_string(i + phase1_put_cnt);
    EXPECT_EQ(Put(key, value), kOk);
  }

  // Check results
  for (int i = 0; i < phase1_put_cnt; ++i) {
    auto key = "key" + std::to_string(i);
    EXPECT_EQ(Get(key, &value), kKeyNotExist);
  }

  for (int i = 0; i < phase2_put_cnt; ++i) {
    auto key = "key" + std::to_string(i + phase1_put_cnt);
    auto expect_value = "value" + std::to_string(i + phase1_put_cnt);
    EXPECT_EQ(Get(key, &value), kOk);
    EXPECT_EQ(value, expect_value);
  }

  ClearTestContext(servers_config);
}

}  // namespace kv
