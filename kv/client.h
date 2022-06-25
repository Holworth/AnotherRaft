#pragma once
#include <thread>
#include <unordered_map>

#include "config.h"
#include "kv_node.h"
#include "raft_type.h"
#include "rpc.h"
#include "type.h"
namespace kv {
class KvServiceClient {
  // If a KV Request is not done within 10 seconds
  static const int kKVRequestTimesoutCnt = 10;

 public:
  KvServiceClient(const KvClusterConfig& config);
  ~KvServiceClient();

 public:
  ErrorType Put(const std::string& key, const std::string& value);
  ErrorType Get(const std::string&, std::string* value);
  ErrorType Delete(const std::string& key);

  raft::raft_node_id_t LeaderId() const { return curr_leader_; }

 private:
  raft::raft_node_id_t DetectCurrentLeader();
  Response WaitUntilRequestDone(const Request& request);
  void sleepMs(int cnt) { std::this_thread::sleep_for(std::chrono::milliseconds(cnt)); }

 private:
  rpc::KvServerRPCClient* GetRPCStub(raft::raft_node_id_t id) { return servers_[id]; }

 private:
  std::unordered_map<raft::raft_node_id_t, rpc::KvServerRPCClient*> servers_;
  raft::raft_node_id_t curr_leader_;
  static const raft::raft_node_id_t kNoDetectLeader = -1;
  raft::raft_term_t curr_leader_term_;
};
}  // namespace kv
