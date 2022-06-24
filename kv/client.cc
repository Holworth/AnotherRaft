#include "client.h"

#include "config.h"
#include "type.h"
namespace kv {
KvServiceClient::KvServiceClient(const KvClusterConfig& config) {
  for (const auto& [id, conf] : config) {
    servers_.insert({id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  curr_leader_ = kNoDetectLeader;
}

KvServiceClient::~KvServiceClient() {
  for (auto [id, ptr] : servers_) {
    delete ptr;
  }
}

Response KvServiceClient::WaitUntilRequestDone(const Request& request) {
  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() < kKVRequestTimesoutCnt * 1000) {
    if (curr_leader_ == kNoDetectLeader && DetectCurrentLeader() == kNoDetectLeader) {
      LOG(raft::util::kRaft, "Detect No Leader");
      sleepMs(300);
      continue;
    }
    auto resp = GetRPCStub(curr_leader_)->DealWithRequest(request);
    switch (resp.err) {
      case kOk:
      case kKeyNotExist:
        return resp;

      case kEntryDeleted:
        break;

      // The leader might be separated from the cluster
      case kRequestExecTimeout:
      case kNotALeader:
        curr_leader_ = kNoDetectLeader;
        curr_leader_term_ = 0;
        break;

      default:
        assert(false);
    }
  }
  // Timeout
  Response resp;
  resp.err = kRequestExecTimeout;
  return resp;
}

ErrorType KvServiceClient::Put(const std::string& key, const std::string& value) {
  Request request = {kPut, 0, 0, key, value};
  auto resp = WaitUntilRequestDone(request);
  return resp.err;
}

ErrorType KvServiceClient::Get(const std::string& key, std::string* value) {
  Request request = {kGet, 0, 0, key, std::string("")};
  auto resp = WaitUntilRequestDone(request);
  if (resp.err == kOk) {
    *value = resp.value;
  }
  return resp.err;
}

ErrorType KvServiceClient::Delete(const std::string &key) {
  Request request = {kDelete, 0, 0, key, ""};
  auto resp = WaitUntilRequestDone(request);
  return resp.err;
}

raft::raft_node_id_t KvServiceClient::DetectCurrentLeader() {
  for (auto &[id, stub] : servers_) {
    if (stub == nullptr) {
      continue;
    }
    Request detect_request = {kDetectLeader, 0, 0, "", ""};
    auto resp = GetRPCStub(id)->DealWithRequest(detect_request);
    if (resp.err == kOk) {
      if (resp.raft_term > curr_leader_term_) {
        curr_leader_ = id;
        curr_leader_term_ = resp.raft_term;
      }
    }
  }
  return curr_leader_;
}

}  // namespace kv
