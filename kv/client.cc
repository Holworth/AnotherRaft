#include "client.h"

#include "config.h"
#include "type.h"
#include "util.h"
namespace kv {
KvServiceClient::KvServiceClient(const KvClusterConfig& config, uint32_t client_id)
    : client_id_(client_id) {
  for (const auto& [id, conf] : config) {
    servers_.insert({id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  curr_leader_ = kNoDetectLeader;
  curr_leader_term_ = 0;
  LOG(raft::util::kRaft, "[C%d] Init Client, CurrLeader=%d CurrLeaderTerm=%d", ClientId(),
      curr_leader_, curr_leader_term_);
}

KvServiceClient::~KvServiceClient() {
  for (auto [id, ptr] : servers_) {
    delete ptr;
  }
}

Response KvServiceClient::WaitUntilRequestDone(const Request& request) {
  raft::util::Timer timer;
  timer.Reset();
  LOG(raft::util::kRaft, "[C%d] Start Dealing with request (%s)", ClientId(),
      ToString(request).c_str());
  while (timer.ElapseMilliseconds() < kKVRequestTimesoutCnt * 1000) {
    if (curr_leader_ == kNoDetectLeader && DetectCurrentLeader() == kNoDetectLeader) {
      LOG(raft::util::kRaft, "[C%d] Detect No Leader", ClientId());
      sleepMs(300);
      continue;
    }
    LOG(raft::util::kRaft, "[C%d] send request (%s) to %d", ClientId(),
        ToString(request).c_str(), curr_leader_);
    auto resp = GetRPCStub(curr_leader_)->DealWithRequest(request);
    switch (resp.err) {
      case kOk:
      case kKeyNotExist:
        return resp;

      case kEntryDeleted:
      // The leader might be separated from the cluster
      case kRequestExecTimeout:
      case kNotALeader:
      case kRPCCallFailed:
        LOG(raft::util::kRaft, "[C%d] Recv Response(err=%s), Fallback to nonleader",
            ClientId(), ToString(resp.err).c_str());
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

OperationResults KvServiceClient::Put(const std::string& key, const std::string& value) {
  Request request = {kPut, ClientId(), 0, key, value};
  auto resp = WaitUntilRequestDone(request);
  return {resp.err, resp.apply_elapse_time};
}

OperationResults KvServiceClient::Get(const std::string& key, std::string* value) {
  Request request = {kGet, ClientId(), 0, key, std::string("")};
  auto resp = WaitUntilRequestDone(request);
  if (resp.err == kOk) {
    *value = resp.value;
  }
  return {resp.err, resp.apply_elapse_time};
}

OperationResults KvServiceClient::Delete(const std::string& key) {
  Request request = {kDelete, ClientId(), 0, key, ""};
  auto resp = WaitUntilRequestDone(request);
  return {resp.err, resp.apply_elapse_time};
}

raft::raft_node_id_t KvServiceClient::DetectCurrentLeader() {
  for (auto& [id, stub] : servers_) {
    if (stub == nullptr) {
      continue;
    }
    Request detect_request = {kDetectLeader, ClientId(), 0, "", ""};
    auto resp = GetRPCStub(id)->DealWithRequest(detect_request);
    if (resp.err == kOk) {
      LOG(raft::util::kRaft,
          "[C%d] DetectLeader Req response with (kOk, raftTerm=%d), Local "
          "CurrentLeader=%d CurrentLeaderTerm=%d",
          ClientId(), resp.raft_term, curr_leader_, curr_leader_term_);
      if (resp.raft_term > curr_leader_term_) {
        curr_leader_ = id;
        curr_leader_term_ = resp.raft_term;
      }
    }
  }
  return curr_leader_;
}

}  // namespace kv
