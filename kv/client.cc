#include "client.h"

#include "config.h"
#include "encoder.h"
#include "type.h"
#include "util.h"
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
  LOG(raft::util::kRaft, "Client start deal with request (%s)",
      ToString(request).c_str());
  while (timer.ElapseMilliseconds() < kKVRequestTimesoutCnt * 1000) {
    if (curr_leader_ == kNoDetectLeader && DetectCurrentLeader() == kNoDetectLeader) {
      LOG(raft::util::kRaft, "Detect No Leader");
      sleepMs(300);
      continue;
    }
    LOG(raft::util::kRaft, "Client send request (%s) to %d", ToString(request).c_str(),
        curr_leader_);
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
        LOG(raft::util::kRaft, "Client Receive Response(err=%s), fallback to nonleader",
            ToString(resp.err).c_str());
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

  LOG(raft::util::kRaft, "Client Recv Response from S%d", resp.reply_server_id);

  if (resp.err != kOk) {
    return resp.err;
  }

  // Decoding the response byte array for further information: we may need to collect
  // other fragments
  auto format = DecodeString(&resp.value);

  if (format.k == 1) {
    GetKeyFromPrefixLengthFormat(format.frag.data(), value);
    return kOk;
  }

  LOG(raft::util::kRaft, "[Get Partial Value: k=%d m=%d readindex=%d], start collecting",
      format.k, format.m, resp.read_index);

  // Start gather get value,
  // TODO: This might be encapsulated into a task process?
  int k = format.k, m = format.m;
  raft::Encoder::EncodingResults input;
  input.insert({format.frag_id,
                raft::Slice(const_cast<char*>(format.frag.data()), format.frag.size())});

  std::string final_value;
  std::atomic<bool> gather_value_done = false;
  ErrorType gather_value_err;
  auto call_back = [=, &input, &gather_value_done,
                    &gather_value_err](const GetValueResponse& resp) {
    LOG(raft::util::kRaft, "Client Recv GetValue Response from S%d",
        resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }
    auto fmt = DecodeString(const_cast<std::string*>(&resp.value));
    if (fmt.k == k && fmt.m == m) {
      input.insert({fmt.frag_id, raft::Slice::Copy(raft::Slice(format.frag.data(),
                                                               format.frag.size()))});
    }

    if (input.size() >= k) {
      raft::Encoder encoder;
      raft::Slice results;
      auto stat = encoder.DecodeSlice(input, k, m, &results);
      if (stat) {
        *value = std::string(results.data(), results.size());
        gather_value_done.store(true);
      } else {
        gather_value_err = kKVDecodeFail;
      }
    }
  };

  // issues parallel GetValue RPC to all nodes and decode the value when receiving
  // at least F response
  auto get_req = GetValueRequest{key, resp.read_index};
  for (auto& [id, server] : servers_) {
    if (id != resp.reply_server_id) {
      GetRPCStub(id)->GetValue(get_req, nullptr);
    }
  }

  LOG(raft::util::kRaft, "Client Loop until gather value process done");
  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() < 1000) {
    while (gather_value_done.load() == false) {
      ;
    }
    *value = final_value;
    return kOk;
  }
  return kRequestExecTimeout;
}

ErrorType KvServiceClient::Delete(const std::string& key) {
  Request request = {kDelete, 0, 0, key, ""};
  auto resp = WaitUntilRequestDone(request);
  return resp.err;
}

raft::raft_node_id_t KvServiceClient::DetectCurrentLeader() {
  for (auto& [id, stub] : servers_) {
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
