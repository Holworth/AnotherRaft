#include "kv_server.h"

#include <mutex>

#include "kv_format.h"
#include "log_entry.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "storage_engine.h"
#include "type.h"
#include "util.h"
namespace kv {
KvServer* KvServer::NewKvServer(const KvServerConfig& config) {
  auto kv_server = new KvServer();
  kv_server->channel_ = Channel::NewChannel(100000);
  kv_server->engine_ = StorageEngine::Default(config.storage_engine_name);
  kv_server->id_ = config.raft_node_config.node_id_me;

  // Pass channel as a Rsm into raft
  auto raft_config = config.raft_node_config;
  raft_config.rsm = kv_server->channel_;
  kv_server->raft_ = new raft::RaftNode(raft_config);

  kv_server->exit_ = false;
  return kv_server;
}

void KvServer::Start() {
  raft_->Start();
  startApplyKvRequestCommandsThread();
}

void KvServer::DealWithRequest(const Request* request, Response* resp) {
  LOG(raft::util::kRaft, "S%d deal with req %s", id_, ToString(*request).c_str());

  resp->type = request->type;
  resp->client_id = request->client_id;
  resp->sequence = request->sequence;

  switch (request->type) {
    case kDetectLeader:
      resp->err = raft_->IsLeader() ? kOk : kNotLeader;
      return;
    case kPut:
    case kDelete:
    // kGet may not need to go along this road?
    case kGet:
      auto size = GetRawBytesSizeForRequest(*request);
      auto data = new char[size];
      RequestToRawBytes(*request, data);

      // Construct a raft command
      auto cmd = raft::CommandData{static_cast<int>(size), raft::Slice(data, size)};
      auto pr = raft_->Propose(cmd);

      // Loop until the propose entry to be applied
      raft::util::Timer timer;
      timer.Reset();
      KvRequestApplyResult ar;
      while (timer.ElapseMilliseconds() <= 300) {
        // Check if applied
        if (CheckEntryCommitted(pr, &ar)) {
          resp->err = ar.err;
          resp->value = ar.value;
          LOG(raft::util::kRaft, "S%d ApplyResult value=%s", id_, resp->value.c_str());
          return;
        }
      }
      // Otherwise timesout
      resp->err = kRequestExecTimeout;
      return;
  }
}

bool KvServer::CheckEntryCommitted(const raft::ProposeResult& pr,
                                   KvRequestApplyResult* apply) {
  // Not committed entry
  std::scoped_lock<std::mutex> lck(map_mutex_);
  if (applied_cmds_.count(pr.propose_index) == 0) {
    return false;
  }

  auto ar = applied_cmds_[pr.propose_index];
  apply->raft_term = ar.raft_term;
  if (ar.raft_term != pr.propose_term) {
    apply->err = kEntryDeleted;
    apply->value = "";
  } else {
    apply->err = kOk;
    apply->value = ar.value;
  }
  return true;
}

void KvServer::ApplyRequestCommandThread(KvServer* server) {
  while (!server->exit_.load()) {
    // Read committed entry from raft
    raft::LogEntry ent = server->channel_->Pop();
    LOG(raft::util::kRaft, "S%d Pop Ent From Raft I%d T%d", server->Id(), ent.Index(),
        ent.Term());

    // Apply this entry to state machine(i.e. Storage Engine)
    Request req;
    RawBytesToRequest(ent.CommandData().data(), &req);

    LOG(raft::util::kRaft, "S%d Apply request(%s) to db", server->Id(),
        ToString(req).c_str());

    std::string get_value;
    KvRequestApplyResult ar = {ent.Term(), kOk, std::string("")};
    switch (req.type) {
      case kPut:
        server->engine_->Put(req.key, req.value);
        break;
      case kDelete:
        server->engine_->Delete(req.key);
        break;
      case kGet:
        if (!server->engine_->Get(req.key, &get_value)) {
          ar.err = kKeyNotExist;
          ar.value = "";
        } else {
          ar.err = kOk;
          ar.value = std::move(get_value);
          LOG(raft::util::kRaft, "S%d apply Get command (get value=%s)", server->Id(),
              ar.value.c_str());
        }
        break;
      default:
        assert(0);
    }
    // Add the apply result into map
    std::scoped_lock<std::mutex> lck(server->map_mutex_);
    server->applied_cmds_.insert({ent.Index(), ar});
  }
}
}  // namespace kv
