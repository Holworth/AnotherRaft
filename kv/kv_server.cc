#include "kv_server.h"

#include <cstdio>
#include <mutex>
#include <tuple>

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
  kv_server->db_ = StorageEngine::Default(config.storage_engine_name);
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
  resp->raft_term = raft_->getRaftState()->CurrentTerm();

  switch (request->type) {
    case kDetectLeader:
      resp->err = raft_->IsLeader() ? kOk : kNotALeader;
      LOG(raft::util::kRaft, "S%d reply err: %s, term=%d", id_,
          ToString(resp->err).c_str(), resp->raft_term);
      return;
    case kPut:
    case kDelete: {
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
          resp->apply_elapse_time = ar.apply_time;
          LOG(raft::util::kRaft, "S%d ApplyResult value=%s", id_, resp->value.c_str());
          return;
        }
      }
      // Otherwise timesout
      resp->err = kRequestExecTimeout;
      return;
    }
    case kGet: {
      ExecuteGetOperation(request, resp);
      return;
    }
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
    apply->apply_time = ar.apply_time;
  } else {
    apply->err = ar.err;
    apply->value = ar.value;
    apply->apply_time = ar.apply_time;
  }
  return true;
}

void KvServer::ApplyRequestCommandThread(KvServer* server) {
  raft::util::Timer elapse_timer;
  while (!server->exit_.load()) {
    raft::LogEntry ent;
    if (!server->channel_->TryPop(ent)) {
      continue;
    }
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
        elapse_timer.Reset();
        server->db_->Put(req.key, req.value);
        ar.apply_time = elapse_timer.ElapseMicroseconds();
        break;
      case kDelete:
        elapse_timer.Reset();
        server->db_->Delete(req.key);
        ar.apply_time = elapse_timer.ElapseMicroseconds();
        break;
      default:
        assert(0);
    }
    // Update applied index for Get operation
    server->applied_index_ = ent.Index();
    LOG(raft::util::kRaft, "S%d Apply request(%s) to db Done", server->Id(),
        ToString(req).c_str());

    // Add the apply result into map
    std::scoped_lock<std::mutex> lck(server->map_mutex_);
    server->applied_cmds_.insert({ent.Index(), ar});
  }
}

void KvServer::ExecuteGetOperation(const Request* request, Response* resp) {
  auto read_index = this->raft_->LastIndex();
  LOG(raft::util::kRaft, "S%d Execute Get Operation, ReadIndex=%d", id_, read_index);

  // spin until the entry has been applied
  raft::util::Timer timer;
  timer.Reset();
  while (LastApplyIndex() < read_index) {
    if (timer.ElapseMilliseconds() >= 300) {
      LOG(raft::util::kRaft, "S%d Execute Get Operation Timeout, ReadIndex=%d", id_,
          read_index);
      resp->err = kRequestExecTimeout;
      return;
    }
    LOG(raft::util::kRaft, "S%d Execute Get Operation(ApplyIndex:%d) ReadIndex%d", id_,
        LastApplyIndex(), read_index);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto succ = db_->Get(request->key, &(resp->value));
  if (!succ) {
    resp->err = kKeyNotExist;
    return;
  } else {
    resp->err = kOk;
    return;
  }
}
}  // namespace kv
