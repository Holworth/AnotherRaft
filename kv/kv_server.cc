#include "kv_server.h"
#include <mutex>

#include "log_entry.h"
#include "raft_struct.h"
#include "type.h"
namespace kv {
void KvServer::DealWithRequest(const Request* request, Response* resp) {
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
      KvRequestApplyResult ar;
      while (timer.ElapseMilliseconds() <= 300) {
        // Check if applied
        if (CheckEntryCommitted(pr, &ar)) {
          resp->err = ar.err;
          resp->value = ar.value;
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

void KvServer::ApplyRequestCommandThread() {
  while (true) {
    // Read data from concurrent queue, the thread should be blocked if there is no
    // entry yet
    raft::LogEntry ent;
    // Only apply this ent when it is valid
    kv_stm_->ApplyLogEntry(ent);

    
    // Add the apply result into map
    KvRequestApplyResult ar;
    ar.raft_term = ent.Term();
    ar.value = std::string("");
    ar.err = kOk;
    std::scoped_lock<std::mutex> lck(map_mutex_);
    applied_cmds_.insert({ent.Index(), ar});
  }
}
}  // namespace kv
