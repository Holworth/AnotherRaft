#pragma once
#include <mutex>
#include <unordered_map>

#include "kv_format.h"
#include "kv_rsm.h"
#include "raft.h"
#include "raft_node.h"
#include "type.h"
namespace kv {
class KvServer {
 public:
  struct KvRequestApplyResult {
    raft::raft_term_t raft_term;
    ErrorType err;
    std::string value;
  };

 public:
  void DealWithRequest(const Request* request, Response* resp);

 private:
  // Check if a log entry has been committed yet
  bool CheckEntryCommitted(const raft::ProposeResult& pr, KvRequestApplyResult* apply);

  // A thread that periodically apply committed raft log entries to KVRsm
  void ApplyRequestCommandThread();

 private:
  raft::RaftNode* raft_;
  KVRsm* kv_stm_;
  // We need a channel that receives apply messages from raft
  // We need a concurrent map to record which entries have been applied, associated with
  // their term and value (if it is a Get operation)
  std::unordered_map<raft::raft_index_t, KvRequestApplyResult> applied_cmds_;
  std::mutex map_mutex_;
};
}  // namespace kv
