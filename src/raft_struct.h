#pragma once
#include <vector>

#include "log_entry.h"
#include "raft_type.h"

namespace raft {
struct AppendEntriesArgs {
  // Leader's term when sending this AppendEntries RPC.
  raft_term_t term;

  // The leader's identifier
  raft_node_id_t leader_id;

  // The raft index and term of the previous log entry. By "previous" we mean
  // the predecessor of the first log entry contained in this AppendEntries
  // RPC call
  raft_index_t prev_log_index;
  raft_term_t prev_log_term;

  // The raft index of the last log entries that has committed by leader
  raft_index_t leader_commit;

  // The number of entries contained in this AppendEntries arguments
  int64_t entry_cnt;

  // The sequence number of all sent log entries contained in this args
  uint64_t seq;

  // We simply use std::vector to denote an array of log entries. NOTE: This may
  // cause shallow copy multiple times
  std::vector<LogEntry> entries;
};

struct AppendEntriesReply {
  // The raft term of the server when processing one AppendEntries RPC call.
  // Used to update the leader's term
  raft_term_t term;

  // Denote if the follower successfully append specified log entries to its
  // own log manager. Return 1 if append is successful, otherwise returns 0
  int success;

  // The next raft index the raft peer wants the leader to send. If success is
  // true, the expect_index is prev_log_index + entry_cnt + 1; otherwise it is
  // the first index that differs from the leader
  raft_index_t expect_index;

  // The raft node id of the server that makes this reply
  raft_node_id_t reply_id;
};

enum {
  kAppendEntriesArgsHdrSize = sizeof(raft_term_t) * 2 + sizeof(raft_index_t) * 2 +
                              sizeof(uint64_t) * 2 + sizeof(raft_node_id_t)
};

}  // namespace raft
