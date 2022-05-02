#pragma once
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

// Persister is an interface used for retriving persisted raft status
// including current term, voteFor, persisted log entries and so on
class Persister {
public:
  struct PersistRaftState {
    // Indicating if this state is valid, for example, when creating
    // a persister on first bootstrap, there is no valid raft state yet
    bool valid;
    raft_term_t persisted_term;
    raft_node_id_t persisted_vote_for;
  };

public:
  // LastIndex returns the raft index of last persisted log entries, if
  // there is no valid log entry, returns 0
  virtual raft_index_t LastIndex() const = 0;

  // Return the persisted raft state. Mark valid field as false if there
  // is no valid raft state
  virtual PersistRaftState PersistState() const = 0;

  // Get all persisted but not discarded log entries and store via a
  // temporary vector variable
  virtual void LogEntries(std::vector<LogEntry> *entries) const = 0;
};

// This class is only for unit test, it is used for simulating the behaviour
// of a persister by place them simply in memory. The test module may feel
// free to inherit this class and override corresponding methods
class MemPersister : public Persister {
public:
  raft_index_t LastIndex() const override {
    if (!persisted_entries_.size()) {
      return 0;
    } else {
      return (persisted_entries_.end() - 1)->Index();
    }
  }

  PersistRaftState PersistState() const override {
    return {true, persisted_term_, persisted_vote_for_};
  }

  void LogEntries(std::vector<LogEntry> *entries) const override {
    *entries = persisted_entries_;
  }

protected:
  raft_term_t persisted_term_;
  raft_node_id_t persisted_vote_for_;
  std::vector<LogEntry> persisted_entries_;
};

} // namespace raft
