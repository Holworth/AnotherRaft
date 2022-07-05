#pragma once
#include <cstring>
#include <map>
#include <unordered_map>

#include "encoder.h"
#include "log_manager.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "rsm.h"
#include "util.h"

namespace raft {

class Storage;

enum RaftRole {
  kFollower = 1,
  kCandidate = 2,
  kLeader = 3,
};

namespace config {
const int64_t kHeartbeatInterval = 100;  // 100ms
};

struct RaftConfig {
  // The node id of curernt peer. A node id is the unique identifier to
  // distinguish different raft peers
  raft_node_id_t id;

  // The raft node id and corresponding network address of all raft peers
  // in current cluster. (including current server itself)
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients;

  // Persistence storage, which is used to recover from failure, could be
  // nullptr. If storage is nullptr, any change to RaftState will not be
  // persisted
  Storage *storage;

  int64_t electionTimeMin, electionTimeMax;

  Rsm *rsm;
};

struct ProposeResult {
  raft_index_t propose_index;
  raft_term_t propose_term;
  bool is_leader;
};

// A monitor that records the number of server that is still alive in current cluster
struct LivenessMonitor {
  static constexpr int kMaxNodeNum = 10;
  int init_num;
  bool response[kMaxNodeNum];
  raft_node_id_t id;  // current server's id

  void Init() { std::memset(response, true, sizeof(response)); }

  void Reset() {
    std::memset(response, false, sizeof(response));
    response[id] = true;
  }

  void SetResponse(raft_node_id_t id) { response[id] = true; }

  int LiveNumber() const {
    int cnt = 0;
    for (int i = 0; i < init_num; ++i) {
      cnt += (response[i]);
    }
    return cnt;
  }

  bool IsAlive(raft_node_id_t target_id) const { return response[target_id]; }
};

struct SequenceGenerator {
 public:
  void Reset() { seq = 1; }
  uint64_t Next() { return seq++; }

 private:
  uint64_t seq;
};

// A raft peer maintains the necessary information in terms of "Logic" state
// of raft algorithm
class RaftPeer {
 public:
  RaftPeer() : next_index_(0), match_index_(0) {}

  raft_index_t NextIndex() const { return next_index_; }
  void SetNextIndex(raft_index_t next_index) { next_index_ = next_index; }

  raft_index_t MatchIndex() const { return match_index_; }
  void SetMatchIndex(raft_index_t match_index) { match_index_ = match_index; }

  void RemoveMatchVersionAt(raft_index_t idx) {
    matchVersion.erase(matchVersion.find(idx));
  }

 public:
  raft_index_t next_index_, match_index_;
  std::unordered_map<raft_index_t, Version> matchVersion;
};

class RaftState {
 public:
  // Construct a RaftState instance from a specified configuration.
  static RaftState *NewRaftState(const RaftConfig &);
  static const raft_node_id_t kNotVoted = -1;

 public:
  RaftState() = default;

  RaftState(const RaftState &) = delete;
  RaftState &operator=(const RaftState &) = delete;

 public:
  // Process a bunch of RPC request or response, the first parameter is the
  // input of this process, the second parameter is the output.
  void Process(RequestVoteArgs *args, RequestVoteReply *reply);
  void Process(RequestVoteReply *reply);

  void Process(AppendEntriesArgs *args, AppendEntriesReply *reply);
  void Process(AppendEntriesReply *reply);

  // This is a command from upper level application, the raft instance is supposed to
  // copy this entry to its own log and replicate it to other followers
  ProposeResult Propose(const CommandData &command);

 public:
  // Init all necessary status of raft state, including reset election timer
  void Init();

  // The driver clock periodically call the tick function to so that raft peer
  // make progress
  void Tick();

  raft_term_t CurrentTerm() const { return current_term_; }
  void SetCurrentTerm(raft_term_t term) { current_term_ = term; }

  raft_node_id_t VoteFor() const { return vote_for_; }
  void SetVoteFor(raft_node_id_t node) { vote_for_ = node; }

  RaftRole Role() const { return role_; }
  void SetRole(RaftRole role) { role_ = role; }

  // ALERT: This public interface should only be used in test case
  void SetVoteCnt(int cnt) { vote_me_cnt_ = cnt; }

  raft_index_t CommitIndex() const { return commit_index_; }
  void SetCommitIndex(raft_index_t raft_index) { commit_index_ = raft_index; }

  raft_index_t LastLogIndex() const { return lm_->LastLogEntryIndex(); }
  raft_term_t TermAt(raft_index_t raft_index) const { return lm_->TermAt(raft_index); }

 private:
  // Check specified raft_index and raft_term is newer than log entries stored
  // in current raft peer. Return true if it is, otherwise returns false
  bool isLogUpToDate(raft_index_t raft_index, raft_term_t raft_term);

  // Check if current raft peer has exactly an entry of specified raft_term at
  // specific raft_index
  bool containEntry(raft_index_t raft_index, raft_term_t raft_term);

  // When receiving AppendEntries Reply, the raft peer checks all peers match index
  // condition and may update the commit_index field
  void tryUpdateCommitIndex();

  void tryApplyLogEntries();

  // Iterate through the entries carried by input args and check if there is conflicting
  // entry: Same index but different term. If there is one, delete all following entries.
  // Add any new entries that are not in raft's log
  void checkConflictEntryAndAppendNew(AppendEntriesArgs *args, AppendEntriesReply *reply);

  // Reset the next index and match index fields when current server becomes leader
  void resetNextIndexAndMatchIndex();

  void tickOnFollower();
  void tickOnCandidate();
  void tickOnLeader();

  void resetElectionTimer();
  void resetHeartbeatTimer();

  void convertToFollower(raft_term_t term);
  void convertToCandidate();
  void convertToLeader();

  void persistVoteFor();
  void persistCurrentTerm();

  // A private function that is used to start a new election
  void startElection();

  // Replicate entries to all other raft peers
  void broadcastHeartbeat();

  void incrementVoteMeCnt() { vote_me_cnt_++; }

  // For a cluster consists of 2F+1 server, F is called the liveness
  // level, which is the maximum number of failure servers the cluster
  // can tolerant
  int livenessLevel() const { return peers_.size() / 2; }

  // Send heartbeat messages to target raft peer
  void sendHeartBeat(raft_node_id_t peer);

  // Send appendEntries messages to target raft peer
  void sendAppendEntries(raft_node_id_t peer);

  void initLivenessMonitorState() {
    live_monitor_.Init();
  }

  void removeLastReplicateVersionAt(raft_index_t idx) {
    last_replicate_.erase(last_replicate_.find(idx));
  };
  void removeTrackVersionOfAll(raft_index_t idx) {
    for (auto &[id, node] : peers_) {
      node->RemoveMatchVersionAt(idx);
    }
  };

  // In flexibleK, the leader needs to send AppendEntries arguments in every
  // heartbeat round
  void replicateEntries();

 private:
  // For concurrency control. A raft state instance might be accessed via
  // multiple threads, e.g. RPC thread that receives request; The state machine
  // thread that peridically apply committed log entries, and so on
  std::mutex mtx_;

  // The id of current raft peer
  raft_node_id_t id_;

  // Record current raft peer's state is Follower, or Candidate, or Leader
  RaftRole role_;

  // Current Term of raft peer, initiated to be 0 when first bootsup
  raft_term_t current_term_;

  // The peer that this peer has voted in current term, initiated to be -1
  // when first bootsup
  raft_node_id_t vote_for_;

  // The raft index of log entry that has been committed and applied to state
  // machine, does not need persistence
  raft_index_t commit_index_;
  raft_index_t last_applied_;

  // Manage all log entries
  LogManager *lm_;
  Storage *storage_;

  // For FlexibleK and CRaft: We need to detect the number of live servers
  LivenessMonitor live_monitor_;
  Encoder encoder_;
  SequenceGenerator seq_gen_;
  // For each index, there is an associated stripe that contains the encoded data
  std::map<raft_index_t, Stripe *> encoded_stripe_;
  // For each index, last_replicate contains the recent replicate version
  std::unordered_map<raft_index_t, Version> last_replicate_;

 private:
  std::unordered_map<raft_node_id_t, RaftPeer *> peers_;
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients_;

  util::Timer election_timer_;   // Record elapse time during election
  util::Timer heartbeat_timer_;  // Record elapse time since last heartbeat

  // Election time should be between [min, max), set by configuration
  int64_t electionTimeLimitMin_, electionTimeLimitMax_;
  // A randomized election timeout based on above interval
  int64_t election_time_out_;
  int64_t heartbeatTimeInterval;

 private:
  int vote_me_cnt_;
  Rsm *rsm_;
};
}  // namespace raft
