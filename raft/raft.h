#pragma once
#include <cstring>
#include <map>
#include <mutex>
#include <unordered_map>

#include "encoder.h"
#include "log_entry.h"
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
  kPreLeader = 3,
  kLeader = 4,
};

namespace config {
const int64_t kHeartbeatInterval = 100;         // 100ms
const int64_t kCollectFragmentsInterval = 100;  // 100ms
const int64_t kReplicateInterval = 500;         // 500ms
const int64_t kElectionTimeoutMin = 500;        // 500ms
const int64_t kElectionTimeoutMax = 1000;       // 800ms
const int kHRaftEncodingK = 4;
const int kHRaftEncodingM = 3;
};  // namespace config

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
  static constexpr int kLivenessTimeoutInterval = 500;
  int node_num;
  bool response[kMaxNodeNum];
  uint64_t response_time[kMaxNodeNum];
  raft_node_id_t me;  // current server's id
  util::Timer timer;

  // void Init() { std::memset(response, true, sizeof(response)); }

  void Init() {
    timer.Reset();
    response[me] = true;
    response_time[me] = 0;
  }

  void UpdateLiveness(raft_node_id_t id) {
    response[id] = true;
    response_time[id] = timer.ElapseMilliseconds();

    // Update other server's state
    auto elapsed = response_time[id];
    for (int i = 0; i < node_num; ++i) {
      if (response[i] && (elapsed - response_time[i]) < kLivenessTimeoutInterval) {
        response[i] = true;
      } else {
        response[i] = false;
      }
    }
    response[me] = true;
  }

  int LiveNumber() const {
    int cnt = 0;
    for (int i = 0; i < node_num; ++i) {
      cnt += (response[i]);
    }
    return cnt;
  }

  // void UpdateLivenessState() {
  //   auto elapsed = timer.ElapseMilliseconds();
  //   for (int i = 0; i < node_num; ++i) {
  //     if (response[i] && (elapsed - response_time[i]) < 100) {
  //       response[i] = true;
  //     } else {
  //       response[i] = false;
  //     }
  //   }
  //   response[me] = true;
  // }

  bool IsAlive(raft_node_id_t target_id) const { return response[target_id]; }
};

struct SequenceGenerator {
 public:
  void Reset() { seq = 1; }
  uint64_t Next() { return seq++; }

 private:
  uint64_t seq;
};

struct PreLeaderStripeStore {
  PreLeaderStripeStore() = default;

  // [start_index, end_index] is the range of index that preLeader collects
  raft_index_t start_index, end_index;
  std::vector<Stripe> stripes;
  bool response_[15];
  int node_num;
  raft_node_id_t me;

  void InitRequestFragmentsTask(raft_index_t start, raft_index_t end, int node_num,
                                raft_node_id_t me) {
    this->start_index = start;
    this->end_index = end;
    this->node_num = node_num;
    this->me = me;
    stripes.clear();
    stripes.reserve(end - start + 1);

    int stripe_cnt = end - start + 1;
    for (int i = 0; i < stripe_cnt; ++i) {
      stripes.push_back(Stripe());
    }

    // Initiate stripe
    for (auto stripe : stripes) {
      // stripe.Init();
    }

    memset(response_, false, sizeof(response_));
    response_[me] = true;
  }

  void UpdateResponseState(raft_node_id_t id) { response_[id] = true; }

  bool IsCollected(raft_node_id_t id) const { return response_[id]; }

  int CollectedFragmentsCnt() const {
    int ret = 0;
    for (int i = 0; i < node_num; ++i) {
      ret += response_[i];
    }
    return ret;
  }

  void AddFragments(raft_index_t idx, const LogEntry &entry) {
    if (idx < start_index || idx > end_index) {
      // NOTE: idx > end_index indicates that current leader receives an entry
      // with index higher than leader's last index, in that way, it simply cut off
      // these entries because the majority of the servers doesn't have this
      // entry(otherwise the leader won't win this election)
      return;
    }
    auto array_index = idx - start_index;
    stripes[array_index].collected_fragments.push_back(entry);
  }
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
    if (matchVersion.count(idx) != 0) {
      matchVersion.erase(matchVersion.find(idx));
    }
  }

 public:
  raft_index_t next_index_, match_index_;
  std::unordered_map<raft_index_t, Version> matchVersion;
  std::unordered_map<raft_index_t, std::vector<Version>> matchVersions_;
};

class RaftState {
  using MappingTable = std::map<raft_node_id_t, std::vector<raft_frag_id_t>>;

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

  void Process(RequestFragmentsArgs *args, RequestFragmentsReply *reply);
  void Process(RequestFragmentsReply *reply);

  // This is a command from upper level application, the raft instance is supposed to
  // copy this entry to its own log and replicate it to other followers
  ProposeResult Propose(const CommandData &command);

  raft::raft_index_t LastIndex() {
    std::scoped_lock<std::mutex> mtx(this->mtx_);
    return lm_->LastLogEntryIndex();
  }

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

  int HRaftK() const { return hraft_k_; }
  int HRaftM() const { return hraft_m_; }

 public:
  // This function receives a proposed data, encode it and transfer it when there are
  // fail servers. This function is only used to do microbench on retransfer
  ProposeResult ReTransferOnFailure(const CommandData &cmd_data);

  void ReTransferRaftEntry(raft_index_t raft_index);

  // Construct a mapping table that only contains the retransfer fragments
  MappingTable ConstructReTransferMappingTable();

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

  // Encoding specified log entry with encoding parameter k, m, the results is written
  // into specified stripe
  void EncodingRaftEntry(raft_index_t raft_index, int k, int m, VersionNumber version_num,
                         Stripe *stripe);

  // Decoding all fragments contained in a stripe into a complete log entry
  bool DecodingRaftEntry(Stripe *stripe, LogEntry *ent);

  bool NeedOverwriteLogEntry(const Version &old_version, const Version &new_version);

  void FilterDuplicatedCollectedFragments(Stripe &stripes);

  bool FindFullEntryInStripe(const Stripe *stripe, LogEntry *ent);

  // Iterate through the entries carried by input args and check if there is conflicting
  // entry: Same index but different term. If there is one, delete all following entries.
  // Add any new entries that are not in raft's log
  void checkConflictEntryAndAppendNew(AppendEntriesArgs *args, AppendEntriesReply *reply);

  // Reset the next index and match index fields when current server becomes leader
  void resetNextIndexAndMatchIndex();

  uint32_t NextSequence() { return seq_gen_.Next(); }

  void tickOnFollower();
  void tickOnCandidate();
  void tickOnLeader();
  void tickOnPreLeader();

  void resetElectionTimer();
  void resetHeartbeatTimer();
  void resetPreLeaderTimer();
  void resetReplicateTimer();

  void convertToFollower(raft_term_t term);
  void convertToCandidate();
  void convertToLeader();
  void convertToPreLeader();

  void PersistRaftState();

  // A private function that is used to start a new election
  void startElection();

  // Replicate entries to all other raft peers
  void broadcastHeartbeat();

  // Collect all needed fragments
  void collectFragments();

  void incrementVoteMeCnt() { vote_me_cnt_++; }

  // For a cluster consists of 2F+1 server, F is called the liveness
  // level, which is the maximum number of failure servers the cluster
  // can tolerant
  int livenessLevel() const { return peers_.size() / 2; }

  // Send heartbeat messages to target raft peer
  void sendHeartBeat(raft_node_id_t peer);

  // Send appendEntries messages to target raft peer
  void sendAppendEntries(raft_node_id_t peer);

  void initLivenessMonitorState() { live_monitor_.Init(); }

  void removeLastReplicateVersionAt(raft_index_t idx) {
    last_replicate_.erase(last_replicate_.find(idx));
  };
  void removeTrackVersionOfAll(raft_index_t idx) {
    for (auto &[id, node] : peers_) {
      node->RemoveMatchVersionAt(idx);
    }
  };

  // Construct a mapping table from current live followers and encoding fragments
  MappingTable ConstructMappingTable();

  int GetClusterServerNumber() const { return peers_.size(); }

  // In flexibleK, the leader needs to send AppendEntries arguments in every
  // heartbeat round
  void replicateEntries();

  // The preleader will try becoming leader if all requested fragments are
  // decoded into complete log entries
  void PreLeaderBecomeLeader();

  void DecodeCollectedStripe();

 public:
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
  //
  // (kqh): For simplicity we use multiple LogManager to contain different fragments
  // each different fragments is added to different LogManager. Then we need a "main"
  // log manager to get information about RaftIndex and RaftTerm
  const static int kMaxFragmentNumber = config::kHRaftEncodingK + config::kHRaftEncodingM;
  LogManager *lm_;
  LogManager *logs_[kMaxFragmentNumber];

  Storage *storage_;

  // For FlexibleK and CRaft: We need to detect the number of live servers
  LivenessMonitor live_monitor_;
  Encoder encoder_;
  SequenceGenerator seq_gen_;
  // For each index, there is an associated stripe that contains the encoded data
  std::map<raft_index_t, Stripe *> encoded_stripe_;

  // For each index, last_replicate contains the recent replicate version
  std::unordered_map<raft_index_t, Version> last_replicate_;

  // A place for storing fragments come from RequestFragments
  PreLeaderStripeStore preleader_stripe_store_;

 public:
  std::map<raft_node_id_t, RaftPeer *> peers_;
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients_;

  util::Timer election_timer_;   // Record elapse time during election
  util::Timer heartbeat_timer_;  // Record elapse time since last heartbeat
  util::Timer preleader_timer_;  // Record fragments collection time
  util::Timer replicate_timer_;  // Record replication timer

  // Election time should be between [min, max), set by configuration
  int64_t electionTimeLimitMin_, electionTimeLimitMax_;
  // A randomized election timeout based on above interval
  int64_t election_time_out_;
  int64_t heartbeatTimeInterval;

  int hraft_k_ = config::kHRaftEncodingK;
  int hraft_m_ = config::kHRaftEncodingM;

  int retransfer_rpc_count = 0;
  int retransfer_rpc_require_count = 0;

  void SetReTransferRPCRequireCount(int num) { retransfer_rpc_require_count = num; }

  int GetReTransferRPCRequireCount() const { return retransfer_rpc_require_count; }

  void SetReTransferRPCCount(int num) { retransfer_rpc_count = num; }

  int GetReTransferRPCCount() const { return retransfer_rpc_count; }

 private:
  int vote_me_cnt_;
  Rsm *rsm_;
};
}  // namespace raft
