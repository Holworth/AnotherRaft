#include "raft.h"

#include <cassert>
#include <cstdlib>
#include <mutex>
#include <vector>

#include "log_manager.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "storage.h"
#include "util.h"

namespace raft {

RaftState *RaftState::NewRaftState(const RaftConfig &config) {
  auto ret = new RaftState;
  ret->id_ = config.id;

  Persister::PersistRaftState state;
  // If the storage provides a valid persisted raft state, use this
  // state to initialize this raft state instance
  if (config.storage != nullptr &&
      (state = config.storage->PersistState(), state.valid)) {
    ret->SetCurrentTerm(state.persisted_term);
    ret->SetVoteFor(state.persisted_vote_for);
  } else {
    ret->SetCurrentTerm(0);
    ret->SetVoteFor(kNotVoted);
  }
  // On every boot, the raft peer is set to be follower
  ret->SetRole(kFollower);

  for (const auto &[id, rpc] : config.rpc_clients) {
    auto peer = new RaftPeer();
    ret->peers_.insert({id, peer});
  }

  ret->rpc_clients_ = config.rpc_clients;

  // Construct log manager from persistence storage
  ret->lm_ = LogManager::NewLogManager(config.storage);

  ret->electionTimeLimitMin_ = config.electionTimeMin;
  ret->electionTimeLimitMax_ = config.electionTimeMax;

  return ret;
}

void RaftState::Init() { resetElectionTimer(); }

// RequestVote RPC call
void RaftState::Process(RequestVoteArgs *args, RequestVoteReply *reply) {
  assert(args != nullptr && reply != nullptr);
  std::scoped_lock<std::mutex> lck(mtx_);

  // The request server has smaller term, just refuse this vote request
  // immediately and return my term to update its term
  if (args->term < CurrentTerm()) {
    reply->term = CurrentTerm();
    reply->vote_granted = false;
    return;
  }

  // If this request carries a higher term, then convert my role to be
  // follower. And reset voteFor attribute for voting in this new term
  if (args->term > CurrentTerm()) {
    convertToFollower(args->term);
  }

  reply->term = CurrentTerm();

  // Check if vote for this requesting server. Rule1 checks if current server
  // has voted; Rule2 checks if requesting server's log is newer
  bool rule1 = (VoteFor() == kNotVoted || VoteFor() == args->candidate_id);
  bool rule2 = isLogUpToDate(args->last_log_index, args->last_log_term);

  // Vote for this requesting server
  if (rule1 && rule2) {
    reply->vote_granted = true;
    SetVoteFor(args->candidate_id);

    // persist vote for since it has been changed
    persistVoteFor();
    resetElectionTimer();
    return;
  }

  // Refuse vote for this server
  reply->vote_granted = false;
  return;
}

void RaftState::Process(AppendEntriesArgs *args, AppendEntriesReply *reply) {
  assert(args != nullptr && reply != nullptr);
  std::scoped_lock<std::mutex> lck(mtx_);

  reply->reply_id = id_;

  // Reply false immediately if arguments' term is smaller
  if (args->term < CurrentTerm()) {
    reply->success = false;
    reply->term = CurrentTerm();
    reply->expect_index = 0;
    return;
  }

  if (args->term > CurrentTerm() || Role() == kCandidate) {
    convertToFollower(args->term);
  }
  resetElectionTimer();

  // Step2: Check if current server contains a log entry at prev log index with
  // prev log term
  if (!containEntry(args->prev_log_index, args->prev_log_term)) {
    // Reply false immediately since current server lacks one log entry: notify
    // the leader to send older entries
    reply->success = false;
    reply->term = CurrentTerm();
    reply->expect_index = commit_index_ + 1;
    return;
  }

  // Step3: Check conflicts and add new entries
  checkConflictEntryAndAppendNew(args);
  reply->expect_index = args->prev_log_index + args->entry_cnt + 1;

  // Step4: Update commit index if necessary
  if (args->leader_commit > CommitIndex()) {
    auto old_commit_idx = CommitIndex();
    raft_index_t new_entry_idx = args->prev_log_index + args->entries.size();
    auto update_commit_idx = std::min(args->leader_commit, new_entry_idx);
    SetCommitIndex(std::min(update_commit_idx, lm_->LastLogEntryIndex()));
  }

  // TODO: Notify applier thread to apply newly committed entries to state machine

  reply->term = CurrentTerm();
  reply->success = true;

  return;
}

void RaftState::Process(AppendEntriesReply *reply) {
  assert(reply != nullptr);
  std::scoped_lock<std::mutex> lck(mtx_);

  // Check if this reply is expired
  if (Role() != kLeader || reply->term < CurrentTerm()) {
    return;
  }

  if (reply->term > CurrentTerm()) {
    convertToFollower(reply->term);
    return;
  }

  auto peer_id = reply->reply_id;
  auto node = peers_[peer_id];
  if (reply->success) {  // Requested entries are successfully replicated
    // Update nextIndex and matchIndex for this server
    auto update_nextIndex = reply->expect_index;
    auto update_matchIndex = update_nextIndex - 1;

    if (node->NextIndex() < update_nextIndex) {
      node->SetNextIndex(update_nextIndex);
    }

    if (node->MatchIndex() < update_matchIndex) {
      node->SetMatchIndex(update_matchIndex);
    }
    tryUpdateCommitIndex();
  } else {
    // Update nextIndex to be expect index
    node->SetNextIndex(reply->expect_index);
  }

  // TODO: May require applier to apply this log entry
  return;
}

// Hello
void RaftState::Process(RequestVoteReply *reply) {
  assert(reply != nullptr);
  std::scoped_lock<std::mutex> lck(mtx_);

  // Current raft peer is no longer candidate, or the term is expired
  if (Role() != kCandidate || reply->term < CurrentTerm()) {
    // LOG(util::kRaft, "Hello");
    return;
  }

  // Receive higher raft term, convert to be follower
  if (reply->term > CurrentTerm()) {
    convertToFollower(reply->term);
    return;
  }

  if (reply->vote_granted == true) {
    incrementVoteMeCnt();
    // Win votes of the majority of the cluster
    if (vote_me_cnt_ >= livenessLevel() + 1) {
      convertToLeader();
    }
  }
  return;
}

bool RaftState::isLogUpToDate(raft_index_t raft_index, raft_term_t raft_term) {
  if (raft_term > lm_->LastLogEntryTerm()) {
    return true;
  }
  if (raft_term == lm_->LastLogEntryTerm() && raft_index >= lm_->LastLogEntryIndex()) {
    return true;
  }
  return false;
}

void RaftState::checkConflictEntryAndAppendNew(AppendEntriesArgs *args) {
  assert(args->entry_cnt == args->entries.size());
  auto old_idx = lm_->LastLogEntryIndex();
  auto array_index = 0;
  for (; array_index < args->entries.size(); ++array_index) {
    auto raft_index = array_index + args->prev_log_index + 1;
    if (raft_index > lm_->LastLogEntryIndex()) {
      break;
    }
    if (args->entries[array_index].Term() != lm_->TermAt(raft_index)) {
      lm_->DeleteLogEntriesFrom(raft_index);
      break;
    }
  }

  for (auto i = array_index; i < args->entries.size(); ++i) {
    auto raft_index = args->prev_log_index + i + 1;
    lm_->AppendLogEntry(args->entries[i]);
  }
}

void RaftState::tryUpdateCommitIndex() {
  for (auto N = lm_->LastLogEntryIndex(); N > CommitIndex(); --N) {
    // For each log entry, traverse all peers to check agreement count
    int agree_cnt = 1;
    for (const auto &[id, peer] : peers_) {
      if (id != id_ && peer->MatchIndex() >= N) {
        agree_cnt++;
      }
    }

    int require_agree_cnt = livenessLevel() + 1;
    if (agree_cnt >= require_agree_cnt && lm_->TermAt(N) == CurrentTerm()) {
      SetCommitIndex(N);
      break;
    }
  }
}

void RaftState::convertToFollower(raft_term_t term) {
  assert(term >= CurrentTerm());

  SetRole(kFollower);
  if (term > CurrentTerm()) {
    SetVoteFor(kNotVoted);
  }
  SetCurrentTerm(term);

  persistCurrentTerm();
  persistVoteFor();
}

void RaftState::convertToCandidate() {
  SetRole(kCandidate);
  resetElectionTimer();
  startElection();
}

void RaftState::convertToLeader() {
  SetRole(kLeader);
  resetNextIndexAndMatchIndex();
  broadcastHeatbeat();
  resetHeartbeatTimer();
}

void RaftState::resetNextIndexAndMatchIndex() {
  auto next_index = lm_->LastLogEntryIndex() + 1;
  // Since there is no match index yet, the server simply set it to be 0
  for (auto &[id, peer] : peers_) {
    peer->SetNextIndex(next_index);
    peer->SetMatchIndex(0);
  }
}

void RaftState::persistVoteFor() {
  // TODO: The persistVoteFor function is basically a wrapper for calling storage to
  // persist the voteFor attribute. This function should be filled when we have a
  // full-functionality persister implementation
}
void RaftState::persistCurrentTerm() {
  // TODO: The persistVoteFor function is basically a wrapper for calling storage to
  // persist the currentTerm attribute. This function should be filled when we have a
  // full-functionality persister implementation
}

// [REQUIRE] Current thread holds the lock of raft state
void RaftState::startElection() {
  assert(Role() == kCandidate);

  // Update current status of raft
  current_term_++;
  vote_for_ = id_;
  vote_me_cnt_ = 1;

  assert(vote_me_cnt_ != kNotVoted);

  // TODO: Persist voteFor and persistCurrentTerm may be combined to one single function,
  // namely, persistRaftState?
  persistVoteFor();
  persistCurrentTerm();

  // Construct RequestVote args
  auto args = RequestVoteArgs{
      CurrentTerm(),
      id_,
      lm_->LastLogEntryIndex(),
      lm_->LastLogEntryTerm(),
  };

  // Send out the requests to any other raft peer
  for (auto &[_, rpc_client] : rpc_clients_) {
    if (_ == id_) {  // Omit self
      continue;
    }
    rpc_client->sendMessage(args);
  }
}

void RaftState::broadcastHeatbeat() {
  for (auto &[id, peer] : peers_) {
    if (id != id_) {
      sendHeartBeat(id);
    }
  }
}

void RaftState::resetElectionTimer() {
  srand(19201);
  if (electionTimeLimitMin_ == electionTimeLimitMax_) {
    election_time_out_ = electionTimeLimitMin_;
  } else {
    election_time_out_ =
        rand() % (electionTimeLimitMax_ - electionTimeLimitMin_) + electionTimeLimitMin_;
  }
  election_timer_.Reset();
}

void RaftState::resetHeartbeatTimer() { heartbeat_timer_.Reset(); }

void RaftState::Tick() {
  std::scoped_lock<std::mutex> lck(mtx_);
  switch (Role()) {
    case kFollower:
      tickOnFollower();
      return;
    case kCandidate:
      tickOnCandidate();
      return;
    case kLeader:
      tickOnLeader();
      return;
    default:
      assert(0);
  }
}

void RaftState::tickOnFollower() {
  if (election_timer_.ElapseMilliseconds() < election_time_out_) {
    return;
  }
  convertToCandidate();
}

void RaftState::tickOnCandidate() {
  if (election_timer_.ElapseMilliseconds() < election_time_out_) {
    return;
  }
  // Start another election
  resetElectionTimer();
  startElection();
}

void RaftState::tickOnLeader() {
  if (heartbeat_timer_.ElapseMilliseconds() < heartbeatTimeInterval) {
    return;
  }
  broadcastHeatbeat();
  resetHeartbeatTimer();
}

void RaftState::sendHeartBeat(raft_node_id_t peer) {
  auto next_index = peers_[peer]->NextIndex();
  auto prev_index = next_index - 1;
  auto prev_term = lm_->TermAt(prev_index);

  auto args = AppendEntriesArgs{CurrentTerm(), id_, prev_index, prev_term,
                                CommitIndex(), 0,   0,          std::vector<LogEntry>()};
  rpc_clients_[peer]->sendMessage(args);
}

void RaftState::sendAppendEntries(raft_node_id_t peer) {
  auto next_index = peers_[peer]->NextIndex();
  auto prev_index = next_index - 1;
  auto prev_term = lm_->TermAt(prev_index);

  auto args = AppendEntriesArgs{CurrentTerm(), id_, prev_index, prev_term, CommitIndex()};

  auto require_entry_cnt = lm_->LastLogEntryIndex() - prev_index;
  args.entries.reserve(require_entry_cnt);
  lm_->GetLogEntriesFrom(next_index, &args.entries);

  assert(require_entry_cnt == args.entries.size());
  args.entry_cnt = args.entries.size();

  rpc_clients_[peer]->sendMessage(args);
}

bool RaftState::containEntry(raft_index_t raft_index, raft_term_t raft_term) {
  if (raft_index == lm_->LastSnapshotIndex()) {
    return raft_term == lm_->LastSnapshotTerm();
  }
  const LogEntry *entry = lm_->GetSingleLogEntry(raft_index);
  if (entry == nullptr || entry->Term() != raft_term) {
    return false;
  }
  return true;
}

}  // namespace raft
