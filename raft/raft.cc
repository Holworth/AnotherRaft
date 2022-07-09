#include "raft.h"

#include <cassert>
#include <cstdlib>
#include <ctime>
#include <map>
#include <mutex>
#include <vector>

#include "log_entry.h"
#include "log_manager.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "storage.h"
#include "util.h"

namespace raft {

RaftState *RaftState::NewRaftState(const RaftConfig &config) {
  auto ret = new RaftState;
  ret->id_ = config.id;

  Storage::PersistRaftState state;
  // If the storage provides a valid persisted raft state, use this
  // state to initialize this raft state instance
  if (config.storage != nullptr &&
      (state = config.storage->PersistState(), state.valid)) {
    ret->SetCurrentTerm(state.persisted_term);
    ret->SetVoteFor(state.persisted_vote_for);
    LOG(util::kRaft, "S%d Read Persist Term%d VoteFor%d", ret->id_, ret->CurrentTerm(),
        ret->VoteFor());
  } else {
    ret->SetCurrentTerm(0);
    ret->SetVoteFor(kNotVoted);
    LOG(util::kRaft, "S%d Init with Term%d VoteFor%d", ret->id_, ret->CurrentTerm(),
        ret->VoteFor());
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

  LOG(util::kRaft, "S%d Log Recover from storage LI%d", ret->id_,
      ret->lm_->LastLogEntryIndex());

  ret->electionTimeLimitMin_ = config.electionTimeMin;
  ret->electionTimeLimitMax_ = config.electionTimeMax;
  ret->rsm_ = config.rsm;
  ret->heartbeatTimeInterval = config::kHeartbeatInterval;
  ret->storage_ = config.storage;  // might be nullptr

  ret->last_applied_ = 0;
  ret->commit_index_ = 0;

  ret->persistVoteFor();
  ret->persistCurrentTerm();

  // FlexibleK: Init liveness monitor state
  ret->live_monitor_.node_num = config.rpc_clients.size() + 1;
  ret->live_monitor_.me = ret->id_;

  return ret;
}

void RaftState::Init() {
  resetElectionTimer();
  live_monitor_.Init();
}

// RequestVote RPC call
void RaftState::Process(RequestVoteArgs *args, RequestVoteReply *reply) {
  assert(args != nullptr && reply != nullptr);

  live_monitor_.UpdateLiveness(args->candidate_id);

  std::scoped_lock<std::mutex> lck(mtx_);
  LOG(util::kRaft, "S%d RequestVote From S%d AT%d", id_, args->candidate_id, args->term);

  reply->reply_id = id_;

  // The request server has smaller term, just refuse this vote request
  // immediately and return my term to update its term
  if (args->term < CurrentTerm()) {
    LOG(util::kRaft, "S%d Refuse: Term is bigger(%d>%d)", id_, CurrentTerm(), args->term);
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
    LOG(util::kRaft, "S%d VoteFor S%d", id_, args->candidate_id);
    reply->vote_granted = true;
    SetVoteFor(args->candidate_id);

    // persist vote for since it has been changed
    persistVoteFor();
    resetElectionTimer();
    return;
  }

  LOG(util::kRaft, "S%d RefuseVote R1=%d R2=%d", id_, rule1, rule2);
  // Refuse vote for this server
  reply->vote_granted = false;
  return;
}

void RaftState::Process(AppendEntriesArgs *args, AppendEntriesReply *reply) {
  assert(args != nullptr && reply != nullptr);
  live_monitor_.UpdateLiveness(args->leader_id);

  std::scoped_lock<std::mutex> lck(mtx_);

  LOG(util::kRaft,
      "S%d Receive AppendEntries From S%d(EC) (T%d PI=%d PT=%d EntCnt=%d LCommit=%d)",
      id_, args->leader_id, args->term, args->prev_log_index, args->prev_log_term,
      args->entries.size(), args->leader_commit);

  reply->reply_id = id_;
  reply->prev_entry_index = args->prev_log_index;
  reply->version_cnt = 0;

  // Reply false immediately if arguments' term is smaller
  if (args->term < CurrentTerm()) {
    reply->success = false;
    reply->term = CurrentTerm();
    reply->expect_index = 0;
    LOG(util::kRaft, "S%d reply to S%d with T%d EI%d", id_, args->leader_id, reply->term,
        reply->expect_index);
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
    LOG(util::kRaft, "S%d reply with expect index=%d", id_, reply->expect_index);
    return;
  }

  // Step3: Check conflicts and add new entries
  assert(args->entry_cnt == args->entries.size());
  if (args->entry_cnt > 0) {
    checkConflictEntryAndAppendNew(args, reply);
  }
  reply->expect_index = args->prev_log_index + args->entry_cnt + 1;
  LOG(util::kRaft, "S%d reply with expect index=%d", id_, reply->expect_index);

  // Step4: Update commit index if necessary
  if (args->leader_commit > CommitIndex()) {
    auto old_commit_idx = CommitIndex();
    raft_index_t new_entry_idx = args->prev_log_index + args->entries.size();
    auto update_commit_idx = std::min(args->leader_commit, new_entry_idx);
    SetCommitIndex(std::min(update_commit_idx, lm_->LastLogEntryIndex()));

    LOG(util::kRaft, "S%d Update CommitIndex (%d->%d)", id_, old_commit_idx,
        CommitIndex());
  }

  // TODO: Notify applier thread to apply newly committed entries to state machine

  reply->term = CurrentTerm();
  reply->success = true;

  // Commit index might have been changed, try apply committed entries
  tryApplyLogEntries();

  return;
}

void RaftState::Process(AppendEntriesReply *reply) {
  assert(reply != nullptr);

  // Note: Liveness monitor must be processed without exclusive access
  live_monitor_.UpdateLiveness(reply->reply_id);

  std::scoped_lock<std::mutex> lck(mtx_);

  LOG(util::kRaft, "S%d RECV AE RESP From S%d (Accept%d Expect I%d Term %d)", id_,
      reply->reply_id, reply->success, reply->expect_index, reply->term);

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
    // auto update_nextIndex = reply->expect_index;
    // auto update_matchIndex = update_nextIndex - 1;
    //
    // if (node->NextIndex() < update_nextIndex) {
    //   node->SetNextIndex(update_nextIndex);
    //   LOG(util::kRaft, "S%d update peer S%d NI%d", id_, peer_id, node->NextIndex());
    // }
    //
    // if (node->MatchIndex() < update_matchIndex) {
    //   node->SetMatchIndex(update_matchIndex);
    //   LOG(util::kRaft, "S%d update peer S%d MI%d", id_, peer_id, node->MatchIndex());
    // }
    //

    // TODO: Modify logic here
    for (int i = 0; i < reply->version_cnt; ++i) {
      auto raft_index = reply->prev_entry_index + i + 1;
      auto reply_version = reply->versions[i];

      if (node->matchVersion.count(raft_index) == 0 ||
          node->matchVersion[raft_index].GetVersionNumber().compare(
              reply_version.GetVersionNumber()) < 0) {
        node->matchVersion[raft_index] = reply_version;
        // Debug:
        // -----------------------------------------------------------------
        LOG(util::kRaft, "S%d Update S%d MATCH VERSION: %s", id_, peer_id,
            reply_version.ToString().c_str());
        // -----------------------------------------------------------------
      }
    }
    tryUpdateCommitIndex();
  } else {
    // NOTE: Simply set NextIndex to be expect_index might be error since the message
    // comes from reply might not be meaningful message
    // Update nextIndex to be expect index
    // reply->expect_index = 0 means when receiving this AE args, the server has higher
    // term, thus this expect_index is of no means
    if (reply->expect_index != 0) {
      node->SetNextIndex(reply->expect_index);
      LOG(util::kRaft, "S%d Update S%d NI%d", id_, peer_id, node->NextIndex());
    }
  }

  // TODO: May require applier to apply this log entry
  tryApplyLogEntries();
  return;
}

void RaftState::Process(RequestVoteReply *reply) {
  assert(reply != nullptr);

  live_monitor_.UpdateLiveness(reply->reply_id);

  std::scoped_lock<std::mutex> lck(mtx_);

  LOG(util::kRaft, "S%d HandleVoteResp from S%d term=%d grant=%d", id_, reply->reply_id,
      reply->term, reply->vote_granted);

  // Current raft peer is no longer candidate, or the term is expired
  if (Role() != kCandidate || reply->term < CurrentTerm()) {
    return;
  }

  // Receive higher raft term, convert to be follower
  if (reply->term > CurrentTerm()) {
    convertToFollower(reply->term);
    return;
  }

  if (reply->vote_granted == true) {
    incrementVoteMeCnt();
    LOG(util::kRaft, "S%d voteMeCnt=%d", id_, vote_me_cnt_);
    // Win votes of the majority of the cluster
    if (vote_me_cnt_ >= livenessLevel() + 1) {
      convertToPreLeader();
    }
  }
  return;
}

void RaftState::Process(RequestFragmentsArgs *args, RequestFragmentsReply *reply) {
  assert(args != nullptr && reply != nullptr);

  live_monitor_.UpdateLiveness(args->leader_id);

  std::scoped_lock<std::mutex> lck(mtx_);

  LOG(util::kRaft, "S%d Receive RequestFragments From S%d(EC) (T%d SI=%d EI=%d)", id_,
      args->leader_id, args->term, args->start_index, args->last_index);

  reply->reply_id = id_;
  reply->start_index = args->start_index;

  if (args->term < CurrentTerm()) {
    reply->term = CurrentTerm();
    reply->entry_cnt = 0;
    reply->fragments.clear();
    reply->success = false;

    LOG(util::kRaft, "S%d refuse RequestFragments: Higher Term(%d>%d)", id_,
        CurrentTerm(), args->term);
    return;
  }

  if (args->term > CurrentTerm() || Role() == kCandidate) {
    convertToFollower(args->term);
  }
  resetElectionTimer();

  raft_index_t raft_index = args->start_index;
  for (; raft_index <= args->last_index; ++raft_index) {
    if (auto ptr = lm_->GetSingleLogEntry(raft_index); ptr) {
      reply->fragments.push_back(*ptr);
    } else {
      break;
    }
  }
  LOG(util::kRaft, "S%d Submit fragments(I%d->I%d)", id_, args->start_index,
      raft_index - 1);

  reply->term = CurrentTerm();
  reply->success = true;
  reply->entry_cnt = reply->fragments.size();
  return;
}

void RaftState::Process(RequestFragmentsReply *reply) {
  assert(reply != nullptr);

  live_monitor_.UpdateLiveness(reply->reply_id);

  std::scoped_lock<std::mutex> lck(mtx_);

  LOG(util::kRaft, "S%d receive RequestFragmentsReply From S%d", id_, reply->reply_id);

  if (Role() != kPreLeader || reply->term < CurrentTerm()) {
    return;
  }

  if (reply->term > CurrentTerm()) {
    convertToFollower(reply->term);
    return;
  }

  LOG(util::kRaft, "S%d ReqFrag Resp (Cnt=%d)", id_, reply->entry_cnt);

  if (preleader_stripe_store_.IsCollected(reply->reply_id)) {
    LOG(util::kRaft, "S%d ommit RequestFragmentsReply from S%d", id_, reply->reply_id);
    return;
  }

  // TODO: RequestFragments may occur multiple times
  // TODO: Store collected fragments into some place and decode them to get the
  // complete entry
  //
  int check_idx = 0;
  for (const auto &entry : reply->fragments) {
    assert(check_idx + reply->start_index == entry.Index());

    // Debug:
    // --------------------------------------------------------------------
    LOG(util::kRaft, "S%d add frag at I%d info:%s", id_, entry.Index(),
        entry.ToString().c_str());
    // --------------------------------------------------------------------
    preleader_stripe_store_.AddFragments(entry.Index(), entry);
    check_idx++;
  }

  preleader_stripe_store_.UpdateResponseState(reply->reply_id);
  PreLeaderBecomeLeader();
  return;
}

ProposeResult RaftState::Propose(const CommandData &command) {
  std::scoped_lock<std::mutex> lck(mtx_);

  if (Role() != kLeader) {
    return ProposeResult{0, 0, false};
  }

  raft_index_t next_entry_index = lm_->LastLogEntryIndex() + 1;
  LogEntry entry;
  entry.SetType(kNormal);
  entry.SetCommandData(command.command_data);
  entry.SetIndex(next_entry_index);
  entry.SetTerm(CurrentTerm());
  entry.SetStartOffset(command.start_fragment_offset);

  lm_->AppendLogEntry(entry);

  LOG(util::kRaft, "S%d Propose at (I%d T%d) (ptr=%p)", id_, next_entry_index,
      CurrentTerm(), entry.CommandData().data());

  // Persist this new entry: maybe it can be ignored?
  if (storage_ != nullptr) {
    std::vector<LogEntry> persist_ent{entry};
    auto lo = lm_->LastLogEntryIndex();
    LOG(util::kRaft, "S%d persist entry(I%d->I%d)", id_, lo, lo);
    storage_->PersistEntries(lo, lo, persist_ent);
    storage_->SetLastIndex(lo);
    LOG(util::kRaft, "S%d persist entry(I%d->I%d) finished", id_, lo, lo);
  }

  int val = *reinterpret_cast<int *>(entry.CommandData().data());

  // Replicate this entry out
  // for (auto &[id, _] : peers_) {
  //   if (id != id_) {
  //     sendAppendEntries(id);
  //   }
  // }
  replicateEntries();
  return ProposeResult{next_entry_index, CurrentTerm(), true};
}

bool RaftState::isLogUpToDate(raft_index_t raft_index, raft_term_t raft_term) {
  LOG(util::kRaft, "S%d CheckLog (LastTerm=%d ArgTerm=%d) (LastIndex=%d ArgIndex=%d)",
      id_, lm_->LastLogEntryTerm(), raft_term, lm_->LastLogEntryIndex(), raft_index);

  if (raft_term > lm_->LastLogEntryTerm()) {
    return true;
  }
  if (raft_term == lm_->LastLogEntryTerm() && raft_index >= lm_->LastLogEntryIndex()) {
    return true;
  }
  return false;
}

void RaftState::checkConflictEntryAndAppendNew(AppendEntriesArgs *args,
                                               AppendEntriesReply *reply) {
  assert(args->entry_cnt == args->entries.size());
  auto old_idx = lm_->LastLogEntryIndex();
  auto array_index = 0;

  raft_index_t conflict_index = 0;
  for (; array_index < args->entries.size(); ++array_index) {
    auto raft_index = array_index + args->prev_log_index + 1;
    if (raft_index > lm_->LastLogEntryIndex()) {
      break;
    }
    if (args->entries[array_index].Term() != lm_->TermAt(raft_index)) {
      // Debug --------------------------------------------
      auto old_last_index = lm_->LastLogEntryIndex();
      lm_->DeleteLogEntriesFrom(raft_index);
      LOG(util::kRaft, "S%d Del Entry (%d->%d)", id_, old_last_index,
          lm_->LastLogEntryIndex());
      conflict_index = raft_index;
      break;
    }

    // Check if we need to overwrite this entry
    auto ent = lm_->GetSingleLogEntry(raft_index);
    if (NeedOverwriteLogEntry(ent->GetVersion(),
                              args->entries[array_index].GetVersion())) {
      lm_->OverWriteLogEntry(args->entries[array_index], raft_index);
    } else {
      // Just simply overwrite version number
      ent->SetVersion(args->entries[array_index].GetVersion());
    }
    ent = lm_->GetSingleLogEntry(raft_index);
    reply->versions.push_back(ent->GetVersion());

    LOG(util::kRaft, "S%d REPLY (I%d T%d V(%s))", id_, raft_index, ent->Term(),
        ent->GetVersion().ToString().c_str());

    // if (ent->Sequence() < args->entries[array_index].Sequence()) {
    //   auto new_ent = args->entries[array_index];
    //   lm_->OverWriteLogEntry(args->entries[array_index], raft_index);
    //
    //   auto reply_version = Version{raft_index, new_ent.Sequence(), new_ent.GetK(),
    //                                new_ent.GetN() - new_ent.GetK()};
    //
    //   // Debug:
    //   // --------------------------------------------------------------------------
    //   LOG(util::kRaft, "S%d overwrite I%d Version(%s)", id_, raft_index,
    //       reply_version.toString().c_str());
    //   // --------------------------------------------------------------------------
    //   reply->versions.push_back(reply_version);
    // } else {
    //   auto reply_version =
    //       Version{raft_index, ent->Sequence(), ent->GetK(), ent->GetN() - ent->GetK()};
    //   // Debug:
    //   // --------------------------------------------------------------------------
    //   LOG(util::kRaft, "S%d maintain I%d Version(%s)", id_, raft_index,
    //       reply_version.toString().c_str());
    //   // --------------------------------------------------------------------------
    //   reply->versions.push_back(reply_version);
    // }
  }
  // For those new entries
  auto old_last_index = lm_->LastLogEntryIndex();
  for (auto i = array_index; i < args->entries.size(); ++i) {
    auto raft_index = args->prev_log_index + i + 1;
    // Debug -------------------------------------------
    lm_->AppendLogEntry(args->entries[i]);

    auto reply_version = args->entries[i].GetVersion();
    LOG(util::kRaft, "S%d APPEND I%d V(%s)", id_, raft_index,
        reply_version.ToString().c_str());
    reply->versions.push_back(reply_version);
  }

  LOG(util::kRaft, "S%d APPEND(%d->%d) ENTCNT=%d", id_, old_last_index,
      lm_->LastLogEntryIndex(), args->entries.size());

  reply->version_cnt = reply->versions.size();

  // Persist newly added log entries, or persist the changes to deleted log entries
  if (storage_ != nullptr) {
    std::vector<LogEntry> persist_entries;
    raft_index_t lo =
        (conflict_index == 0) ? (array_index + args->prev_log_index + 1) : conflict_index;
    if (lo <= lm_->LastLogEntryIndex()) {
      lm_->GetLogEntriesFrom(lo, &persist_entries);
      LOG(util::kRaft, "S%d Persist Entries (I%d->I%d)", id_, lo,
          lm_->LastLogEntryIndex());
      storage_->PersistEntries(lo, lm_->LastLogEntryIndex(), persist_entries);
      storage_->SetLastIndex(lm_->LastLogEntryIndex());
      LOG(util::kRaft, "S%d Persist Entries (I%d->I%d) Finished", id_, lo,
          lm_->LastLogEntryIndex());
    }
  }
}

void RaftState::tryUpdateCommitIndex() {
  for (auto N = CommitIndex() + 1; N <= lm_->LastLogEntryIndex(); ++N) {
    bool replicate_frag[10] = {false};

    // The entry at index N has not been replicated yet, which means index >=
    // N has not been replicated neither, directly return
    if (encoded_stripe_.count(N) == 0) {
      return;
    }
    auto request_version = last_replicate_[N];

    // Get number of agreement
    //
    int agree_cnt = 1;
    for (auto &[id, node] : peers_) {
      // If the node's last response version matches the last requested
      // version by leader
      if (node->matchVersion.count(N) == 0) {
        continue;
      }

      // Debug:
      // ------------------------------------------------------------------------------
      LOG(util::kRaft, "S%d COMMIT VERSION: %s S%d REPLY VERSION: %s", id_,
          request_version.ToString().c_str(), id,
          node->matchVersion[N].ToString().c_str());
      // ------------------------------------------------------------------------------

      // TODO: Is it ok to only check the version number?
      // if (node->matchVersion[N].sequence == request_version.sequence) {
      //   agree_cnt++;
      // }
      if (node->matchVersion[N].GetVersionNumber().compare(
              request_version.GetVersionNumber()) >= 0) {
        // Two different servers may replicate the same fragment, we must decide the
        // number of different fragments that have been replicated
        auto frag_id = replicate_frag[node->matchVersion[N].GetFragmentId()];
        if (replicate_frag[frag_id] == false) {
          agree_cnt++;
          LOG(util::kRaft, "S%d Add AgreeCnt: %d", agree_cnt);
          replicate_frag[frag_id] = true;
        }
      }
    }

    // Debug:
    // ---------------------------------------------------------------------------------
    LOG(util::kRaft, "S%d COMMIT REQUIRE %d, GET %d", id_,
        request_version.k + livenessLevel(), agree_cnt);
    // ---------------------------------------------------------------------------------

    if (agree_cnt >= request_version.k + livenessLevel() &&
        lm_->GetSingleLogEntry(N)->Term() == CurrentTerm()) {
      SetCommitIndex(N);
      // Index N is committed, no need to track them any more
      removeLastReplicateVersionAt(N);
      removeTrackVersionOfAll(N);
    }
  }
}

// TODO: Use a specific thread to commit applied entries to application
// Do not call this function in RPC, which may results in blocked RPC
void RaftState::tryApplyLogEntries() {
  while (last_applied_ < commit_index_) {
    auto old_apply_idx = last_applied_;

    // apply this message on state machine:
    if (rsm_ != nullptr) {
      // In asynchronize applying scheme, the applier thread may find that one
      // entry has been released due to the main thread adding more commands.
      LogEntry ent;
      auto stat = lm_->GetEntryObject(last_applied_ + 1, &ent);
      assert(stat == kOk);

      // if (ent.Index() == 3) {
      //   auto val = *reinterpret_cast<int*>(ent.CommandData().data());
      //   LOG(util::kRaft, "S%d in APPLY detect value=%d", id_, val);
      // }

      rsm_->ApplyLogEntry(ent);
      LOG(util::kRaft, "S%d Push ent(I%d T%d) to channel", id_, ent.Index(), ent.Term());
    }
    last_applied_ += 1;
    LOG(util::kRaft, "S%d APPLY(%d->%d)", id_, old_apply_idx, last_applied_);
  }
}

void RaftState::convertToFollower(raft_term_t term) {
  // This assertion ensures that the server will only convert to follower with higher
  // term. i.e. The term attribute in followre is monotonically increasing
  assert(term >= CurrentTerm());
  LOG(util::kRaft, "S%d ToFollower(T%d->T%d)", id_, CurrentTerm(), term);

  SetRole(kFollower);
  if (term > CurrentTerm()) {
    SetVoteFor(kNotVoted);
    SetCurrentTerm(term);
    persistCurrentTerm();
    persistVoteFor();
  }
}

void RaftState::convertToCandidate() {
  LOG(util::kRaft, "S%d ToCandi(T%d)", id_, CurrentTerm());
  SetRole(kCandidate);
  resetElectionTimer();
  startElection();
}

void RaftState::convertToLeader() {
  LOG(util::kRaft, "S%d ToLeader(T%d) LI%d", id_, CurrentTerm(),
      lm_->LastLogEntryIndex());
  SetRole(kLeader);
  resetNextIndexAndMatchIndex();

  // reset liveness monitor status
  // initLivenessMonitorState();

  broadcastHeartbeat();
  resetHeartbeatTimer();
}

void RaftState::convertToPreLeader() {
  LOG(util::kRaft, "S%d ToPreLeader(T%d) COMMIT I%d LI%d", id_, CurrentTerm(),
      CommitIndex(), lm_->LastLogEntryIndex());
  SetRole(kPreLeader);

  // If there is no entry need to be collected, become leader immediately
  if (CommitIndex() == lm_->LastLogEntryIndex()) {
    convertToLeader();
    return;
  }
  collectFragments();
}

void RaftState::resetNextIndexAndMatchIndex() {
  auto next_index = lm_->LastLogEntryIndex() + 1;
  LOG(util::kRaft, "S%d set NI=%d MI=%d", id_, next_index, 0);
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
  if (storage_ != nullptr) {
    storage_->PersistState(Storage::PersistRaftState{true, CurrentTerm(), VoteFor()});
  }
}
void RaftState::persistCurrentTerm() {
  // TODO: The persistVoteFor function is basically a wrapper for calling storage to
  // persist the currentTerm attribute. This function should be filled when we have a
  // full-functionality persister implementation
  if (storage_ != nullptr) {
    storage_->PersistState(Storage::PersistRaftState{true, CurrentTerm(), VoteFor()});
  }
}

// [REQUIRE] Current thread holds the lock of raft state
void RaftState::startElection() {
  assert(Role() == kCandidate);

  LOG(util::kRaft, "S%d Start Election (T%d) LI%d LT%d", id_, CurrentTerm() + 1,
      lm_->LastLogEntryIndex(), lm_->LastLogEntryTerm());

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

    LOG(util::kRaft, "S%d RequestVote to S%d", id_, _);
    rpc_client->sendMessage(args);
  }
}

void RaftState::broadcastHeartbeat() {
  // for (auto &[id, peer] : peers_) {
  //   if (id != id_) {
  //     sendHeartBeat(id);
  //   }
  // }
  replicateEntries();
}

void RaftState::collectFragments() {
  // // Initiate a fragments collection task
  // LOG(util::kRaft, "S%d Collect Fragments(I%d->I%d)", id_, CommitIndex() + 1,
  //     lm_->LastLogEntryIndex());
  //
  // // Initiate a request fragments task
  // preleader_stripe_store_.InitRequestFragmentsTask(
  //     CommitIndex() + 1, lm_->LastLogEntryIndex(), peers_.size() + 1, id_);
  // preleader_timer_.Reset();
  //
  // for (int i = 0; i < preleader_stripe_store_.stripes.size(); ++i) {
  //   raft_index_t r_idx = i + preleader_stripe_store_.start_index;
  //   auto ent = lm_->GetSingleLogEntry(r_idx);
  //   assert(ent != nullptr);
  //
  //   // NOTE: The stripe meta data is set by current leader, however, that might
  //   // be invalid since a collected stripe may contain multiple kind of entries
  //   // i.e. with different k and m parameters
  //   Stripe &stripe = preleader_stripe_store_.stripes[i];
  //   stripe.SetIndex(ent->Index());
  //   stripe.SetTerm(ent->Term());
  //   /*
  //   // K + M
  //   // TODO: Set k after filtering fragments, use the smaller k
  //   stripe.SetFragmentCnt(ent->FragmentsCnt());
  //   // K
  //   stripe.SetFragRecoverCnt(ent->FragmentRequireCnt());
  //   stripe.SetFragLength(ent->FragmentLength());
  //   */
  //
  //   // Add current server's saved fragments(It might be a complete log entry as well)
  //   stripe.AddFragments(
  //       *(lm_->GetSingleLogEntry(preleader_stripe_store_.start_index + i)));
  //   LOG(util::kRaft, "S%d Add Its Fragments into Stripe I%d", id_, ent->Index());
  // }
  //
  // RequestFragmentsArgs args;
  // args.term = CurrentTerm();
  // args.leader_id = id_;
  // args.start_index = CommitIndex() + 1;
  // args.last_index = lm_->LastLogEntryIndex();
  //
  // for (auto &[id, rpc] : rpc_clients_) {
  //   if (id == id_) {
  //     continue;
  //   }
  //   rpc->sendMessage(args);
  // }
}

void RaftState::resetElectionTimer() {
  srand(id_);
  auto id_rand = rand();
  srand(time(nullptr) * id_ * id_);  // So that we have "true" random number
  if (electionTimeLimitMin_ == electionTimeLimitMax_) {
    election_time_out_ = electionTimeLimitMin_;
  } else {
    election_time_out_ =
        rand() % (electionTimeLimitMax_ - electionTimeLimitMin_) + electionTimeLimitMin_;
  }
  election_timer_.Reset();
}

void RaftState::resetHeartbeatTimer() { heartbeat_timer_.Reset(); }
void RaftState::resetPreLeaderTimer() { preleader_timer_.Reset(); }

void RaftState::Tick() {
  std::scoped_lock<std::mutex> lck(mtx_);
  switch (Role()) {
    case kFollower:
      tickOnFollower();
      return;
    case kCandidate:
      tickOnCandidate();
      return;
    case kPreLeader:
      tickOnPreLeader();
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
  broadcastHeartbeat();
  resetHeartbeatTimer();
}

void RaftState::tickOnPreLeader() {
  if (preleader_timer_.ElapseMilliseconds() < config::kCollectFragmentsInterval) {
    return;
  }
  collectFragments();
  resetPreLeaderTimer();
}

void RaftState::EncodingRaftEntry(raft_index_t raft_index, int k, int m,
                                  VersionNumber version_num, Stripe *stripe) {
  assert(raft_index <= lm_->LastLogEntryIndex());
  auto ent = lm_->GetSingleLogEntry(raft_index);
  assert(ent != nullptr);

  stripe->raft_index = ent->Index();
  stripe->raft_term = ent->Term();
  stripe->fragments.clear();

  LOG(util::kRaft, "S%d Encode I%d T%d K%d M%d", id_, raft_index, stripe->raft_term, k,
      m);

  Encoder::EncodingResults results;
  auto data_to_encode = ent->CommandData().data() + ent->StartOffset();
  auto datasize_to_encode = ent->CommandData().size() - ent->StartOffset();
  Slice encode_slice = Slice(data_to_encode, datasize_to_encode);
  encoder_.EncodeSlice(encode_slice, k, m, &results);

  for (const auto &[frag_id, frag] : results) {
    LogEntry encoded_ent;
    encoded_ent.SetIndex(raft_index);
    encoded_ent.SetTerm(stripe->raft_term);
    encoded_ent.SetType(kFragments);
    encoded_ent.SetNotEncodedSlice(Slice(ent->CommandData().data(), ent->StartOffset()));
    encoded_ent.SetFragmentSlice(frag);
    encoded_ent.SetVersion(Version{version_num, k, m, frag_id});
    stripe->fragments[frag_id] = encoded_ent;
  }
}

// TODO: Update logic
void RaftState::replicateEntries() {
  LOG(util::kRaft, "S%d REPLICATE ENTRIES", id_);
  auto live_servers = live_monitor_.LiveNumber();

  int encode_k = live_servers - livenessLevel();
  int encode_m = livenessLevel();
  auto version_num = VersionNumber{CurrentTerm(), NextSequence()};

  LOG(util::kRaft, "S%d Estimate %d Server Alive K:%d M:%d VERSION NUM:%s", id_,
      live_servers, encode_k, encode_m, version_num.ToString().c_str());

  // Step1: Encoding all necessary entries from CommitIndex() to LastIndex()
  auto last_index = lm_->LastLogEntryIndex();
  for (auto raft_index = CommitIndex() + 1; raft_index <= last_index; ++raft_index) {
    bool need_encoding = false;
    if (encoded_stripe_.count(raft_index) == 0) {
      need_encoding = true;
    } else {
      auto encoded_version = encoded_stripe_.at(raft_index)->version;
      if (encoded_version.GetK() != encode_k || encoded_version.GetM() != encode_m) {
        need_encoding = true;
      }
    }
    if (need_encoding) {
      auto stripe = new Stripe();
      EncodingRaftEntry(raft_index, encode_k, encode_m, version_num, stripe);
      encoded_stripe_.insert_or_assign(raft_index, stripe);
    } else {
      // Simply update the encoding version
      auto stripe = encoded_stripe_[raft_index];
      assert(stripe->version.GetK() == encode_k);
      assert(stripe->version.GetM() == encode_m);
    }

    auto new_version = Version{version_num, encode_k, encode_m};
    auto stripe = encoded_stripe_[raft_index];
    stripe->version = new_version;

    // Update Commit Requirements
    last_replicate_.insert_or_assign(raft_index, new_version);
  }

  // Step2: construct a map to decide which fragment will each follower receive
  std::map<raft_node_id_t, raft_frag_id_t> frag_map;
  int start_frag_id = 0;
  for (const auto &[id, _] : peers_) {
    if (live_monitor_.IsAlive(id)) {
      frag_map[id] = start_frag_id++;
    }
  }

  // Step3: For each follower, send out the messages
  for (const auto &[id, _] : peers_) {
    if (id == id_) {
      continue;
    }
    if (!live_monitor_.IsAlive(id)) {
      LOG(util::kRaft, "S%d detect S%d is not alive, send heartbeat", id_, id);
      // TODO: Should we send heartbeat messages here? so that we can detect a restart
      // server?
      continue;
    }

    // Construct an AE args
    AppendEntriesArgs args;
    args.term = CurrentTerm();
    args.leader_id = id_;
    args.leader_commit = CommitIndex();
    args.prev_log_index = CommitIndex();
    args.prev_log_term = lm_->TermAt(args.prev_log_index);

    // TODO: Should we send all entries [CommitIndex, LastIndex] to follower? what
    // about eliminating some entries by using NextIndex?
    auto send_index = args.prev_log_index + 1;
    for (; send_index <= lm_->LastLogEntryIndex(); ++send_index) {
      assert(encoded_stripe_.count(send_index) != 0);
      auto fragment_id = frag_map[id];
      auto fragment = encoded_stripe_[send_index]->fragments[fragment_id];
      assert(fragment_id == fragment.GetVersion().fragment_id);
      args.entries.push_back(fragment);
      LOG(util::kRaft, "S%d Send (I%d T%d FragId%d) To S%d", id_, send_index,
          fragment.Term(), fragment_id, id);
    }
    args.entry_cnt = args.entries.size();
    rpc_clients_[id]->sendMessage(args);
  }
}

void RaftState::sendHeartBeat(raft_node_id_t peer) {
  auto next_index = peers_[peer]->NextIndex();
  auto prev_index = next_index - 1;
  auto prev_term = lm_->TermAt(prev_index);

  LOG(util::kRaft, "S%d send heartbeat to S%d(I%d->I%d)", id_, peer, next_index,
      next_index);
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
  LOG(util::kRaft, "S%d require entry cnt=%d, get entry cnt=%d", id_, require_entry_cnt,
      args.entries.size());
  LOG(util::kRaft, "S%d AE To S%d (I%d->I%d) at T%d", id_, peer, next_index,
      lm_->LastLogEntryIndex(), CurrentTerm());

  // if (lm_->LastLogEntryIndex() >= 3) {
  //   auto val = *reinterpret_cast<int
  //   *>(lm_->GetSingleLogEntry(3)->CommandData().data()); LOG(util::kRaft, "S%d detect
  //   value=%d ptr=%p size=%d", id_, val,
  //       lm_->GetSingleLogEntry(3)->CommandData().data(),
  //       lm_->GetSingleLogEntry(3)->CommandData().size());
  // }

  assert(require_entry_cnt == args.entries.size());
  args.entry_cnt = args.entries.size();

  rpc_clients_[peer]->sendMessage(args);
}

bool RaftState::containEntry(raft_index_t raft_index, raft_term_t raft_term) {
  LOG(util::kRaft, "S%d ContainEntry? (LT%d AT%d) (LI%d AI%d)", id_,
      lm_->LastLogEntryTerm(), raft_term, lm_->LastLogEntryIndex(), raft_index);

  if (raft_index == lm_->LastSnapshotIndex()) {
    return raft_term == lm_->LastSnapshotTerm();
  }
  const LogEntry *entry = lm_->GetSingleLogEntry(raft_index);
  if (entry == nullptr || entry->Term() != raft_term) {
    return false;
  }
  return true;
}

void RaftState::PreLeaderBecomeLeader() {
  LOG(util::kRaft, "S%d PreLeaderStore Response: %d", id_,
      preleader_stripe_store_.CollectedFragmentsCnt());
  if (preleader_stripe_store_.CollectedFragmentsCnt() >= livenessLevel() + 1) {
    LOG(util::kRaft, "S%d Rebuild fragments", id_);
    EncodeCollectedStripe();
    convertToLeader();
  }
}

// TODO: Update logic
void RaftState::EncodeCollectedStripe() {
  // Debug:
  // ------------------------------------------------------------------
  LOG(util::kRaft, "S%d decode collected stripes", id_);
  // ------------------------------------------------------------------
  // for (int i = 0; i < preleader_stripe_store_.stripes.size(); ++i) {
  //   auto &stripe = preleader_stripe_store_.stripes[i];
  //
  //   stripe.Filter();
  //
  //   // Debug:
  //   // ------------------------------------------------------------------
  //   LOG(util::kRaft, "S%d decode stripe I%d", id_, stripe.GetIndex());
  //   // ------------------------------------------------------------------
  //
  //   LogEntry entry;
  //   auto succ = encoder_.DecodeEntry(&stripe, &entry);
  //   auto r_idx = i + preleader_stripe_store_.start_index;
  //   if (succ) {
  //     lm_->OverWriteLogEntry(entry, r_idx);
  //   } else {
  //     // Failed to decode a full entry, delete all preceding log entries
  //     lm_->DeleteLogEntriesFrom(r_idx);
  //     return;
  //   }
  // }
}

bool RaftState::NeedOverwriteLogEntry(const Version &old_version,
                                      const Version &new_version) {
  return old_version.GetK() != new_version.GetK() ||
         old_version.GetM() != new_version.GetM() ||
         old_version.GetFragmentId() != new_version.GetFragmentId();
}

}  // namespace raft
