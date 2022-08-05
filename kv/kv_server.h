#pragma once
#include <mutex>
#include <thread>
#include <unordered_map>

#include "channel.h"
#include "concurrent_queue.h"
#include "kv_format.h"
#include "raft.h"
#include "raft_node.h"
#include "raft_type.h"
#include "storage_engine.h"
#include "type.h"
namespace kv {

struct KvServerConfig {
  raft::RaftNode::NodeConfig raft_node_config;
  std::string storage_engine_name;
};

class KvServer {
 public:
  struct KvRequestApplyResult {
    raft::raft_term_t raft_term;
    ErrorType err;
    std::string value;
    uint64_t apply_time;
  };

 public:
  KvServer() = default;
  ~KvServer() {
    delete channel_;
    delete db_;
    delete raft_;
  }

 public:
  static KvServer* NewKvServer(const KvServerConfig& kv_server_config);

  raft::raft_index_t LastApplyIndex() const { return applied_index_; }

 public:
  void DealWithRequest(const Request* request, Response* resp);
  // Disable this server

  // Start running this kv server
  void Start();

  // Do necessary initialize work, for example, apply existed Snapshot to
  // storage engine(Not implemented yet), currently just initialize raft
  // state
  void Init() { raft_->Init(); }

  auto Id() const { return id_; }

  bool IsLeader() const { return raft_->IsLeader(); }

 private:
  // Check if a log entry has been committed yet
  bool CheckEntryCommitted(const raft::ProposeResult& pr, KvRequestApplyResult* apply);

 public:
  // A thread that periodically apply committed raft log entries to KVRsm
  static void ApplyRequestCommandThread(KvServer* server);

  void startApplyKvRequestCommandsThread() {
    std::thread t(ApplyRequestCommandThread, this);
    t.detach();
  }

  void ExecuteGetOperation(const Request* request, Response* resp);

 public:
  // For test and debug
  void Exit() {
    exit_.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    raft_->Exit();
    db_->Close();
  };

  bool Exited() const { return exit_.load(); }

  void Disconnect() { raft_->Disconnect(); }

  bool IsDisconnected() const { return raft_->IsDisconnected(); }

  void Reconnect() { raft_->Reconnect(); }

  StorageEngine* DB() { return db_; }

 private:
  raft::RaftNode* raft_;
  Channel* channel_;  // channel is used to interact with lower level raft library
  StorageEngine* db_;

  // We need a channel that receives apply messages from raft
  // We need a concurrent map to record which entries have been applied, associated with
  // their term and value (if it is a Get operation)
  std::unordered_map<raft::raft_index_t, KvRequestApplyResult> applied_cmds_;
  std::mutex map_mutex_;

  // Check if this server has exited
  std::atomic<bool> exit_;

  raft::raft_node_id_t id_;

  raft::raft_index_t applied_index_;
};
}  // namespace kv
