#pragma once
#include <cstdio>
#include <memory>
#include <unordered_map>

#include "raft.h"
#include "rcf_rpc.h"
#include "rpc.h"
#include "rsm.h"
namespace raft {

// A raft node is the collection of raft state runtime, i.e. the raft node is responsible
// for maintaining the RaftState instance, creating RPC calls, persisting log entries,
// creating timer thread and so on.
class RaftNode {
 public:
  struct NodeConfig {
    raft_node_id_t node_id_me;
    std::unordered_map<raft_node_id_t, rpc::NetAddress> servers;
    // The storage file_name, not used for now
    std::string storage_filename;
    // TODO: Add state machine into this config
    Rsm* rsm;
  };

  // Constructor
  RaftNode(const NodeConfig& node_config);
  ~RaftNode();

  // Start running this raft node
  void Start();

  // Do all necessary initialization work before starting running this node server
  void Init();

  // Calling exit to stop running this raft node, and release all resources
  void Exit();
  // Check if current node has exited
  bool Exited() { return exit_.load(); }

  // NOTE: This method should only be used in test or debug mod
  RaftState* getRaftState() { return raft_state_; }
  Rsm* getRsm() { return rsm_; }

  void Propose(const CommandData& cmd) {
    raft_state_->Propose(cmd);
  }

 private:
  void startTickerThread();
  void startApplierThread();

 private:
  raft_node_id_t node_id_me_;
  std::unordered_map<raft_node_id_t, rpc::NetAddress> servers_;
  RaftState* raft_state_;
  // RPC related struct
  rpc::RpcServer* rcf_server_;
  std::unordered_map<raft_node_id_t, rpc::RpcClient*> rcf_clients_;
  // Inidicating if this server has exited, i.e. Stop running, this is important so that
  // the ticker thread and applier thread can also exit normally
  std::atomic<bool> exit_;
  Rsm *rsm_;
};
}  // namespace raft
