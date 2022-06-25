#pragma once
#include "config.h"
#include "kv_server.h"
#include "raft_type.h"
#include "rpc.h"
namespace kv {

// A KvServiceNode is basically an adapter that combines the KvServer and
// RPC server
class KvServiceNode {
 public:
  static KvServiceNode* NewKvServiceNode(const KvClusterConfig& config,
                                         raft::raft_node_id_t id);
  KvServiceNode() = default;
  ~KvServiceNode();

  KvServiceNode(const KvClusterConfig& config, raft::raft_node_id_t id);
  void InitServiceNodeState();
  void StartServiceNode();
  void StopServiceNode();

 private:
  KvClusterConfig config_;
  raft::raft_node_id_t id_;
  KvServer* kv_server_;
  rpc::KvServerRPCServer* rpc_server_;
};
}  // namespace kv
