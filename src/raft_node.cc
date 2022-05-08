#include "raft_node.h"

#include <chrono>
#include <memory>
#include <thread>

#include "raft.h"
#include "rcf_rpc.h"
#include "util.h"
namespace raft {

RaftNode::RaftNode(const NodeConfig& node_config)
    : node_id_me_(node_config.node_id_me),
      servers_(node_config.servers),
      raft_state_(nullptr) {}

void RaftNode::Init() {
  // Create an RPC server that receives request from remote servers
  rcf_server_ = new rpc::RCFRpcServer(servers_[node_id_me_]);

  // Create an RPC client that is able to send RPC request to remote server
  for (const auto& [id, addr] : servers_) {
    if (id != node_id_me_) {
      rcf_clients_.insert({id, new rpc::RCFRpcClient(addr)});
    }
  }

  // Create Raft State instance
  RaftConfig config = RaftConfig{node_id_me_, rcf_clients_, nullptr, 150, 300};
  raft_state_ = RaftState::NewRaftState(config);

  // Set related state for all RPC related struct
  rcf_server_->setState(raft_state_);
  for (auto& [_, client] : rcf_clients_) {
    client->setState(raft_state_);
  }
}

void RaftNode::Start() {
  LOG(util::kRaft, "S%d Starts", node_id_me_);
  exit_.store(false);
  rcf_server_->Start();
  raft_state_->Init();
  startTickerThread();
  startApplierThread();
}

void RaftNode::Exit() {
  // First ensures ticker thread and applier thread exits, in case they access raft_state
  // field after we release it
  this->exit_.store(true);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // TODO: Release storage or state machine if it's necessary
}

RaftNode::~RaftNode() {
  delete raft_state_;
  for (auto& [_, client] : rcf_clients_) {
    delete client;
  }
  delete rcf_server_;
}

void RaftNode::startTickerThread() {
  auto ticker = [=]() {
    while (!this->Exited()) {
      // Tick the raft state for every 10ms so that the raft can make progress
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      this->raft_state_->Tick();
    }
  };
  std::thread ticker_thread(ticker);
  ticker_thread.detach();
}

void RaftNode::startApplierThread() {
  // TODO
}

}  // namespace raft
