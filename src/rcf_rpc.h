#pragma once

#include <memory>

#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "raft_struct.h"
#include "rpc.h"

namespace raft {
namespace rpc {

RCF_BEGIN(I_RaftRPCService, "I_RaftRPCService")
RCF_METHOD_R1(RCF::ByteBuffer, RequestVote, const RCF::ByteBuffer &)
RCF_END(I_RaftService)

class RaftService {};

// An implementation of RpcClient interface using RCF (Remote Call Framework)
class RCFRpcClient final : public RpcClient {
 public:
  // Construction
  RCFRpcClient(const NetAddress &target_address);

  RCFRpcClient &operator=(const RCFRpcClient &) = delete;
  RCFRpcClient(const RCFRpcClient &) = delete;

 public:
  void SetRaftState(RaftState *raft) { raft_ = raft; }

 public:
  void Init() override;
  void sendMessage(const RequestVoteArgs &args) override;
  void sendMessage(const AppendEntriesArgs &args) override;

 private:
  RaftState *raft_;
  RcfClient<I_RaftRPCService> client_;
};

}  // namespace rpc
}  // namespace raft
