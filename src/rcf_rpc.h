#pragma once

#include "RCF/Future.hpp"
#include "raft_struct.h"
#include "rpc.h"
#include <memory>

namespace raft {
namespace rpc {

RCF_BEGIN(I_RaftRPCService, "I_RaftRPCService")
RCF_METHOD_R1(RCF::ByteBuffer, RequestVote, const RCF::ByteBuffer &)
RCF_END(I_RaftService)

class RaftService {
};

class RCFRpcClient final : public RpcClient {
  using RaftRpcServicePtr = std::shared_ptr<RcfClient<I_RaftRPCService>>;

public:
  RCFRpcClient(const Address &target_address, RaftState *raft);

public:
  void Init() override;
  void sendMessage(const RequestVoteArgs &args) override;
  void sendMessage(const AppendEntriesArgs& args) override;

private:
  void onRequestVoteComp(RCF::Future<RCF::ByteBuffer> reply,
                         RaftRpcServicePtr clientPtr);

private:
  RaftState *raft_;
  RcfClient<I_RaftRPCService> client_;
};

class RCFRpcServer final : public RpcServer {
public:
  RCFRpcServer(const Address &address, RaftState *raft);

public:
  void Start() override;
  void dealWithMessage(const RequestVoteArgs &reply) override;

private:
  RaftState *raft_;
};

} // namespace rpc
} // namespace raft
