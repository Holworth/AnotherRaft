#pragma once

#include <chrono>
#include <memory>

#include "RCF/ByteBuffer.hpp"
#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RcfServer.hpp"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "serializer.h"

namespace raft {
namespace rpc {

namespace config {
// Each RPC call size must not exceed 512MB
static constexpr size_t kMaxMessageLength = 512 * 1024 * 1024;
};  // namespace config

RCF_BEGIN(I_RaftRPCService, "I_RaftRPCService")
RCF_METHOD_R1(RCF::ByteBuffer, RequestVote, const RCF::ByteBuffer &)
RCF_METHOD_R1(RCF::ByteBuffer, AppendEntries, const RCF::ByteBuffer &)
RCF_METHOD_R1(RCF::ByteBuffer, RequestFragments, const RCF::ByteBuffer &)
RCF_END(I_RaftService)

class RaftRPCService {
 public:
  RaftRPCService() = default;
  void SetRaftState(RaftState *raft) { raft_ = raft; }
  RCF::ByteBuffer RequestVote(const RCF::ByteBuffer &arg_buf);
  RCF::ByteBuffer AppendEntries(const RCF::ByteBuffer &arg_buf);
  RCF::ByteBuffer RequestFragments(const RCF::ByteBuffer &arg_buf);

 private:
  RaftState *raft_;
};

// An implementation of RpcClient interface using RCF (Remote Call Framework)
class RCFRpcClient final : public RpcClient {
  using ClientPtr = std::shared_ptr<RcfClient<I_RaftRPCService>>;

 public:
  // Construction
  RCFRpcClient(const NetAddress &target_address, raft_node_id_t id);

  RCFRpcClient &operator=(const RCFRpcClient &) = delete;
  RCFRpcClient(const RCFRpcClient &) = delete;

 public:
  void SetRaftState(RaftState *raft) { raft_ = raft; }

 public:
  void Init() override;
  void sendMessage(const RequestVoteArgs &args) override;
  void sendMessage(const AppendEntriesArgs &args) override;
  void sendMessage(const RequestFragmentsArgs &args) override;
  void setState(void *state) override { raft_ = reinterpret_cast<RaftState *>(state); }

  void setMaxTransportLength(ClientPtr ptr) {
    ptr->getClientStub().getTransport().setMaxOutgoingMessageLength(
        config::kMaxMessageLength);
    ptr->getClientStub().getTransport().setMaxIncomingMessageLength(
        config::kMaxMessageLength);
  }

  void stop() override { stopped_ = true; };
  void recover() override { stopped_ = false; };

 private:
  // Callback function
  static void onRequestVoteComplete(RCF::Future<RCF::ByteBuffer> buf,
                                    ClientPtr client_ptr, RaftState *raft,
                                    raft_node_id_t peer);

  static void onAppendEntriesComplete(RCF::Future<RCF::ByteBuffer> buf,
                                      ClientPtr client_ptr, RaftState *raft,
                                      raft_node_id_t peer);

  static void onAppendEntriesCompleteRecordTimer(RCF::Future<RCF::ByteBuffer> buf,
                                                 ClientPtr client_ptr, RaftState *raft,
                                                 raft_node_id_t peer,
                                                 util::AppendEntriesPerfCounter counter);

  static void onRequestFragmentsComplete(RCF::Future<RCF::ByteBuffer> buf,
                                         ClientPtr client_ptr, RaftState *raft,
                                         raft_node_id_t peer);

 private:
  RaftState *raft_;
  RCF::RcfInit rcf_init_;
  NetAddress target_address_;
  bool stopped_;
  raft_node_id_t id_;
};

class RCFRpcServer final : public RpcServer {
 public:
  RCFRpcServer(const NetAddress &my_address);

 public:
  void Start() override;
  void Stop() override;
  void dealWithMessage(const RequestVoteArgs &reply) override;
  void setState(void *state) override {
    service_.SetRaftState(reinterpret_cast<RaftState *>(state));
  }

 private:
  RCF::RcfInit rcf_init_;
  RCF::RcfServer server_;
  RaftRPCService service_;
};
}  // namespace rpc
}  // namespace raft
