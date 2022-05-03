#include "rcf_rpc.h"

#include "RCF/ClientStub.hpp"
#include "RCF/Endpoint.hpp"
#include "RCF/Future.hpp"
#include "RCF/RCF.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"

namespace raft {
namespace rpc {

RCF::ByteBuffer RaftRPCService::RequestVote(const RCF::ByteBuffer &arg_buf) {
  RequestVoteArgs args;
  RequestVoteReply reply;

  auto serializer = Serializer::NewSerializer();
  serializer.Deserialize(&arg_buf, &args);
  raft_->Process(&args, &reply);

  RCF::ByteBuffer reply_buf(serializer.getSerializeSize(reply));
  serializer.Serialize(&reply, &reply_buf);

  return reply_buf;
}

RCF::ByteBuffer RaftRPCService::AppendEntries(const RCF::ByteBuffer &arg_buf) {
  AppendEntriesArgs args;
  AppendEntriesReply reply;

  auto serializer = Serializer::NewSerializer();
  serializer.Deserialize(&arg_buf, &args);
  raft_->Process(&args, &reply);

  RCF::ByteBuffer reply_buf(serializer.getSerializeSize(reply));
  serializer.Serialize(&reply, &reply_buf);

  return reply_buf;
}

RCFRpcClient::RCFRpcClient(const NetAddress &target_address)
    : rcf_client_(RCF::TcpEndpoint(target_address.ip, target_address.port)),
      rcf_init_() {}

void RCFRpcClient::Init() {}

void RCFRpcClient::sendMessage(const RequestVoteArgs &args) {
  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  RCF::Future<RCF::ByteBuffer> ret;
  auto cmp_callback = [=]() { onRequestVoteComplete(ret, this->raft_); };

  ret = this->rcf_client_.RequestVote(RCF::AsyncTwoway(cmp_callback), arg_buf);
}

void RCFRpcClient::sendMessage(const AppendEntriesArgs &args) {
  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  RCF::Future<RCF::ByteBuffer> ret;
  auto cmp_callback = [=]() { onAppendEntriesComplete(ret, this->raft_); };

  ret = this->rcf_client_.AppendEntries(RCF::AsyncTwoway(cmp_callback), arg_buf);
}

void RCFRpcClient::onRequestVoteComplete(RCF::Future<RCF::ByteBuffer> ret,
                                         RaftState *raft) {
  RCF::ByteBuffer ret_buf = *ret;
  RequestVoteReply reply;
  Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
  raft->Process(&reply);
}

void RCFRpcClient::onAppendEntriesComplete(RCF::Future<RCF::ByteBuffer> ret,
                                           RaftState *raft) {
  RCF::ByteBuffer ret_buf = *ret;
  AppendEntriesReply reply;
  Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
  raft->Process(&reply);
}

RCFRpcServer::RCFRpcServer(const NetAddress &my_address)
    : rcf_init_(),
      server_(RCF::TcpEndpoint(my_address.ip, my_address.port)),
      service_() {}

void RCFRpcServer::Start() {
  server_.bind<I_RaftRPCService>(service_);
  server_.start();
}

void RCFRpcServer::dealWithMessage(const RequestVoteArgs &reply) {
  // Nothing to do
}

}  // namespace rpc
}  // namespace raft
