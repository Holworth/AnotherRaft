#include "rcf_rpc.h"

#include <chrono>
#include <ratio>

#include "RCF/ByteBuffer.hpp"
#include "RCF/ClientStub.hpp"
#include "RCF/Endpoint.hpp"
#include "RCF/Future.hpp"
#include "RCF/RCF.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "util.h"

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

#ifdef ENABLE_PERF_RECORDING
  util::RaftAppendEntriesProcessPerfCounter counter(arg_buf.getLength());
#endif

  raft_->Process(&args, &reply);

#ifdef ENABLE_PERF_RECORDING
  counter.Record();
  PERF_LOG(&counter);
#endif

  RCF::ByteBuffer reply_buf(serializer.getSerializeSize(reply));
  serializer.Serialize(&reply, &reply_buf);

  return reply_buf;
}

RCF::ByteBuffer RaftRPCService::RequestFragments(const RCF::ByteBuffer &arg_buf) {
  RequestFragmentsArgs args;
  RequestFragmentsReply reply;

  auto serializer = Serializer::NewSerializer();
  serializer.Deserialize(&arg_buf, &args);
  raft_->Process(&args, &reply);

  RCF::ByteBuffer reply_buf(serializer.getSerializeSize(reply));
  serializer.Serialize(&reply, &reply_buf);

  return reply_buf;
}

RCFRpcClient::RCFRpcClient(const NetAddress &target_address, raft_node_id_t id)
    : target_address_(target_address), id_(id), rcf_init_(), stopped_(false) {
  LOG(util::kRaft, "S%d init with Raft RPC server(ip=%s port=%d)", id_,
      target_address_.ip.c_str(), target_address_.port);
}

void RCFRpcClient::Init() {}

void RCFRpcClient::sendMessage(const RequestVoteArgs &args) {
  if (stopped_) {  // Directly return if this client is stopped
    return;
  }

  ClientPtr client_ptr(new RcfClient<I_RaftRPCService>(
      RCF::TcpEndpoint(target_address_.ip, target_address_.port)));

  setMaxTransportLength(client_ptr);

  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  RCF::Future<RCF::ByteBuffer> ret;
  auto cmp_callback = [=]() {
    onRequestVoteComplete(ret, client_ptr, this->raft_, this->id_);
  };
  ret = client_ptr->RequestVote(RCF::AsyncTwoway(cmp_callback), arg_buf);
}

void RCFRpcClient::sendMessage(const AppendEntriesArgs &args) {
  if (stopped_) {  // Directly return if this client is stopped
    return;
  }
  ClientPtr client_ptr(new RcfClient<I_RaftRPCService>(
      RCF::TcpEndpoint(target_address_.ip, target_address_.port)));

  setMaxTransportLength(client_ptr);

  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  RCF::Future<RCF::ByteBuffer> ret;

#ifdef ENABLE_PERF_RECORDING
    util::AppendEntriesRPCPerfCounter counter(arg_buf.getLength());
#endif

  auto cmp_callback = [=]() {
#ifdef ENABLE_PERF_RECORDING
    onAppendEntriesCompleteRecordTimer(ret, client_ptr, this->raft_, this->id_, counter);
#else
    onAppendEntriesComplete(ret, client_ptr, this->raft_, this->id_);
#endif
  };
  ret = client_ptr->AppendEntries(RCF::AsyncTwoway(cmp_callback), arg_buf);
}

void RCFRpcClient::sendMessage(const RequestFragmentsArgs &args) {
  if (stopped_) {
    return;
  }

  ClientPtr client_ptr(new RcfClient<I_RaftRPCService>(
      RCF::TcpEndpoint(target_address_.ip, target_address_.port)));

  setMaxTransportLength(client_ptr);

  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  RCF::Future<RCF::ByteBuffer> ret;
  auto cmp_callback = [=]() {
    onRequestFragmentsComplete(ret, client_ptr, this->raft_, this->id_);
  };
  ret = client_ptr->RequestFragments(RCF::AsyncTwoway(cmp_callback), arg_buf);
}

void RCFRpcClient::onRequestVoteComplete(RCF::Future<RCF::ByteBuffer> ret,
                                         ClientPtr client_ptr, RaftState *raft,
                                         raft_node_id_t peer) {
  (void)client_ptr;
  auto ePtr = ret.getAsyncException();
  if (ePtr.get()) {
    LOG(util::kRPC, "S%d RequestVote RPC Call Error: %s", peer,
        ePtr->getErrorString().c_str());
  } else {
    RCF::ByteBuffer ret_buf = *ret;
    RequestVoteReply reply;
    Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
    raft->Process(&reply);
  }
}

void RCFRpcClient::onAppendEntriesComplete(RCF::Future<RCF::ByteBuffer> ret,
                                           ClientPtr client_ptr, RaftState *raft,
                                           raft_node_id_t peer) {
  (void)client_ptr;

  auto ePtr = ret.getAsyncException();
  if (ePtr.get()) {
    LOG(util::kRPC, "S%d AppendEntries RPC Call Error: %s", peer,
        ePtr->getErrorString().c_str());
  } else {
    RCF::ByteBuffer ret_buf = *ret;
    AppendEntriesReply reply;
    Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
    raft->Process(&reply);
  }
}

void RCFRpcClient::onAppendEntriesCompleteRecordTimer(
    RCF::Future<RCF::ByteBuffer> ret, ClientPtr client_ptr, RaftState *raft,
    raft_node_id_t peer, util::AppendEntriesRPCPerfCounter counter) {
  (void)client_ptr;

  auto ePtr = ret.getAsyncException();
  if (ePtr.get()) {
    LOG(util::kRPC, "S%d AppendEntries RPC Call Error: %s", peer,
        ePtr->getErrorString().c_str());
  } else {
    counter.Record();
    PERF_LOG(&counter);

    RCF::ByteBuffer ret_buf = *ret;
    AppendEntriesReply reply;
    Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
    raft->Process(&reply);
  }
}

void RCFRpcClient::onRequestFragmentsComplete(RCF::Future<RCF::ByteBuffer> ret,
                                              ClientPtr client_ptr, RaftState *raft,
                                              raft_node_id_t peer) {
  (void)client_ptr;

  auto ePtr = ret.getAsyncException();
  if (ePtr.get()) {
    LOG(util::kRPC, "S%d RequestFragments RPC Call Error: %s", peer,
        ePtr->getErrorString().c_str());
  } else {
    RCF::ByteBuffer ret_buf = *ret;
    RequestFragmentsReply reply;
    Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
    raft->Process(&reply);
  }
}

RCFRpcServer::RCFRpcServer(const NetAddress &my_address)
    : rcf_init_(),
      server_(RCF::TcpEndpoint(my_address.ip, my_address.port)),
      service_() {}

void RCFRpcServer::Start() {
  server_.getServerTransport().setMaxIncomingMessageLength(config::kMaxMessageLength);
  server_.bind<I_RaftRPCService>(service_);
  server_.start();
}

void RCFRpcServer::Stop() {
  LOG(util::kRaft, "stop running raft rpc server");
  server_.stop();
}

void RCFRpcServer::dealWithMessage(const RequestVoteArgs &reply) {
  // Nothing to do
}

}  // namespace rpc
}  // namespace raft
