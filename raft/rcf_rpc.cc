#include "rcf_rpc.h"

#include <sys/stat.h>

#include <ctime>
#include <fstream>

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

  // Record the time of processing an AppendEntries RPC call
  auto start = util::NowTime();
  if (raft_ != nullptr) {
    raft_->Process(&args, &reply);
  }
  auto dura = util::DurationToMicros(start, util::NowTime());
  reply.process_time = dura;

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

  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  setMaxTransportLength(client_ptr);
  client_ptr->getClientStubPtr()->setRemoteCallTimeoutMs(config::kRPCTimeout);

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

  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer arg_buf(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &arg_buf);

  setMaxTransportLength(client_ptr);
  client_ptr->getClientStubPtr()->setRemoteCallTimeoutMs(config::kRPCTimeout);

  auto start_time = util::NowTime();

  RCF::Future<RCF::ByteBuffer> ret;
  auto cmp_callback = [=]() {
    onAppendEntriesComplete(ret, client_ptr, this->raft_, this->id_,
                            {arg_buf.getLength(), start_time}, &(this->recorder_));
  };
  ret = client_ptr->AppendEntries(RCF::AsyncTwoway(cmp_callback), arg_buf);
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
                                           raft_node_id_t peer, RPCArgStats rpc_stats,
                                           RPCStatsRecorder *recorder) {
  (void)client_ptr;

  auto ePtr = ret.getAsyncException();
  if (ePtr.get()) {
    LOG(util::kRPC, "S%d AppendEntries RPC Call Error: %s", peer,
        ePtr->getErrorString().c_str());
  } else {
    auto time = util::DurationToMicros(rpc_stats.start_time, util::NowTime());

    RCF::ByteBuffer ret_buf = *ret;
    AppendEntriesReply reply;
    Serializer::NewSerializer().Deserialize(&ret_buf, &reply);
    if (raft != nullptr) {
      raft->Process(&reply);
    }

    // Only record stat that is not heartbeat messages
    if (rpc_stats.arg_size > kAppendEntriesArgsHdrSize) {
      auto stat = RPCStats{rpc_stats.arg_size, ret_buf.getLength(), time,
                           time - reply.process_time, reply.process_time};
      recorder->Add(stat);
    }
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

void RPCStatsRecorder::Dump(const std::string &dst) {
  std::ofstream of;
  of.open(dst);

  uint64_t total_total_time = 0;
  uint64_t total_transfer_time = 0;
  uint64_t total_process_time = 0;

  for (const auto &stat : history_) {
    of << stat.ToString() << "\n";
    total_total_time += stat.total_time;
    total_process_time += stat.process_time;
    total_transfer_time += stat.transfer_time;
  }

  of << "[Average Total Time]:" << total_total_time / history_.size()
     << "[Average Process Time]:" << total_process_time / history_.size()
     << "[Average Transfer Time]:" << total_transfer_time / history_.size();
}

}  // namespace rpc
}  // namespace raft
