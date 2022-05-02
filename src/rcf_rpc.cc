#include "rcf_rpc.h"
#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/RCF.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "raft_struct.h"
#include "serializer.h"

namespace raft {
namespace rpc {

RCFRpcClient::RCFRpcClient(const Address &target_address, RaftState *raft)
    : raft_(raft),
      client_(RCF::TcpEndpoint(target_address.ip, target_address.port)) {}

// Asynchronizingly send RequestVote args
void RCFRpcClient::sendMessage(const RequestVoteArgs &args) {
  auto serializer = Serializer::NewSerializer();
  RCF::ByteBuffer args_buffer(serializer.getSerializeSize(args));
  serializer.Serialize(&args, &args_buffer);
}

void RCFRpcClient::sendMessage(const AppendEntriesArgs& args) {}

void RCFRpcClient::onRequestVoteComp(RCF::Future<RCF::ByteBuffer> reply,
                                     RaftRpcServicePtr clientPtr) {
  auto serializer = Serializer::NewSerializer();
  auto client = clientPtr.get();
}

} // namespace rpc
} // namespace raft
