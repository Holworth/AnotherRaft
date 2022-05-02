#pragma once

#include "raft_struct.h"
#include "raft_type.h"
#include <string>

namespace raft {
namespace rpc {

struct Address {
  std::string ip;
  uint16_t port;
};

class RaftState;

// An interface for sending rpc requests to target remote peer
class RpcClient {
public:
  // Do all initialization work, for example, bind to remote target server
  virtual void Init() = 0;
  virtual void sendMessage(const RequestVoteArgs &args) = 0;
  virtual void sendMessage(const AppendEntriesArgs& args) = 0;
};

// An interface for receiving rpc request and deals with it
class RpcServer {
public:
  // Start running the server
  virtual void Start() = 0;
  virtual void dealWithMessage(const RequestVoteArgs &reply) = 0;
};

} // namespace rpc
} // namespace raft
