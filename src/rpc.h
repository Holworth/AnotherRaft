#pragma once

#include "raft_struct.h"
#include "raft_type.h"
#include <string>

namespace raft {
namespace rpc {

struct NetAddress {
  std::string ip;
  uint16_t port;
};


// An interface for sending rpc requests to target remote peer
class RpcClient {
public:
  virtual ~RpcClient() = default;
  // Do all initialization work, for example, bind to remote target server
  virtual void Init() = 0;
  virtual void sendMessage(const RequestVoteArgs &args) = 0;
  virtual void sendMessage(const AppendEntriesArgs& args) = 0;
  virtual void setState(void* state) = 0;
};

// An interface for receiving rpc request and deals with it
class RpcServer {
public:
  virtual ~RpcServer() = default;
  // Start running the server
  virtual void Start() = 0;
  virtual void dealWithMessage(const RequestVoteArgs &reply) = 0;
  virtual void setState(void* state) = 0;
};

} // namespace rpc
} // namespace raft
