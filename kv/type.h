#pragma once
#include <cstdint>
#include <string>

#include "raft_type.h"

namespace kv {
enum RequestType {
  kPut = 1,
  kDelete = 2,
  kGet = 3,
  kDetectLeader = 4,
  // kUndetermined = 5,
};

enum ErrorType {
  kNotALeader = 1,
  kKeyNotExist = 2,
  kEntryDeleted = 3,
  kRequestExecTimeout = 4,
  kOk = 5,
  // This error is used in test, in which a request can not be done within
  // specified seconds, probably due to no enough servers
  kKVRequestTimesout = 6,
  kRPCCallFailed = 7,
};

struct Request {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  std::string key;
  std::string value;  // Ignore it if this request is not Put
  void serialize(SF::Archive& ar) { ar& type& client_id& sequence& key& value; }
};

struct Response {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  ErrorType err;
  raft::raft_term_t raft_term;
  std::string value;           // Valid if type is Get
  uint64_t apply_elapse_time;  // Time elapsed to apply this entry to state machine
  void serialize(SF::Archive& ar) {
    ar& type& client_id& sequence& err& raft_term& value& apply_elapse_time;
  }
};

struct OperationResults {
  ErrorType err;
  uint64_t apply_elapse_time;
};

inline constexpr size_t RequestHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2;
}

inline constexpr size_t ResponseHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2 + sizeof(ErrorType) +
         sizeof(raft::raft_term_t);
}

const std::string ToString(RequestType type);
const std::string ToString(ErrorType type);
const std::string ToString(const Request& req);

}  // namespace kv
