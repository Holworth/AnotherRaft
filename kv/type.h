#pragma once
#include <cstdint>
#include <string>

namespace kv {
enum RequestType {
  kPut = 1,
  kDelete = 2,
  kGet = 3,
  kUndetermined = 4,
  kDetectLeader = 5
};

enum ErrorType { kNotLeader = 1, kKeyNotExist = 2, kEntryDeleted = 3, kOk = 4 };

struct Request {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  std::string key;
  std::string value;  // Ignore it if this request is not Put
};

struct Response {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  ErrorType err;
  std::string value;  // Valid if type is Get
};

inline constexpr size_t RequestHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2;
}

inline constexpr size_t ResponseHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2 + sizeof(ErrorType);
}

}  // namespace kv
