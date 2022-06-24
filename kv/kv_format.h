#pragma once
#include "type.h"
namespace kv {
// Get the size of bytes needed to serialize an request into raw bytes
size_t GetRawBytesSizeForRequest(const Request& request);

// Serialize a request into raw bytes
void RequestToRawBytes(const Request& request, char* bytes);

// Deserialize a request from raw bytes
void RawBytesToRequest(char* bytes, Request* request);

// Serialize a string to raw bytes specified by buf, returns the next position of
// last written bytes
inline char* MakePrefixLengthKey(const std::string& s, char* buf) {
  *reinterpret_cast<int*>(buf) = static_cast<int>(s.size());
  std::memcpy(buf + sizeof(int), s.c_str(), s.size());
  return buf + sizeof(int) + s.size();
}

inline char* GetKeyFromPrefixLengthFormat(char* buf, std::string* key) {
  int key_size = *reinterpret_cast<int*>(buf);
  *key = std::string(buf + sizeof(int), key_size);
  return buf + sizeof(int) + key_size;
}
}  // namespace kv
