#include "kv_format.h"

#include <cstddef>
#include <cstring>

#include "type.h"

namespace kv {
size_t GetRawBytesSizeForRequest(const Request& request) {
  size_t hdr_size = RequestHdrSize();
  size_t key_size = sizeof(int) + request.key.size();
  size_t val_size = sizeof(int) + request.value.size();
  return hdr_size + key_size + val_size;
}

void RequestToRawBytes(const Request& request, char* bytes) {
  std::memcpy(bytes, &request, RequestHdrSize());
  bytes = MakePrefixLengthKey(request.key, bytes + RequestHdrSize());
  MakePrefixLengthKey(request.value, bytes);
}

void RawBytesToRequest(char* bytes, Request* request) {
  std::memcpy(request, bytes, RequestHdrSize());
  bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));
  GetKeyFromPrefixLengthFormat(bytes, &(request->value));
}

}  // namespace kv
