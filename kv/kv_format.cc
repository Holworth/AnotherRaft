#include "kv_format.h"

#include <cstddef>
#include <cstring>

#include "raft_type.h"
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

void RaftEntryToRequest(const raft::LogEntry& ent, Request* request) {
  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    char tmp_data[12];
    *reinterpret_cast<int*>(tmp_data) = 1;
    *reinterpret_cast<int*>(tmp_data + 4) = 0;
    *reinterpret_cast<int*>(tmp_data + 8) = 0;

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.FragmentSlice().size());

    char tmp_data[12];
    *reinterpret_cast<int*>(tmp_data) = ent.GetVersion().GetK();
    *reinterpret_cast<int*>(tmp_data + 4) = ent.GetVersion().GetM();
    *reinterpret_cast<int*>(tmp_data + 8) = ent.GetVersion().GetFragmentId();

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // Append the value contents
    request->value.append(ent.FragmentSlice().data(), ent.FragmentSlice().size());
  }
}

}  // namespace kv
