#include "serializer.h"

#include <cstring>

#include "RCF/ByteBuffer.hpp"
#include "log_entry.h"
#include "raft_struct.h"
#include "raft_type.h"

namespace raft {
Serializer Serializer::NewSerializer() { return Serializer(); }

char *Serializer::serialize_logentry_helper(const LogEntry *entry, char *dst) {
  std::memcpy(dst, entry, sizeof(LogEntry));
  dst += sizeof(LogEntry);
  dst = PutPrefixLengthSlice(entry->NotEncodedSlice(), dst);
  dst = PutPrefixLengthSlice(entry->FragmentSlice(), dst);
  return dst;
}

const char *Serializer::deserialize_logentry_helper(const char *src, LogEntry *entry) {
  std::memcpy(entry, src, sizeof(LogEntry));
  src += sizeof(LogEntry);
  Slice not_encoded, frag;
  src = ParsePrefixLengthSlice(src, &not_encoded);
  src = ParsePrefixLengthSlice(src, &frag);

  entry->SetNotEncodedSlice(not_encoded);
  entry->SetFragmentSlice(frag);

  if (entry->Type() == kNormal) {
    entry->SetCommandData(not_encoded);
  }
  return src;
}

const char *Serializer::deserialize_logentry_withbound(const char *src, size_t len,
                                                       LogEntry *entry) {
  if (len < sizeof(LogEntry)) {
    return nullptr;
  }
  std::memcpy(entry, src, sizeof(LogEntry));
  src += sizeof(LogEntry);
  len -= sizeof(LogEntry);
  Slice not_encoded, frag;
  auto tmp_src = src;
  src = ParsePrefixLengthSliceWithBound(src, len, &not_encoded);
  if (src == nullptr) return nullptr;
  len -= (src - tmp_src);
  src = ParsePrefixLengthSliceWithBound(src, len, &frag);
  if (src == nullptr) return nullptr;

  entry->SetNotEncodedSlice(not_encoded);
  entry->SetFragmentSlice(frag);

  if (entry->Type() == kNormal) {
    entry->SetCommandData(not_encoded);
  }
  return src;
}

void Serializer::Serialize(const LogEntry *entry, RCF::ByteBuffer *buffer) {
  serialize_logentry_helper(entry, buffer->getPtr());
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, LogEntry *entry) {
  deserialize_logentry_helper(buffer->getPtr(), entry);
}

void Serializer::Serialize(const RequestVoteArgs *args, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, args, sizeof(RequestVoteArgs));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestVoteArgs *args) {
  auto src = buffer->getPtr();
  std::memcpy(args, src, sizeof(RequestVoteArgs));
}

void Serializer::Serialize(const RequestVoteReply *reply, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, reply, sizeof(RequestVoteReply));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestVoteReply *reply) {
  auto src = buffer->getPtr();
  std::memcpy(reply, src, sizeof(RequestVoteReply));
}

void Serializer::Serialize(const AppendEntriesArgs *args, RCF::ByteBuffer *buffer) {
  assert(args->entry_cnt == args->entries.size());
  auto dst = buffer->getPtr();
  std::memcpy(dst, args, kAppendEntriesArgsHdrSize);
  dst += kAppendEntriesArgsHdrSize;
  for (const auto &ent : args->entries) {
    dst = serialize_logentry_helper(&ent, dst);
  }
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, AppendEntriesArgs *args) {
  const char *src = buffer->getPtr();
  std::memcpy(args, src, kAppendEntriesArgsHdrSize);
  src += kAppendEntriesArgsHdrSize;
  args->entries.reserve(args->entry_cnt);
  for (decltype(args->entry_cnt) i = 0; i < args->entry_cnt; ++i) {
    LogEntry ent;
    src = deserialize_logentry_helper(src, &ent);
    args->entries.push_back(ent);
  }
}

void Serializer::Serialize(const AppendEntriesReply *reply, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, reply, sizeof(AppendEntriesReply));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, AppendEntriesReply *reply) {
  auto src = buffer->getPtr();
  std::memcpy(reply, src, sizeof(AppendEntriesReply));
}

char *Serializer::PutPrefixLengthSlice(const Slice &slice, char *buf) {
  *reinterpret_cast<size_t *>(buf) = slice.size();
  buf += sizeof(size_t);
  std::memcpy(buf, slice.data(), slice.size());
  return buf + slice.size();
}

const char *Serializer::ParsePrefixLengthSlice(const char *buf, Slice *slice) {
  size_t size = *reinterpret_cast<const size_t *>(buf);
  // printf("[Deserialize alloc size=%lu]\n", size);
  char *data = new char[size];
  buf += sizeof(size_t);
  std::memcpy(data, buf, size);
  *slice = Slice(data, size);
  return buf + size;
}

const char *Serializer::ParsePrefixLengthSliceWithBound(const char *buf, size_t len,
                                                        Slice *slice) {
  if (len < sizeof(size_t)) {
    return nullptr;
  }
  size_t size = *reinterpret_cast<const size_t *>(buf);
  if (size + sizeof(size_t) > len) {  // Beyond range
    return nullptr;
  }
  char *data = new char[size];
  buf += sizeof(size_t);
  std::memcpy(data, buf, size);
  *slice = Slice(data, size);
  return buf + size;
}

size_t Serializer::getSerializeSize(const LogEntry &entry) {
  size_t ret = sizeof(LogEntry);
  ret += entry.NotEncodedSlice().size();
  ret += entry.FragmentSlice().size();
  ret += 2 * sizeof(size_t);
  // Make size 4B aligment
  return (ret - 1) / 4 * 4 + 4;
}

size_t Serializer::getSerializeSize(const RequestVoteArgs &args) { return sizeof(args); }

size_t Serializer::getSerializeSize(const RequestVoteReply &reply) {
  return sizeof(reply);
}

size_t Serializer::getSerializeSize(const AppendEntriesArgs &args) {
  size_t ret = kAppendEntriesArgsHdrSize;
  for (const auto &ent : args.entries) {
    ret += getSerializeSize(ent);
  }
  // Make the size 4B alignment
  return (ret - 1) / 4 * 4 + 4;
}

size_t Serializer::getSerializeSize(const AppendEntriesReply &reply) {
  return sizeof(reply);
}

}  // namespace raft
