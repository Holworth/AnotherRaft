#pragma once

#include "raft_type.h"
#include "SF/Archive.hpp"

#include <cstddef>
#include <string>
#include <algorithm>
#include <cassert>

namespace raft {

class Serializer;

class Slice {
public:
  Slice(char* data, size_t size): data_(data), size_(size) {}
  Slice(const std::string& s):data_(new char[s.size()]), size_(s.size()) {
    std::memcpy(data_, s.c_str(), size_);
  }

  Slice() = default;
  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;
  
  auto data() const -> char* { return data_; }
  auto size() const -> size_t { return size_; }
  auto valid() const -> bool { return data_ != nullptr && size_ > 0; }
  auto toString() const -> std::string { return std::string(data_, size_); }

  // Require both slice are valid
  auto compare(const Slice& slice) -> bool {
    assert(valid() && slice.valid());
    auto cmp_len = std::min(size(), slice.size());
    auto cmp_res = std::memcmp(data(), slice.data(), cmp_len);
    if (cmp_res != 0 || size() == slice.size()) {
      return cmp_res;
    }
    return size() > slice.size() ? 1 : -1;
  }

private:
  char* data_ = nullptr;
  size_t size_ = 0;
};

class LogEntry {
  friend class Serializer;
public:
  LogEntry() = default;
  LogEntry& operator=(const LogEntry&) = default;
  
  auto Index() const -> raft_index_t { return index; }
  void SetIndex(raft_index_t index) { this->index = index; }

  auto Term() const -> raft_term_t { return term; }
  void SetTerm(raft_term_t term) { this->term = term; }

  auto Type() const -> raft_entry_type { return type; }
  void SetType(raft_entry_type type) { this->type = type; }

  auto Sequence() const -> raft_sequence_t { return seq; }
  void SetSequence(raft_sequence_t seq) { this->seq = seq; }

  auto GetN() const -> uint16_t { return n; }
  auto SetN(uint16_t n) { this->n = n; }

  auto GetK() const -> uint16_t { return k; }
  void SetK(uint16_t k) { this->k = k; }

  auto FragId() const -> uint16_t { return fragment_id; }
  void SetFragId(uint16_t id)  { this->fragment_id = id; }

  auto StartOffset() -> int { return start_fragment_offset; }
  void SetStartOffset(int off) { start_fragment_offset = off; }

  auto CommandData() const -> Slice { return Type() == kNormal ? command_data_ : Slice(); }
  void SetCommandData(const Slice& slice) { command_data_ = slice; }

  auto NotEncodedSlice() const -> Slice { return Type() == kNormal ? CommandData() : not_encoded_slice_; }
  void SetNotEncodedSlice(const Slice& slice) { not_encoded_slice_ = slice; }
  
  auto FragmentSlice() const -> Slice { return Type() == kNormal ? Slice() : fragment_slice_; }
  void SetFragmentSlice(const Slice& slice) { fragment_slice_ = slice; }
  
  // Serialization function required by RCF
  void serialize(SF::Archive& ar);

private:
  // These three attributes are allocated when creating a command
  raft_term_t term;
  raft_index_t index;
  raft_entry_type type; // Full entry or fragments
  raft_sequence_t seq;

  // k+m in ec mode
  uint16_t n;

  // k: the minimum number of fragments to recover a full entry
  uint16_t k;

  // Uniquely identify a fragment among a stripe, valid value: [0, n)
  uint16_t fragment_id;

  // [REQUIRE] specified by user, indicating the start offset of command 
  // data for encoding
  int start_fragment_offset;

  Slice command_data_;  // Spcified by user, valid iff type = normal
  Slice not_encoded_slice_; // Command data not being encoded
  Slice fragment_slice_;  // Fragments of encoded data
};

auto operator==(const LogEntry& lhs, const LogEntry& rhs) -> bool;
}
