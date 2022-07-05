#pragma once
#include <cstdint>
#include <cstring>
#include <sstream>

#include "RCF/RCF.hpp"

namespace raft {
using raft_index_t = uint32_t;
using raft_term_t = uint32_t;
using raft_node_id_t = uint32_t;
using raft_sequence_t = uint32_t;

enum raft_entry_type { kNormal = 0, kFragments = 1, kTypeMax = 2 };

struct Version {
  raft_index_t idx;
  uint64_t sequence;
  int k, m;

  // Dump the important information
  std::string toString() const {
    std::stringstream ss;
    ss << "Version {index:" << idx << ", sequence:" << sequence << ", k:" << k
       << ", m:" << m << "}";
    return ss.str();
  }

  bool operator==(const Version& rhs) const {
    return std::memcmp(this, &rhs, sizeof(Version)) == 0;
  }

  bool operator!=(const Version& rhs) const {
    return !(*this == rhs);
  }
};

// Structs that are related to raft core algorithm

}  // namespace raft
