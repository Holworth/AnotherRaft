#pragma once
#include <cstdint>
#include "RCF/RCF.hpp"

namespace raft {
  using raft_index_t = uint32_t;
  using raft_term_t  = uint32_t;
  using raft_node_id_t = uint32_t;
  using raft_sequence_t = uint32_t;
  
  enum raft_entry_type {
    kNormal = 0, kFragments = 1, kTypeMax = 2
  };
}
