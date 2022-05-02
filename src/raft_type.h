#pragma once
#include "RCF/RCF.hpp"
#include <cstdint>
#include <cstring>

namespace raft {
using raft_index_t = uint32_t;
using raft_term_t = uint32_t;
using raft_node_id_t = uint32_t;
using raft_sequence_t = uint32_t;

enum raft_entry_type { kNormal = 0, kFragments = 1, kTypeMax = 2 };

// Structs that are related to raft core algorithm
struct RequestVoteArgs {
  raft_term_t term;

  raft_node_id_t candidate_id;

  raft_index_t last_log_index;

  raft_term_t last_log_term;
};

struct RequestVoteReply {
  raft_term_t term;

  int vote_granted;
};



} // namespace raft
