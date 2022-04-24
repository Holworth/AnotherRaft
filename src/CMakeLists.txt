cmake_minimum_required(VERSION 3.8)

set(RAFT_SRC log_entry.cc)

add_library(raft ${RAFT_SRC})
