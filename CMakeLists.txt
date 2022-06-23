cmake_minimum_required(VERSION 3.8)
project(raft)

set(CMAKE_CXX_STANDARD 17)

set(ENABLE_RAFT_LOG
    "false"
    CACHE STRING "Enable Raft runtime log or not, disable log by default")
  # add_compile_definitions(ENABLE_RAFT_LOG="${ENABLE_RAFT_LOG}")
  # message("ENABLE_RAFT_LOG = ${ENABLE_RAFT_LOG}")

include_directories(${CMAKE_SOURCE_DIR}/RCF/include)
add_subdirectory(${CMAKE_SOURCE_DIR}/RCF)

include_directories(${CMAKE_SOURCE_DIR}/third-party/leveldb/include)

add_subdirectory(${CMAKE_SOURCE_DIR}/third-party)
add_subdirectory(${CMAKE_SOURCE_DIR}/raft)
add_subdirectory(${CMAKE_SOURCE_DIR}/test)
add_subdirectory(${CMAKE_SOURCE_DIR}/bench)
add_subdirectory(${CMAKE_SOURCE_DIR}/kv)



