cmake_minimum_required(VERSION 3.8)
project(raft)

set(CMAKE_CXX_STANDARD 17)

if(${LOG} STREQUAL on)
  add_compile_definitions(ENABLE_LOG)
endif()

include_directories(${CMAKE_SOURCE_DIR}/RCF/include)
add_subdirectory(${CMAKE_SOURCE_DIR}/RCF)

include_directories(${CMAKE_SOURCE_DIR}/third-party/leveldb/include)
include_directories(${CMAKE_SOURCE_DIR}/third-party/rocksdb/include)

add_subdirectory(${CMAKE_SOURCE_DIR}/third-party)
add_subdirectory(${CMAKE_SOURCE_DIR}/raft)
add_subdirectory(${CMAKE_SOURCE_DIR}/test)
add_subdirectory(${CMAKE_SOURCE_DIR}/kv)
add_subdirectory(${CMAKE_SOURCE_DIR}/bench)



