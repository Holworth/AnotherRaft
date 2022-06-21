find_package(Threads REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${GMOCK_INCLUDE_DIRS})

include_directories(${LEVELDB_DIR}/include)


SET(
  KV_SRC 
  storage_engine.cc
)

add_library(kv ${KV_SRC})
target_link_libraries(kv raft)
