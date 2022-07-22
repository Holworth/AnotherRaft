include_directories(${LEVELDB_DIR}/include)
find_library(libleveldb 
             NAMES leveldb 
             HINTS ../third-party/leveldb/lib/libleveldb.a)

if (NOT libleveldb)
    MESSAGE(FATAL_ERROR "libleveldb not found")
endif()

SET(
  KV_SRC 
  type.cc
  config.cc
  kv_format.cc
  storage_engine.cc
  kv_server.cc
  rpc.cc
  kv_node.cc
  client.cc
)


add_library(kv ${KV_SRC})
target_link_libraries(kv raft ${libleveldb})
target_include_directories(kv PUBLIC ${CMAKE_CURRENT_LIST_DIR})

add_subdirectory(test)

