find_package(Threads REQUIRED)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${GMOCK_INCLUDE_DIRS})

include_directories(${LEVELDB_DIR}/include)

find_library(libleveldb 
             NAMES leveldb 
             HINTS ../third-party/leveldb/lib/libleveldb.a)

if (NOT libleveldb)
    MESSAGE(FATAL_ERROR "libleveldb not found")
endif()


find_library(libpthread NAMES pthread)
if (NOT libpthread)
    MESSAGE(FATAL_ERROR "library pthread Not Found")
endif()

find_library(libdl NAMES dl)
if (NOT libdl)
    MESSAGE(FATAL_ERROR "library dl Not Found")
endif()

find_library(libuuid NAMES uuid)
if (NOT libuuid)
    MESSAGE(FATAL_ERROR "library uuid Not Found")
endif()

SET(LIBS ${libpthread} ${libdl} ${libuuid})

SET(
  KV_SRC 
  type.cc
  kv_format.cc
  storage_engine.cc
  kv_server.cc
  rpc.cc
)


add_library(kv ${KV_SRC})
target_link_libraries(kv raft ${libleveldb})
target_include_directories(kv PUBLIC ${CMAKE_CURRENT_LIST_DIR})

set(TEST_FILES)
file(GLOB TEST_FILES ./*.cc)

foreach(test_file_source ${TEST_FILES}) 
  get_filename_component(test_file_name ${test_file_source} NAME)
  if(${test_file_name} MATCHES "_test.cc$")
    string(REPLACE ".cc" "" test_exec_name ${test_file_name})
    add_executable(${test_exec_name} ${test_file_name})
    target_link_libraries(${test_exec_name} kv rcf ${LIBS} ${GTEST_BOTH_LIBRARIES})
  endif()
endforeach()
