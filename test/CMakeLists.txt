cmake_minimum_required(VERSION 3.8)
SET(CMAKE_CXX_STANDARD 17)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})


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

add_executable(server SampleServer.cc)
add_executable(client SampleClient.cc)

add_executable(async_server AsyncServer.cc)
add_executable(async_client AsyncClient.cc)

target_link_libraries(server rcf ${LIBS} GTest::gtest GTest::gtest_main)
target_link_libraries(client rcf ${LIBS} GTest::gtest GTest::gtest_main)

target_link_libraries(async_server rcf ${LIBS} GTest::gtest GTest::gtest_main)
target_link_libraries(async_client rcf ${LIBS} GTest::gtest GTest::gtest_main)




