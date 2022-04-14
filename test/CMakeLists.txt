cmake_minimum_required(VERSION 3.8)
SET(CMAKE_CXX_STANDARD 17)

add_executable(server SampleServer.cc)
add_executable(client SampleClient.cc)

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

target_link_libraries(server rfc ${LIBS})
target_link_libraries(server rfc ${LIBS})


