cmake_minimum_required(VERSION 3.8)
project(raft)

SET(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR}/RCF/include)
add_subdirectory(${CMAKE_SOURCE_DIR}/RCF)

include_directories(${CMAKE_SOURCE_DIR}/src)
add_subdirectory(${CMAKE_SOURCE_DIR}/src)
add_subdirectory(${CMAKE_SOURCE_DIR}/test)
add_subdirectory(${CMAKE_SOURCE_DIR}/bench)



