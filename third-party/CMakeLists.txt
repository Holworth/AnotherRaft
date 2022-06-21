cmake_minimum_required(VERSION 3.8)

SET(CMAKE_CXX_STANDARD 17)
SET(LEVELDB_DIR GLOB ${CMAKE_SOURCE_DIR}/leveldb)
include_directories(${LEVELDB_DIR}/include)
