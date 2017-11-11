cmake_minimum_required(VERSION 3.3)
project(arrow_simpledb)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra -std=c++11 -pthread")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

ADD_SUBDIRECTORY(libdb)
ADD_SUBDIRECTORY(testdb)
ADD_SUBDIRECTORY(main)

