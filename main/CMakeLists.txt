
project(main)

find_package( Threads )

set(ARROW_INCLIDE_DIRS /home/spiro/GitRepos/arrow/cpp/src)
set(ARROW_LIBS libarrow.a)

include_directories(BEFORE ${ARROW_INCLIDE_DIRS})

link_directories(/home/spiro/GitRepos/arrow/cpp/release/release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(learning_arrow_cpp ${SOURCE_FILES})

target_link_libraries(learning_arrow_cpp ${ARROW_LIBS} ${CMAKE_THREAD_LIBS_INIT} libdb)