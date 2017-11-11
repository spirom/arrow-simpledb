
project(libdb)

find_package( Threads )


set(ARROW_INCLIDE_DIRS /home/spiro/GitRepos/arrow/cpp/src)
set(ARROW_LIBS libarrow.a)

include_directories(BEFORE ${ARROW_INCLIDE_DIRS})

link_directories(/home/spiro/GitRepos/arrow/cpp/release/release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES ChunkedColumnCursor.cpp ScanTableCursor.cpp
        ColumnCursorWrapper.cpp GenericColumnCursor.cpp TableCursor.cpp
        FilterProjectTableCursor.cpp filters/GreaterThanFilter.cpp filters/GreaterThanFilter.h filters/LessThanFilter.cpp filters/LessThanFilter.h filters/AndFilter.cpp filters/AndFilter.h)
add_library(libdb STATIC ${SOURCE_FILES})

target_link_libraries(libdb ${ARROW_LIBS} ${CMAKE_THREAD_LIBS_INIT})

target_include_directories(libdb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})