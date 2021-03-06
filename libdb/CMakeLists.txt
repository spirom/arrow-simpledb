
project(libdb)

MESSAGE(STATUS "Arrow root = " ${ARROW_ROOT})

find_package(Threads REQUIRED)

set(ARROW_LIBS libarrow.a)

include_directories(${ARROW_INCLIDE_DIRS})

link_directories(${ARROW_LINK_DIRS})

set(SOURCE_FILES
        columns/BaseColumnCursor.h
        columns/ChunkedColumnCursor.cpp
        columns/ChunkedDictColumnCursor.cpp
        tables/ScanTableCursor.cpp
        columns/GenericColumnCursor.cpp tables/TableCursor.cpp
        tables/FilterProjectTableCursor.cpp
        filters/Filter.h
        filters/GreaterThanFilter.cpp filters/GreaterThanFilter.h
        filters/LessThanFilter.cpp filters/LessThanFilter.h
        filters/AndFilter.cpp filters/AndFilter.h
        tables/DBTable.cpp tables/DBTable.h
        tables/DBColumnBuilder.cpp tables/DBColumnBuilder.h
        core/DBSchema.h core/DBSchema.cpp
        filters/IsNullFilter.cpp filters/IsNullFilter.h filters/NotFilter.cpp filters/NotFilter.h columns/BaseColumnCursor.cpp core/Status.h)

add_library(libdb STATIC ${SOURCE_FILES})

target_link_libraries(libdb ${ARROW_LIBS} ${CMAKE_THREAD_LIBS_INIT})

target_include_directories(libdb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})