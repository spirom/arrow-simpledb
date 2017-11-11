project(testdb)

set(GTEST_ROOT "" CACHE PATH "GoogleTest root directory")

include_directories(${ARROW_INCLIDE_DIRS})
include_directories(${GTEST_ROOT}/googletest/include)

link_directories(${GTEST_ROOT}/build/googlemock/gtest)
link_directories(${ARROW_LINK_DIRS})

add_executable(testdb TableTest.cpp main.cpp Tables.cpp)

target_link_libraries(testdb libgtest.a libdb)

install(TARGETS testdb DESTINATION bin)