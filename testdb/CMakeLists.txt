project(testdb)

set(ARROW_INCLIDE_DIRS /home/spiro/GitRepos/arrow/cpp/src)
include_directories(BEFORE ${ARROW_INCLIDE_DIRS})
include_directories(/home/spiro/GitRepos/googletest/googletest/include)

link_directories(/home/spiro/GitRepos/googletest/build/googlemock/gtest)
link_directories(/home/spiro/GitRepos/arrow/cpp/release/release)

add_executable(testdb TableTest.cpp main.cpp Tables.cpp)

target_link_libraries(testdb libgtest.a libdb)

install(TARGETS testdb DESTINATION bin)