# CMakeList.txt : CMake project for zdb, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.10)
# We use std::variant (C++17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Add source to this project's executable.
include_directories(${PROJECT_SOURCE_DIR})
add_executable (zdb "zdb.cpp" "row.cpp" "schema.cpp" "table.cpp" "config.cpp"  "time.cpp")
configure_file(zdb.conf zdb.conf COPYONLY)

# TODO: Add tests and install targets if needed.
