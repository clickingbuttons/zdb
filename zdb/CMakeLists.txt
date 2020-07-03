# CMakeList.txt : CMake project for zdb, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.10)
# We use std::variant (C++17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Until std::format (C++20)
find_package(fmt CONFIG REQUIRED)

# Add source to this project's executable.
include_directories(${PROJECT_SOURCE_DIR})
add_executable (zdb "zdb.cpp" "row.cpp" "schema.cpp" "table_write.cpp" "config.cpp"  "format.cpp" "variantrow.cpp" "log.h"  "table_read.cpp" "table_shared.cpp")
configure_file(zdb.conf zdb.conf COPYONLY)

target_link_libraries(zdb PRIVATE fmt::fmt)
