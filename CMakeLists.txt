# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.8)

project ("zdb")

# We use std::variant (C++17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Something about PIE
# https://gcc.gnu.org/onlinedocs/gcc/Code-Gen-Options.html#Code-Gen-Options
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIE")

# Include deps
add_subdirectory ("deps/fmt")

# Include sub-projects.
add_subdirectory ("zdb")
# add_subdirectory ("bench")
