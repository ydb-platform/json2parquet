cmake_minimum_required(VERSION 3.16)
project(json2parquet)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ARROW REQUIRED parquet)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(json2parquet main.cpp)
target_link_libraries(json2parquet ${ARROW_LIBRARIES})

