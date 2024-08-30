project(distributed_db)
cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)

# -----------------------------------------------
# Environment
# -----------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -pthread -Wno-deprecated-declarations")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g" CACHE INTERNAL "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3" CACHE INTERNAL "compiler options" FORCE)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
    message("${BoldGreen}Release mode: ${ColourReset} " ${CMAKE_BUILD_TYPE})
endif()

find_package(Threads REQUIRED)

# -----------------------------------------------
# Includes
# -----------------------------------------------
include_directories(${PROJECT_SOURCE_DIR}) 
find_library(JEMALLOC_LIB jemalloc)

# -----------------------------------------------
# Files
# -----------------------------------------------
file(GLOB_RECURSE SRC_FILES common/* benchmark/* concurrency/* storage/* index/* system/* net/* server/* client/*)
file(GLOB_RECURSE INDEX common/* net/* index/* client/*)
file(GLOB_RECURSE CLIENT common/* benchmark/* concurrency/* system/* storage/* net/* index/* client/*)
file(GLOB_RECURSE SERVER common/* benchmark/* concurrency/* system/* storage/* net/* index/* server/*)

#add_definitions(-DNOWAIT)
#add_definitions(-DWAITDIE)

add_executable(idx_test test/idx_wrapper.cpp ${INDEX}) 
add_executable(ycsb_compute test/ycsb_compute.cpp ${SRC_FILES})
add_executable(tpcc_compute test/tpcc_compute.cpp ${SRC_FILES})
add_executable(memory_server test/server.cpp ${SRC_FILES})

target_compile_definitions(ycsb_compute PUBLIC -DYCSB)
target_compile_definitions(tpcc_compute PUBLIC -DTPCC)

target_link_libraries(idx_test pthread ibverbs ${JEMALLOC_LIB})
target_link_libraries(ycsb_compute pthread ibverbs ${JEMALLOC_LIB})
target_link_libraries(tpcc_compute pthread ibverbs ${JEMALLOC_LIB})
target_link_libraries(memory_server pthread ibverbs ${JEMALLOC_LIB})
