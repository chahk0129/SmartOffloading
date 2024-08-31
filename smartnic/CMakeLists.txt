project(smartnic_db)
cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)

# -----------------------------------------------
# Environment
# -----------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -pthread -Wno-deprecated-declarations")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -g" CACHE INTERNAL "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3" CACHE INTERNAL "compiler options" FORCE)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
    message("${BoldGreen}Release mode: ${ColourReset} " ${CMAKE_BUILD_TYPE})
endif()

find_package(Threads REQUIRED)
include(FindPkgConfig)
pkg_search_module(TBB REQUIRED tbb)
find_library(JEMALLOC_LIB jemalloc)

# -----------------------------------------------
# Includes
# -----------------------------------------------
include_directories(${PROJECT_SOURCE_DIR}) 
include_directories(${TBB_INCLUDE_DIRS})

# -----------------------------------------------
# Files
# -----------------------------------------------
file(GLOB_RECURSE SRC_FILES common/* benchmark/* concurrency/* storage/* index/* system/* net/* server/* client/* worker/*)
file(GLOB_RECURSE COMPUTE common/* benchmark/* storage/* net/* client/* worker/transport*)
file(GLOB_RECURSE SERVER common/* net/* server/*)
file(GLOB_RECURSE WORKER common/* benchmark/* storage/* net/* index/* worker/* concurrency/*)

# -----------------------------------------------
# Benchmark Tests --- YCSB
# -----------------------------------------------

add_executable(ycsb_compute test/ycsb_compute.cpp ${COMPUTE})
add_executable(ycsb_worker test/ycsb_worker.cpp ${WORKER})
add_executable(memory_server test/server.cpp ${SERVER})

target_compile_definitions(ycsb_compute PUBLIC -DYCSB)
target_compile_definitions(ycsb_worker PUBLIC -DYCSB)

target_link_libraries(ycsb_compute pthread ibverbs ${JEMALLOC_LIB})
target_link_libraries(ycsb_worker pthread ibverbs ${JEMALLOC_LIB} ${TBB_LIBRARIES})
target_link_libraries(memory_server pthread ibverbs ${JEMALLOC_LIB})


# -----------------------------------------------
# Benchmark Tests --- TPCC
# -----------------------------------------------

add_executable(tpcc_compute test/tpcc_compute.cpp ${COMPUTE})
add_executable(tpcc_worker test/tpcc_worker.cpp ${WORKER})

target_compile_definitions(tpcc_compute PUBLIC -DTPCC)
target_compile_definitions(tpcc_worker PUBLIC -DTPCC)

target_link_libraries(tpcc_compute pthread ibverbs ${JEMALLOC_LIB})
target_link_libraries(tpcc_worker pthread ibverbs ${JEMALLOC_LIB} ${TBB_LIBRARIES})
