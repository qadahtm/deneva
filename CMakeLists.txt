cmake_minimum_required(VERSION 3.5)
project(expodb-qcd)

set(CMAKE_CXX_STANDARD 11)
#set(CMAKE_BUILD_TYPE RelWithDebInfo)
#set(CMAKE_BUILD_TYPE Debug)

macro (add_sources)
    file (RELATIVE_PATH _relPath "${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
    foreach (_src ${ARGN})
        if (_relPath)
            list (APPEND SRCS "${_relPath}/${_src}")
        else()
            list (APPEND SRCS "${_src}")
        endif()
    endforeach()
    if (_relPath)
        # propagate SRCS to parent directory
        set (SRCS ${SRCS} PARENT_SCOPE)
    endif()
endmacro()

# organize built files
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra -Werror -D NOGRAPHITE=1")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -D NOGRAPHITE=1")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")

include_directories(benchmarks)
include_directories(client)
include_directories(system)
include_directories(transport)
include_directories(concurrency_control)
include_directories(storage)
include_directories(transport)
include_directories(statistics)
include_directories(".")

add_subdirectory(benchmarks)
add_subdirectory(client)
add_subdirectory(concurrency_control)
add_subdirectory(storage)
add_subdirectory(statistics)
add_subdirectory(system)
add_subdirectory(transport)
add_subdirectory(scripts)

add_sources(
        config.cpp
)
if(APPLE)
    include_directories("/usr/local/Cellar/boost/1.67.0_1/include")
    include_directories("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/include/c++/v1")
    include_directories("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/8.1.0/include")

    include_directories("../deps/jemalloc_mac/include")
    include_directories("../deps/nanomsg_mac/include")
    link_directories(../deps/jemalloc_mac/lib)
    link_directories(../deps/nanomsg_mac/lib)
    link_directories("/usr/local/Cellar/boost/1.67.0_1/lib")

else()
    include_directories("../boost_1_63_0")
    include_directories("../deps/nanomsg-1.0.0/include")
    include_directories("../deps/jemalloc-4.5.0/include")

    link_directories(../deps/jemalloc-4.5.0/lib)
    link_directories(../deps/nanomsg-1.0.0/lib64)
    link_directories(../deps/nanomsg-1.0.0/lib)
endif()


add_library(expodb ${SRCS})

if(APPLE)
    target_link_libraries(expodb jemalloc nanomsg pthread boost_system-mt boost_thread-mt)
else()
    target_link_libraries(expodb jemalloc nanomsg pthread)
endif()

add_executable(rundb system/main.cpp)
target_link_libraries(rundb expodb)

add_executable(runcl client/client_main.cpp)
target_link_libraries(runcl expodb)