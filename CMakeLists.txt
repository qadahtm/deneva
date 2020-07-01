cmake_minimum_required(VERSION 3.5)
project(expodb-qcd)

set(CMAKE_CXX_STANDARD 11)
#set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_BUILD_TYPE Debug)

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

# Additional libraries
# GoogleTest

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
    include_directories("${gtest_SOURCE_DIR}/include")
endif()


#Yaml parser


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
include_directories(util)
include_directories(paxos)

add_subdirectory(benchmarks)
add_subdirectory(client)
add_subdirectory(concurrency_control)
add_subdirectory(storage)
add_subdirectory(statistics)
add_subdirectory(system)
add_subdirectory(transport)
add_subdirectory(scripts)
add_subdirectory(util)
add_subdirectory(paxos)
add_subdirectory(conf)
#Tests
add_subdirectory(test)

add_sources(
        config.cpp
)
#Important note:
# When running on Ubuntu 18.04 we need to install dependent packages via apt/apt-get using the following command (See INSTALL.md for details).
# $ sudo apt-get install -y cmake libboost-atomic-dev libnuma-dev libyaml-dev libnanomsg-dev libjemalloc-dev python3


if(APPLE)
    include_directories("/usr/local/Cellar/boost/1.67.0_1/include")
    include_directories("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/include/c++/v1")
    include_directories("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/8.1.0/include")
    include_directories(../deps/zookeeper_mac/include)

    include_directories("../deps/jemalloc_mac/include")
    include_directories("../deps/nanomsg_mac/include")
    link_directories(../deps/jemalloc_mac/lib)
    link_directories(../deps/nanomsg_mac/lib)
    link_directories("/usr/local/Cellar/boost/1.67.0_1/lib")

endif()


add_library(expodb ${SRCS})

if(APPLE)
    target_link_libraries(expodb jemalloc nanomsg pthread boost_system-mt boost_thread-mt)
else()
    target_link_libraries(expodb jemalloc nanomsg pthread)
endif()

# Copy scripts
configure_file(run_local_test.sh run_local_test.sh COPYONLY)
configure_file(start_node.py start_node.py COPYONLY)
configure_file(ifconfig.txt ifconfig.txt COPYONLY)


#using libyaml installed by apt-get on host
target_link_libraries(expodb yaml)

#using libyaml installed by apt-get on host
target_link_libraries(expodb yaml-cpp)


#zookeeper
target_link_libraries(expodb zookeeper_mt)

add_executable(rundb system/main.cpp)
target_link_libraries(rundb expodb)

add_executable(runcl client/client_main.cpp)
target_link_libraries(runcl expodb)

#Test modules
add_executable(basic_tests test/basic_tests.cpp test/paxos_test.cpp)
target_link_libraries(basic_tests gtest_main)
target_link_libraries(basic_tests expodb)

add_executable(config_yaml_test test/yaml_test.cpp)
target_link_libraries(config_yaml_test gtest_main)
target_link_libraries(config_yaml_test expodb)