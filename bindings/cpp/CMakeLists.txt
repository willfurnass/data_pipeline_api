cmake_minimum_required(VERSION 3.12)

project(data_pipeline_cpp
  VERSION "0.1.0"
  LANGUAGES CXX)


option(ENABLE_HDF5 "Enable HDF5 C++ support (use H5Cpp.h)" OFF)
option(ENABLE_HDF5_GENERATOR "Enable python genetor to gen HDF5 types" OFF)
option(ENABLE_TOML "Enable toml configuraton file support" OFF)

###############################################
if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 14)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#message(STUTAS  "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
#message(STUTAS  "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")


## put all targets in bin and lib
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
link_directories(${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

################################################

include_directories("${PROJECT_SOURCE_DIR}/third-party/")
find_package(pybind11 QUIET)  # systemwide installation by package manager  `/usr/include/`
if(NOT pybind11_FOUND)
    message("===============================================================\n"
            "pybind11 not found on system, use the downloaded version \n"
            "===============================================================\n")
    
    if( NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third-party/pybind11")
    execute_process(
        COMMAND git submodule add https://github.com/pybind/pybind11
        WORKING_DIRECTORY {PROJECT_SOURCE_DIR}/third-party
    )
    endif()
    
    # not tested, maybe tested in docker later
    add_subdirectory(third-party/pybind11)  # no need to find_package, include_directories
    #include_directories("${PROJECT_SOURCE_DIR}/third-party/pybind11/include")
    set(pybind11_FOUND ON)
endif()

######################################################################
set(DATA_PIPELINE_HEADERS table.hh datapipeline.hh)

add_library(data_pipeline_cpp
  table.cc datapipeline.cc
  ${DATA_PIPELINE_HEADERS})
target_link_libraries(data_pipeline_cpp PUBLIC pybind11::embed)

add_executable(test_datapipeline
  test_datapipeline.cc)
target_link_libraries(test_datapipeline data_pipeline_cpp)

#
add_executable(test_array  test_array.cpp)
target_link_libraries(test_array PUBLIC pybind11::embed)

foreach(MyTarget test_array data_pipeline_cpp test_datapipeline)
    # Let's set compiler-specific flags
    if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
        target_compile_options(${MyTarget} PRIVATE -fvisibility=hidden)
    elseif (${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
        target_compile_options(${MyTarget} PRIVATE -fvisibility=hidden)
    elseif(${CMAKE_CXX_COMPILER_ID} STREQUAL "MSVC")
        target_compile_definitions(${MyTarget} PRIVATE WIN_EXPORT)
    endif()
endforeach()