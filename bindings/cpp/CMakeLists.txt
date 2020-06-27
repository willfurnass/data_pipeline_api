cmake_minimum_required(VERSION 3.12)

project(data_pipeline_cpp
  VERSION "0.1.0"
  LANGUAGES CXX)

###############################################
if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 14)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# clone only the branch, wihch conflicts with master 
# git clone -b cppbindings --single-branch git@github.com:ScottishCovidResponse/data_pipeline_api.git
# install all python modules , see Ian's Readme.md
# using system installed pybind11 ubuntu 20.04 and my mini CMakeLists.txt
# cmake .. -DPYTHON_EXECUTABLE:FILEPATH=$(which python3)
# export PYTHONPATH=$PWD/../../src:$PYTHONPATH
# run the test_datapipeline 

## put all targets in bin and lib
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
link_directories(${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

find_package(pybind11 QUIET)  # systemwide installation by package manager  `/usr/include/`
if(NOT pybind11_FOUND)
    message("===============================================================\n"
            "pybind11 not found on system, use the downloaded version \n"
            "===============================================================\n")
    add_subdirectory(pybind11)  # there is no need to find_package
    set(pybind11_FOUND ON)
endif()

#set(PYBIND11_INSTALL ON CACHE BOOL "")
#add_subdirectory(pybind11)

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