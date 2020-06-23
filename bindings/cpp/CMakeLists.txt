cmake_minimum_required(VERSION 3.12)

project(data_pipeline_cpp
  LANGUAGES CXX)

add_subdirectory(pybind11)

add_library(data_pipeline_cpp
  table.cc datapipeline.cc
  table.hh datapipeline.hh)
target_link_libraries(data_pipeline_cpp PUBLIC pybind11::embed)

add_executable(test_datapipeline
  test_datapipeline.cc)
target_link_libraries(test_datapipeline data_pipeline_cpp)
