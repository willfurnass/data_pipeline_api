cmake_minimum_required(VERSION 3.12)

project(data_pipeline_cpp
  VERSION "0.1.0"
  LANGUAGES CXX)

set(PYBIND11_INSTALL ON CACHE BOOL "")
add_subdirectory(pybind11)

set(DATA_PIPELINE_HEADERS table.hh datapipeline.hh)

add_library(data_pipeline_cpp
  table.cc datapipeline.cc
  ${DATA_PIPELINE_HEADERS})
target_link_libraries(data_pipeline_cpp PUBLIC pybind11::embed)

add_executable(test_datapipeline
  test_datapipeline.cc)
target_link_libraries(test_datapipeline data_pipeline_cpp)

##################################################
# Installation

include(GNUInstallDirs)
install(TARGETS data_pipeline_cpp
  EXPORT data_pipeline_cppTargets
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )
install(FILES ${DATA_PIPELINE_HEADERS} DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  data_pipeline_cppConfigVersion.cmake
  VERSION ${PACKAGE_VERSION}
  COMPATIBILITY SameMajorVersion
  )

# This is a workaround for pybind11 not currently installing its targets
# Definitely should be removed if that changes
install(EXPORT "pybind11Targets"
  NAMESPACE "pybind11::"
  DESTINATION ${PYBIND11_CMAKECONFIG_INSTALL_DIR})

install(EXPORT data_pipeline_cppTargets
  FILE data_pipeline_cppTargets.cmake
  NAMESPACE data_pipeline_cpp::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/data_pipeline_cpp"
  )

configure_package_config_file(data_pipeline_cppConfig.cmake.in data_pipeline_cppConfig.cmake
  INSTALL_DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/data_pipeline_cppConfig.cmake"
  )
install(
  FILES
      "${CMAKE_CURRENT_BINARY_DIR}/data_pipeline_cppConfig.cmake"
      "${CMAKE_CURRENT_BINARY_DIR}/data_pipeline_cppConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/data_pipeline_cpp"
  )
