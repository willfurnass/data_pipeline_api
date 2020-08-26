find_package(Threads REQUIRED)

include(ExternalProject)

ExternalProject_Add(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  UPDATE_COMMAND ""
  GIT_TAG release-1.10.0
  INSTALL_COMMAND ""
)

SET(TEST_NAME ${PROJECT_NAME}-tests)

# assume built-in pthreads on MacOS
IF(APPLE)
    set(CMAKE_THREAD_LIBS_INIT "-lpthread")
    set(CMAKE_HAVE_THREADS_LIBRARY 1)
    set(CMAKE_USE_WIN32_THREADS_INIT 0)
    set(CMAKE_USE_PTHREADS_INIT 1)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
ENDIF()

ExternalProject_Get_Property(googletest source_dir)
set(GTEST_INCLUDE_DIRS ${source_dir}/googletest/include)
set(GMOCK_INCLUDE_DIRS ${source_dir}/googlemock/include)

ExternalProject_Get_Property(googletest binary_dir)
set(GTEST_LIBRARY_PATH  ${binary_dir}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a)
set(GMOCK_LIBRARY_PATH ${binary_dir}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmock.a)
set(GTEST_MAIN_LIBRARY_PATH ${binary_dir}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_main.a)
set(GMOCK_MAIN_LIBRARY_PATH ${binary_dir}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmock_main.a)
set(GTEST_LIBRARY gtest)
set(GMOCK_LIBRARY gmock)
set(GTEST_MAIN_LIBRARY gtest_main)
set(GMOCK_MAIN_LIBRARY gmock_main)

add_library(${GTEST_LIBRARY} UNKNOWN IMPORTED)
add_library(${GMOCK_LIBRARY} UNKNOWN IMPORTED)
add_library(${GTEST_MAIN_LIBRARY} UNKNOWN IMPORTED)
add_library(${GMOCK_MAIN_LIBRARY} UNKNOWN IMPORTED)

set_target_properties(${GTEST_LIBRARY} PROPERTIES
    IMPORTED_LOCATION ${GTEST_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(${GMOCK_LIBRARY} PROPERTIES
    IMPORTED_LOCATION ${GMOCK_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(${GTEST_MAIN_LIBRARY} PROPERTIES
    IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(${GMOCK_MAIN_LIBRARY} PROPERTIES
    IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(${GTEST_LIBRARY} googletest)
add_dependencies(${GMOCK_LIBRARY} googletest)
add_dependencies(${GTEST_MAIN_LIBRARY} googletest)
add_dependencies(${GMOCK_MAIN_LIBRARY} googletest)

file(GLOB_RECURSE src_unit "test_*.cc")

add_executable(${TEST_NAME} ${src_unit})
add_compile_options(${TEST_NAME} ${BUILD_TYPE_COMPILE_FLAGS})

target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}
                           ${TEST_NAME} PUBLIC ${PYBIND11LIBS}
                           ${TEST_NAME} PUBLIC ${PYTHON_INCLUDE_DIRS}
                           ${TEST_NAME} PRIVATE ${GTEST_INCLUDE_DIRS}
                           ${TEST_NAME} PRIVATE ${GMOCK_INCLUDE_DIRS})

MESSAGE(STATUS "----- Configuring Test Build -----")
MESSAGE(STATUS "[Libraries]:")
MESSAGE(STATUS "\tAPI C++ BINDINGS: ${CMAKE_SOURCE_DIR}")
MESSAGE(STATUS "\tGTEST: ${GTEST_INCLUDE_DIRS}")
MESSAGE(STATUS "\tGMOCK: ${GMOCK_INCLUDE_DIRS}")
MESSAGE(STATUS "\tFLAGS: ${CMAKE_CXX_FLAGS}")
MESSAGE(STATUS "----------------------------------")

target_link_libraries(${TEST_NAME} PRIVATE ${PROJECT_NAME} ${PROJECT_LIBS} ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})

include(GoogleTest)
gtest_discover_tests(${TEST_NAME})