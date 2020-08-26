CMAKE_MINIMUM_REQUIRED( VERSION 3.10 )

if( CMAKE_HOST_WIN32 )
    SET( PYEXE "python3.exe" )
    SET( PYCONFEXE "python3-config.exe" )
else()
    SET( PYEXE "python3" )
    SET( PYCONFEXE "python3-config" )
endif()

execute_process(
    COMMAND ${PYEXE} -c "import os; import pybind11; print(os.path.dirname(pybind11.__file__)+'/include', end='')"
    OUTPUT_VARIABLE PYBIND11LIBS
)

execute_process(
    COMMAND bash -c "echo \"$(git describe --dirty --always | cut -d '-' -f 1)\"| tr -d '[:space:]'"
    OUTPUT_VARIABLE SOFT_VERSION
)

execute_process(
    COMMAND bash -c "echo \"$(git remote show origin | grep Fetch | cut -d ' ' -f 5)\"| tr -d '[:space:]'"
    OUTPUT_VARIABLE GIT_REMOTE_URL
)

project( datapipeline VERSION 0.7.3 LANGUAGES CXX )

SET( CMAKE_CXX_STANDARD 17 )
SET( CMAKE_CXX_STANDARD_REQUIRED ON )
SET( CMAKE_THREAD_LIBS_INIT "-lpthread" )

SET( SRCFILES array.cc distributions.cc datapipeline.cc table.cc )
get_filename_component( BINDINGS_ROOT ${CMAKE_SOURCE_DIR} DIRECTORY )
get_filename_component( DATA_PIPELINE_ROOT ${BINDINGS_ROOT} DIRECTORY )
set( CMAKE_CXX_FLAGS "-DROOTDIR=\\\"${DATA_PIPELINE_ROOT}\\\" -DGIT_URL=\\\"${GIT_REMOTE_URL}\\\" -DVERSION=\\\"${SOFT_VERSION}\\\"" )

find_package(PythonLibs 3 REQUIRED)

MESSAGE(STATUS "----- SCRC API C++ Bindings -----")
MESSAGE(STATUS "[Setup]:")
MESSAGE(STATUS "\tVERSION: ${SOFT_VERSION}")
MESSAGE(STATUS "\tGIT_REMOTE_URL: ${GIT_REMOTE_URL}")
MESSAGE(STATUS "\tPYBIND11 LIBRARIES: ${PYBIND11LIBS}")
MESSAGE(STATUS "[Flags]:")
MESSAGE(STATUS "\t${CMAKE_CXX_FLAGS}")
MESSAGE(STATUS "\tPYTHONLIBS: ${PYTHON_INCLUDE_DIRS}")
MESSAGE(STATUS "---------------------------------")

add_library( ${PROJECT_NAME} STATIC ${SRCFILES} )
target_include_directories( ${PROJECT_NAME} PRIVATE ${CMAKE_SOURCE_DIR}
                            ${PROJECT_NAME} PUBLIC ${PYBIND11LIBS}
                            ${PROJECT_NAME} PUBLIC ${PYTHON_INCLUDE_DIRS}
                          )
target_link_libraries( ${PROJECT_NAME} ${PYTHON_LIBRARIES} )
set( CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin )

add_subdirectory(tests)
