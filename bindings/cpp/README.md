
# C++ bindings for Python Data Pipeline API

[![](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/ci-cppbindings/badge.svg?branch=cppbindings)](https://github.com/ScottishCovidResponse/data_pipeline_api/actions?query=workflow%3Aci-cppbindings)![SCRC API C++ Bindings (CMake)](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/SCRC%20API%20C++%20Bindings%20(CMake)/badge.svg)

This directory contains C++ bindings for the Python data pipeline API.

(See [DiRAC CSD3](#DiRAC_CSD3) for specific notes on installing on that machine.)

## Requirements

- Python 3.7 or greater with a working python3-config

- Create and activate a virtual environment for the required packages from the top-level repository directory:
  ```
  python3 -m venv .venv
  source .venv/bin/activate
  ```

- Install the required packages:
  ```
  pip install -r bindings/cpp/requirements.txt
  ```
  
- Check that `data_pipeline_api` is working by running the tests,
  `pytest tests`, from the top-level repository directory.

## Building the C++ wrapper library

```
cd bindings/cpp
make
```

## Run the C++ wrapper tests

The test program for the wrapper can be run as:
```
make test
```

## Alternative build with CMake

Alternative to the above the library can also be built using CMake.

```
cmake -H. -Bbuild
cmake --build build
```

the tests for this method have been written using GTest and can be run using the created binary:

```
./build/bin/SCRCdataAPI-tests
```

## Machine-specific notes

### DiRAC CSD3

Follow the above instructions, but be sure to run
```
module load python/3.7
```
first. The default python does not have python3-config, which is required for the compilation.

## Developer notes

- We have not been successful in running with Conda. The problem
  appears to be related to the provided Python being compiled with a
  different compiler than is used to build the C++ bindings.

- requirements.txt can be manually updated from environment.yml with
  ```
  cd bindings/cpp
  make requirements.txt
  ```
  as long as a python with pyyaml is available. A better solution should probably be
  found.
