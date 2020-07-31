
# C++ bindings for Python Data Pipeline API

[![](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/ci-cppbindings/badge.svg?branch=cppbindings)](https://github.com/ScottishCovidResponse/data_pipeline_api/actions?query=workflow%3Aci-cppbindings)

This directory contains C++ bindings for the Python data pipeline API.

(See [DiRAC CSD3](#DiRAC_CSD3) for specific notes on installing on that machine.)

## Requirements

- Python 3 with a working python3-config

- Create and activate a virtual environment for the required packages from the top-level repository directory:
  ```
  python3 -m venv .venv
  source .venv/bin/activate
  ```

- Install the required packages:
  ```
  pip install networkx matplotlib pandas toml h5py scipy pyyaml semver fsspec s3fs click requests paramiko coverage pytest pytest-cov aiohttp pybind11
  ```
  
- Note: we have not been successful in running with Conda. The problem
  appears to be related to the provided Python being compiled with a
  different compiler than is used to build the C++ bindings.

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

## DiRAC CSD3

Follow the above instructions, but be sure to run
```
module load python/3.6
```
first. The default python does not have python3-config, which is required for the compilation.
