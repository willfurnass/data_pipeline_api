
# C++ bindings for Python Data Pipeline API

[![](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/ci-cppbindings/badge.svg?branch=cppbindings)](https://github.com/ScottishCovidResponse/data_pipeline_api/actions?query=workflow%3Aci-cppbindings)

This directory contains C++ bindings for the Python data pipeline API.

Temporay note:  this should be fixed very soon
clone only the branch, wihch conflicts with master 
`git clone -b cppbindings --single-branch git@github.com:ScottishCovidResponse/data_pipeline_api.git`


## Requirements

There are two ways to make sure you have a working Python with all the
required packages:

1. Manual: If you are familiar with Python, have Python3 installed,
   and are willing to edit the Makefile to add certain paths to
   installed packages if it doesn't work, just ensure you have the
   required packages installed, e.g. using pip3:
   ```
   pip3 install pybind11 pyyaml pandas scipy toml semver
   ```
   Add --user if needed.
2. Automatic: Giving up on any existing installation, we can build our
   own by running the provided script:
   ```
   cd data_pipeline_api/bindings/cpp
   ./install-python-stack $PWD/python
   ```
   Copy and paste the "export PATH=" line printed at the end to choose this version of Python.

## Run the Python example

To test that things are working so far, you should now be able to run the python pipeline API. 

The Python example:

```
cd data_pipeline_api
export PYTHONPATH=$PWD/src:$PYTHONPATH
python3 examples/data_access.py
```
This should output

```
    source   target    mixing
0   [0,17)   [0,17)  2.158825
1   [0,17)  [17,70)  1.642811
2   [0,17)      70+  0.095393
3  [17,70)   [0,17)  0.540818
4  [17,70)  [17,70)  2.291221
5  [17,70)      70+  0.175799
6      70+   [0,17)  0.036736
7      70+  [17,70)  0.274034
8      70+      70+  0.075217
```
(You might also get some YAML warnings.)

## Building the C++ wrapper

Dependencies: 
`apt install python3-dev python3-pybind11`  (ubuntu package name)
`pip3 install h5py` tested, `apt install python3-h5py` should also work.

### Unix Makefile
Now build the C++ test program:

```
cd bindings/cpp
make
```
## CMake

A CMakeLists.txt has been provided, it can detect system installation of pybind11, or download the latest pybind11 as a submodule. 

On Ubuntu 20.04, dependency `python3-dev python3-pybind11` is installed by `apt` instead of `pip3`

out of source build in a subfolder: `mkdir build && cd build`
`cmake .. -DPYTHON_EXECUTABLE:FILEPATH=$(which python3)`

On Ubuntu 18.04, it may need extra  option `PYTHON_LIBRARIES`
`cmake .. -DPYTHON_EXECUTABLE:FILEPATH=/usr/bin/python3 -DPYTHON_LIBRARIES=/usr/lib/x86_64-linux-gnu/libpython3.6m.so `. While, `ldd $(which python3)` can help to find the `PYTHON_LIBRARIES`

The the difference could be caused by version of pybind11/python3, or pip3/apt installation. 

To find PYTHON in conda/virtual environment. `cmake .. -DPYTHON_EXECUTABLE:FILEPATH=$(which python3)  -DPYTHON_LIBRARIES=??` should work. 

Todo: Need test out conda env, esp on MacOS.

https://github.com/jkhoogland/FindPythonAnaconda 


## Running the C++ wrapper

### Make **data_pipeline_api** on `PYTHONPATH`

In order to run the C++ test program, **python_pipeline_api/src**  python modules must be on `PYTHONPATH`, **data_pipeline_api** can be installed by `pip3 + git`  or conda (later). 

Without installation, just download/git-clone the repository, and  `export PYTHONPATH=path_to_data_pipeline_api_repo/data_pipeline_api:$PYTHONPATH`. Adjust the path depend on where **data_pipeline_api** repository directory locates.

### Run the tests
The test program for the wrapper can be run as:
```
./test_datapipeline
```

Todo: provide config_file_path as command line parameter, otherwise, default config path coded into the cpp may not work, depending where you run the test program.

It should run without producing an error (you might get warnings about
YAML) and output some data from the data repository.


### Notes on read_table() and write_table()

`Table` class is a column-major impl, columns are transposed into a group of HDF5 attributes and saved to hdf5 by `pandas.to_hdf()`.  The tabular data are not available in any HDF viewer.  `pandas.read_hdf()`.  Limited C++ scalar types, `double, int64_t, bool` are supported, but `std::string` is supported without using data pipeline API.

The python pipeline API, converts DataFrame into records then write to hdf5, data table are in tabular data format, but "std::string" as column data format are not supported.
> runtime error:  TypeError: Object dtype dtype('O') has no native HDF5 equivalent

There is another problem when writing table metadata, "get_write_group()" is not available in standard_api.py.

There is `RowTable<RowType>` class which can save any RowType data class as a row in HDF5 dataset, assisted by a code generator.  While this is not standard API, `write_rtable(std::vector<RowType>)` is not added yet.

## Notes for installation on DiRAC CSD3

(TODO: update to use the self-contained build above)

The python/3.6 module on CSD3 can be used, but it has no pip
available, and some of the packages appear to be broken, so it's best
to install pip3 and all required packages in ~/.local as follows:

```bash
. /etc/profile.d/modules.sh
module purge
module load python/3.6

# Bootstrap pip3
python -m ensurepip --user --upgrade

# Install required packages to ~/.local
pip3 install --user --upgrade pybind11 pyyaml pandas scipy toml
```

Then proceed as above.
