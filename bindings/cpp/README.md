
# C++ bindings for Python Data Pipeline API

[![](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/ci-cppbindings/badge.svg?branch=cppbindings)](https://github.com/ScottishCovidResponse/data_pipeline_api/actions?query=workflow%3Aci-cppbindings)

This directory contains C++ bindings for the Python data pipeline API.

NOTE: if you have done single-branch, clone, there is some troubles to rebase
you can either redo the git clone, or fix it by <https://stackoverflow.com/questions/17714159/how-do-i-undo-a-single-branch-clone>

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
export PYTHONPATH=$PWD/datapipeline:$PYTHONPATH
python3 tests/data_access.py
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

There is another example: `python3 tests/standard_api_usage.py`

## Building the C++ wrapper

Dependencies: 
`apt install python3-dev python3-pybind11`  (ubuntu package name)
`pip3 install h5py` tested, `apt install python3-h5py` should also work.

Choose one of the build methods: Unix Makefile or CMake

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
```bash
cmake .. -DPYTHON_EXECUTABLE:FILEPATH=$(which python3)
make -j2
# the path to the config yaml file as the first argument to test_datapipeline
./bin/test_datapipeline  ../../../tests/data/config.yaml
```

On Ubuntu 18.04, it may need extra  option `PYTHON_LIBRARIES`
`cmake .. -DPYTHON_EXECUTABLE:FILEPATH=/usr/bin/python3 -DPYTHON_LIBRARIES=/usr/lib/x86_64-linux-gnu/libpython3.6m.so `. While, `ldd $(which python3)` can help to find the `PYTHON_LIBRARIES`

The the difference could be caused by version of pybind11/python3, or pip3/apt installation. 

To find PYTHON in conda/virtual environment. `cmake .. -DPYTHON_EXECUTABLE:FILEPATH=$(which python3)  -DPYTHON_LIBRARIES=??` should work. 

Todo: Need test out conda env, esp on MacOS.

https://github.com/jkhoogland/FindPythonAnaconda 


## Running the C++ wrapper

### Make **data_pipeline_api** on `PYTHONPATH`

In order to run the C++ test program, **python_pipeline_api**  python modules must be on `PYTHONPATH`, **data_pipeline_api(repo path)** can be installed by `pip3 + git`  or conda (later). 

Without installation, just download/git-clone the repository, and  `export PYTHONPATH=path_to_data_pipeline_api_repo/data_pipeline_api:$PYTHONPATH`. Adjust the path depend on where **data_pipeline_api** repository directory locates.

NOTE:  **python_pipeline_api** is the repo name/path, it has a folder called **python_pipeline_api** contains all *.py files. set `PYTHONPATH` to **data_pipeline_api(repo path)**. 

### Run the tests
The test program for the wrapper can be run as:
```bash
# may provide the config file path as the argument,  
# `./test_datapipeline  path_to_config_file`
# otherwise, error message `can not parse the yaml file` may appear
# if hard-code config file relative path is not correctly located, 
# depends on where is the working directory
./test_datapipeline
```

Todo: provide config_file_path as command line parameter, otherwise, default config path coded into the cpp may not work, depending where you run the test program.

It should run without producing an error (you might get warnings about
YAML) and output some data from the data repository.


## Documentation

"test_datapipeline.cpp" have some demo to construct Array and Table class instance in C++, model developer may have a look.

### Table creation

```cpp
  const std::string TEST_HDF5_DATAPRODUCT = "test_cpp_data"; // folder name, not filename
  Table table;
  table.add_column<int64_t>("int", {1, 2});
  table.add_column<double>("double", {1.1, 2.2});
  table.add_column<bool>("bool", {true, false});
  // runtime error:  TypeError: Object dtype dtype('O') has no native HDF5 equivalent
  // because DataFrame is converted to_records() in write_table()
  //table.add_column<std::string>("str", {"str1", "str2"});

  table.set_column_units({"unit1", "unit2", "unit3"});
  dp.write_table(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME, table);
```

### Notes on read_table() and write_table()

`Table` class is a column-major impl, columns are transposed into a group of HDF5 attributes and saved to hdf5 by `pandas.to_hdf()`.  The tabular data are not available in any HDF viewer.  `pandas.read_hdf()`.  

The python pipeline API, converts DataFrame into records then write to hdf5, data table are kept in tabular data format, but `std::string` as column data format is not supported.
> runtime error:  TypeError: Object dtype dtype('O') has no native HDF5 equivalent

```py
def write_table(file: IOBase, component: str, table: Table):
    records = table.to_records(index=False)
    get_write_group(file, component).require_dataset(
        "table", shape=records.shape, dtype=records.dtype, data=records
    )
```

Column types are limited to C++ scalar types, `double, int64_t, bool` are supported, because R, Python only support these 2 scalar types. `std::string` is supported without using data pipeline API.


There is another problem when writing table metadata, "get_write_group()" is not available in standard_api.py.

There is `RowTable<RowType>` class which can save any RowType data class as a row in HDF5 dataset, assisted by a code generator.  While this is not standard API, `write_rtable(std::vector<RowType>)` is not added yet.

### Array creation

Demo of Array creation is inside `test_datapipeline.cc` such as `create_array<T>()`. The most common constructor is: 
`ArrayT(const std::vector<size_t> _shape, const std::vector<T> & flattened_vector)`

Element types can be any numpy.array supported int and floating pointer scalar types, but it is still recommended to use `double, int64_t, bool` only, to be compatible with other programming languages.

NOTE: use `BoolArray`, instead of `ArrayT<bool>`,  Reasons
+ `std::vector<bool>` is a specialized std::vector<T>, each element use a bit not a byte
+ left reference to `std::vector<bool>` element will not compile, such as `T& operator []`
+ HDF5 C-API, save bool as unsigned byte, while h5py can save bool as ENUM
http://docs.h5py.org/en/stable/config.html

Array<std::string> is possible but yet implemented by template specialization the `decode_array() encode_array()` method, see example how  dimension names are encoded and decoded in `decode_metadata() encode_metadata()` of `array.h`.

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
