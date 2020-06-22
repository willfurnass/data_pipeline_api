# C++ bindings for python data api

## With pip

Procedure on CSD3:

```
. /etc/profile.d/modules.sh
module purge
#module load rhel7/default-peta4
module load python/3.6
```

Install required packages with pip:

```
# Get a pip3, as there is none in this python installation
python -m ensurepip --user --upgrade

# Install required packages to ~/.local
pip3 install --user --upgrade pybind11 pyyaml pandas scipy toml

# Run the example pipeline script
cd data_pipeline_api
export PYTHONPATH=$PWD/src:$PYTHONPATH
python examples/data_access.py
```
This should output

```
   columnA  columnB
0  goodbye        2
1    hello        4
0.2
```

```
cd bindings/cpp
make
export PYTHONPATH=$PWD/../../src:$PYTHONPATH
./cppbindings
```
You should get
```
Hello, World!
Hello, World! The answer is 42
   columnA  columnB
0  goodbye        2
1    hello        4
0.2
```




## With miniconda

(I couldn't get this to work, when trying it on macOS, but I'm leaving
notes here in case it can be fixed in future.)

Notes:

- Install miniconda 
- Create an env for the project and activate it
- Install pyyaml pandas package
- Determine `PYTHON_CFLAGS` and `PYTHON_LDFLAGS` from python3-config.
- Make the executable
- Set PYTHONHOME to the miniconda3 directory
- Run the executable
