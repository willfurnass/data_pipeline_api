
# C++ bindings for Python Data Pipeline API

[![](https://github.com/ScottishCovidResponse/data_pipeline_api/workflows/ci-cppbindings/badge.svg?branch=cppbindings)](https://github.com/ScottishCovidResponse/data_pipeline_api/actions?query=workflow%3Aci-cppbindings)

This directory contains C++ bindings for the Python data pipeline API.

There are two ways to make sure you have a working Python with all the
required packages:

1. Manual: If you are familiar with Python, have Python3 installed,
   and are willing to edit the Makefile to add certain paths to
   installed packages if it doesn't work, just ensure you have the
   required packages installed, e.g. using pip3:
   ```
   pip3 install pybind11 pyyaml pandas scipy toml
   ```
   Add --user if needed.
2. Automatic: Giving up on any existing installation, we can build our
   own by running the provided script:
   ```
   cd data_pipeline_api/bindings/cpp
   ./install-python-stack $PWD/python
   ```
   Copy and paste the "export PATH=" line printed at the end to choose this version of Python.

You should now be able to run the Python example:

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

Now build the C++ test program:

```
cd bindings/cpp
make
./test_datapipeline
```

If you get errors about pybind11/embed.h being missing, edit the
Makefile to add the path to the required include directory installed
by pip3.  It doesn't appear to be possible to determine this
automatically.

## Notes for installation on DiRAC CSD3

(TODO: update to use the self-contained build above)

The python/3.6 module on CSD3 can be used, but it has no pip
available, and some of the packages appear to be broken, so it's best
to install pip3 and all required packages in ~/.local as follows:

```
. /etc/profile.d/modules.sh
module purge
module load python/3.6

# Bootstrap pip3
python -m ensurepip --user --upgrade

# Install required packages to ~/.local
pip3 install --user --upgrade pybind11 pyyaml pandas scipy toml
```

Then proceed as above.
