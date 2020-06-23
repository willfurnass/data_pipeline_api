
# C++ bindings for Python Data Pipeline API

This directory contains C++ bindings for the Python data pipeline API.

Ensure that you have the required Python packages installed, for
example using pip:

```
pip3 install --user pybind11 pyyaml pandas scipy toml
```

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
