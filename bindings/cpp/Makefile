
# Use this on CSD3.  Adapt it to point to your pip-installed packages if they cannot be found at compile time
#PYTHON_EXTRA_CFLAGS := -I${HOME}/.local/include/python3.6m -I/usr/local/lib/python3.7/site-packages/pybind11/include

# Use this if you have python 3.8
#PYTHON_EXTRA_LDFLAGS := #-lpython3.8

PYBIND11_INCLUDE := -I$(shell python3 -c 'import os; import pybind11; print(os.path.dirname(pybind11.__file__)+"/include")')

$(info PYBIND11_INCLUDE = $(PYBIND11_INCLUDE))

PYTHON_CFLAGS := $(PYTHON_EXTRA_CFLAGS) $(PYBIND11_INCLUDE) $(shell python3-config --cflags) -UNDEBUG -fPIC
PYTHON_LDFLAGS := $(PYTHON_EXTRA_LDFLAGS) $(shell python3-config --ldflags)

CXXFLAGS = -g -std=c++11 -Wall -Wno-attributes $(PYTHON_CFLAGS)

test_datapipeline: test_datapipeline.cc table.cc datapipeline.cc
	$(CXX) -o test_datapipeline $(CPPFLAGS) $(CXXFLAGS) test_datapipeline.cc table.cc datapipeline.cc $(PYTHON_LDFLAGS)
