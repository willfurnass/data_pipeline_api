
# CSD3
# PYTHON_CFLAGS := -I/home/dc-hind2/.local/include/python3.6m -I/usr/local/software/master/python/3.6/include/python3.6m -I/usr/local/software/master/python/3.6/include/python3.6m  -Wno-unused-result -Wsign-compare -fPIC -DNDEBUG -g -fwrapv -O3 -Wall
# PYTHON_LDFLAGS :=  -L/usr/local/software/master/python/3.6/lib -lpython3.6m -lintl -lpthread -ldl  -lutil -lm  -Xlinker -export-dynamic

# It's not clear how to determine these automatically
PYTHON_EXTRA_CFLAGS := -I${HOME}/.local/include/python3.6m -I/usr/local/lib/python3.7/site-packages/pybind11/include

PYTHON_EXTRA_LDFLAGS :=

PYTHON_CFLAGS := $(PYTHON_EXTRA_CFLAGS) $(shell python3-config --cflags) -UNDEBUG
PYTHON_LDFLAGS := $(PYTHON_EXTRA_LDFLAGS) $(shell python3-config --ldflags)

CXXFLAGS = -g -std=c++11 -Wall -Wno-attributes $(PYTHON_CFLAGS)

test_datapipeline: test_datapipeline.cc
	$(CXX) -o test_datapipeline $(PYTHON_LDFLAGS) $(CPPFLAGS) $(CXXFLAGS) test_datapipeline.cc table.cc datapipeline.cc
