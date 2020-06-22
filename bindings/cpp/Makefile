
# CSD3
# PYTHON_CFLAGS := -I/home/dc-hind2/.local/include/python3.6m -I/usr/local/software/master/python/3.6/include/python3.6m -I/usr/local/software/master/python/3.6/include/python3.6m  -Wno-unused-result -Wsign-compare -fPIC -DNDEBUG -g -fwrapv -O3 -Wall
# PYTHON_LDFLAGS :=  -L/usr/local/software/master/python/3.6/lib -lpython3.6m -lintl -lpthread -ldl  -lutil -lm  -Xlinker -export-dynamic

PYTHON_EXTRA_CFLAGS := -I${HOME}/.local/include/python3.6m # for CSD3 only
PYTHON_EXTRA_LDFLAGS :=

PYTHON_CFLAGS := $(PYTHON_EXTRA_CFLAGS) $(shell python-config --cflags) -UNDEBUG
PYTHON_LDFLAGS := $(PYTHON_EXTRA_LDFLAGS) $(shell python-config --ldflags)

# Laptop
# PYTHON_CFLAGS = -I/Users/h52229ih/opt/miniconda3/envs/data_pipeline_api/include/python3.7m -I/Users/h52229ih/opt/miniconda3/envs/data_pipeline_api/include/python3.7m -Wno-unused-result -Wsign-compare -Wunreachable-code -DNDEBUG -g -fwrapv -O3 -Wall -Wstrict-prototypes -I/Users/h52229ih/opt/miniconda3/envs/data_pipeline_api/include -arch x86_64 -I/Users/h52229ih/opt/miniconda3/envs/data_pipeline_api/include -arch x86_64
# PYTHON_LDFLAGS = -L/Users/h52229ih/opt/miniconda3/envs/data_pipeline_api/lib/python3.7/config-3.7m-darwin -lpython3.7m -ldl -framework CoreFoundation
#CPPFLAGS = -I /usr/local/lib/python3.7/site-packages/pybind11/include -I /usr/local/Cellar/python/3.7.6_1/Frameworks/Python.framework/Versions/3.7/include/python3.7m

CXXFLAGS = -g -std=c++11 -Wall $(PYTHON_CFLAGS)

cppbindings: cppbindings.cc
	$(CXX) -o cppbindings $(PYTHON_LDFLAGS) $(CPPFLAGS) $(CXXFLAGS) cppbindings.cc table.cc datapipeline.cc
