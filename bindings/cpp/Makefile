
# Use this on CSD3.  Adapt it to point to your pip-installed packages if they cannot be found at compile time
#PYTHON_EXTRA_CFLAGS := -I${HOME}/.local/include/python3.6m -I/usr/local/lib/python3.7/site-packages/pybind11/include

# Use this if you have python 3.8
#PYTHON_EXTRA_LDFLAGS := #-lpython3.8

# Customise these using var=val arguments to make, if needed

PYTHON           := python3
PYTHON_CONFIG    := $(PYTHON)-config
AR               := ar

# Python-related flags

PYBIND11_INCLUDE := -I$(shell $(PYTHON) -c 'import os; import pybind11; print(os.path.dirname(pybind11.__file__)+"/include")')
PYTHON_CFLAGS    := $(PYTHON_EXTRA_CFLAGS) $(PYBIND11_INCLUDE) $(shell $(PYTHON_CONFIG) --cflags) -UNDEBUG -fPIC
PYTHON_LDFLAGS   := $(PYTHON_EXTRA_LDFLAGS) $(shell $(PYTHON_CONFIG) --ldflags)

TARGET_EXEC_NAMES := test_datapipeline test_local

BUILD_DIR := ./build
SRC_DIR := .

TARGET_LIB := $(BUILD_DIR)/libdatapipeline.a

LIB_SRC_NAMES := datapipeline.cc table.cc
EXE_SRC_NAMES := test_datapipeline.cc test_local.cc

LIB_SRCS := $(addprefix $(SRC_DIR)/,$(LIB_SRC_NAMES))
EXE_SRCS := $(addprefix $(SRC_DIR)/,$(EXE_SRC_NAMES))

SRCS := $(LIB_SRCS) $(EXE_SRCS)
LIB_OBJS := $(LIB_SRCS:%=$(BUILD_DIR)/%.o)
EXE_OBJS := $(EXE_SRCS:%=$(BUILD_DIR)/%.o)
OBJS := $(SRCS:%=$(BUILD_DIR)/%.o)
DEPS := $(OBJS:.o=.d)


TARGET_EXECS := $(addprefix $(BUILD_DIR)/,$(TARGET_EXEC_NAMES))

INC_DIRS := $(SRC_DIR)
INC_FLAGS := $(addprefix -I,$(INC_DIRS))


CXXFLAGS         := $(CXXFLAGS) -g -std=c++11 -Wno-attributes $(PYTHON_CFLAGS) -Wno-everything #-Wall

CPPFLAGS := $(CPPFLAGS) $(INC_FLAGS) -MMD -MP
LDFLAGS := $(PYTHON_LDFLAGS) $(LDFLAGS)

.PHONY : all

all: $(TARGET_EXECS)

$(TARGET_LIB): $(LIB_OBJS)
	$(MKDIR_P) $(dir $@)
	ar -crs $@ $(LIB_OBJS)

$(TARGET_EXECS): $(BUILD_DIR)/% : $(BUILD_DIR)/%.cc.o $(TARGET_LIB)
	$(MKDIR_P) $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.cc.o: %.cc
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

.PHONY: clean

clean:
	$(RM) -r $(BUILD_DIR)

-include $(DEPS)

MKDIR_P ?= mkdir -p
