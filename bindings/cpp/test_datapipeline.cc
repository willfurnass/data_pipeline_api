#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

#include "table.hh"
#include "datapipeline.hh"

#include "array.h"

using namespace std;

const std::string TEST_HDF5_FILENAME = "test_table.h5";
const std::string TEST_HDF5_FILENAME2 = "test_table2.h5";
const std::string TEST_DATASET_NAME = "table_ds";


void test_dp_table(DataPipeline &dp)
{
  cout << "test_dp_table:" << endl;
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
  //
  Table h5table = dp.read_table(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME);
  cout << endl;
}

void test_dp_array(DataPipeline &dp)
{
  cout << "test_dp_array:" << endl;

  // make test input array data by py
  py::module np = py::module::import("numpy");
  typedef int64_t DT;
  // np.arange()  return the int64_t type? not double
  py::array a = np.attr("arange")(12); // implicitly downcast from py::object to py::array
  std::vector<size_t> _s = {3, 4};
  py::list s = py::cast(_s);
  py::array mat = np.attr("reshape")(a, s);
  // np.attr("ascontiguousarray")();  make no diff
  // Return a contiguous array (ndim >= 1) in memory (C order), x.flags['C_CONTIGUOUS']
  py::print(mat.dtype().kind(), mat.nbytes(), mat);

  // py::array to Array<T> bug !  first 2 rows correct, but third all zero
  ArrayT<DT>::Ptr ap = ArrayT<DT>::decode_array(mat);
  std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
  std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;
  ap->units() = "unknown";
  ap->dim_unit(0) = "second";
  ap->dim_unit(1) = "mm";
  ap->dim_values(0) = {1, 4};

  const std::string TEST_HDF5_DATAPRODUCT = "test_npy";
  // error: Unable to create link (name already exists)
  // when I create_dataset from the given group object,  it may be caused by
  // `get_write_group()`
  dp.write_array(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME, *ap);
  dp.read_array(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME);
  cout << endl;
}

int main(int argc, char *argv[])
{
  pybind11::scoped_interpreter guard{}; // start the interpreter and keep it alive

  // default path may works only for unix makefile
  // for CMake out of source build, provide path to config as the first cmd argument
  std::string config_path = "../../tests/data/config.yaml";
  if (argc > 1)
  {
    config_path = std::string(argv[1]);
  }
  DataPipeline dp(config_path);

#if 0 // CSV test data, outdated API may not work with latest python API
  // read_table
  Table table = dp.read_table("human/mixing-matrix");
  cout << "human/mixing-matrix:" << endl
       << table.to_string() << endl;
  vector<double> mixing = table.get_column<double>("mixing");
  cout << "mixing = [" << mixing.at(0) << "," << mixing.at(1) << ", ... ]" << endl;
#endif

//  test_dp_array(dp);
  test_dp_table(dp);

  std::cout << "data pipeline C++ api tested pass successfully\n";

  return 0;
}
