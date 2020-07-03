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

namespace local
{
  /// use h5py to open file and create a H5Group PyObject
  void write_array(const string &data_product, const string &component, const Array &da)
  {
    py::module h5py = py::module::import("h5py");
    auto h5file = h5py.attr("File")(data_product, "w");

    auto group = h5file.attr("create_group")(component);
    da.encode(group);

    // no need to close dataset?
    h5file.attr("close")();
  }

  template <typename DT>
  typename ArrayT<DT>::Ptr read_array_T(const string &data_product, const string &component)
  {
    py::module h5py = py::module::import("h5py");
    auto h5file = h5py.attr("File")(data_product, "r");

    py::object group = h5file[py::str(component)];

    typename ArrayT<DT>::Ptr ap = ArrayT<DT>::decode(group);

    // no need to close dataset?
    h5file.attr("close")();
    return ap;
  }

  ArrayT<int64_t>::Ptr create_int64_array()
  {
    typedef int64_t DT;

    // make test input array data by pybind11
    py::module np = py::module::import("numpy");
    // np.arange()  return the int64_t type? not double
    py::array a = np.attr("arange")(12); // implicitly downcast from py::object to py::array
    std::vector<size_t> _s = {3, 4};
    py::list s = py::cast(_s);
    py::array mat = np.attr("reshape")(a, s);
    // np.attr("ascontiguousarray")();  make no diff
    // Return a contiguous array (ndim >= 1) in memory (C order), x.flags['C_CONTIGUOUS']
    py::print(mat.dtype().kind(), mat.nbytes(), mat);

    ArrayT<DT>::Ptr ap = ArrayT<DT>::decode_array(mat);
    std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
    std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;
    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

  template <typename DT>
  typename ArrayT<DT>::Ptr create_array()
  {
    std::vector<DT> values(12);
    ShapeType s = {3, 4};
    typename ArrayT<DT>::Ptr ap = std::make_shared<ArrayT<DT>>(s, values);

    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

  BoolArray::Ptr create_bool_array()
  {
    typedef uint8_t DT;

    ShapeType s = {3, 4};
    std::vector<DT> cbools(12); // HDF5 save bool as unsigned byte
    //BoolArray::Ptr ap = new BoolArray(s, cbools);
    BoolArray::Ptr ap = std::make_shared<BoolArray>(s, cbools);

    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

  Table read_table(const string &data_product, const string &component)
  {

    Table table;

    py::module pd = py::module::import("pandas");
    py::object dataframe = pd.attr("read_hdf")(data_product, component);

    vector<string> colnames = dataframe.attr("columns").attr("tolist")().cast<vector<string>>();

    for (const auto &colname : colnames)
    {

      string dtype = py::str(dataframe.attr("dtypes").attr(colname.c_str()));

      if (dtype == "float64")
      {
        vector<double> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<double>>();
        table.add_column<double>(colname, values);
      }
      else if (dtype == "int64")
      {
        vector<int64_t> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<int64_t>>();
        table.add_column<int64_t>(colname, values);
      }
      else if (dtype == "bool")
      {
        vector<bool> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<bool>>();
        table.add_column<bool>(colname, values);
      }
      else if (dtype == "string" || dtype == "object") // tested working
      {
        vector<std::string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<std::string>>();
        table.add_column<std::string>(colname, values);
      }
      else
      {
        cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;
        vector<string> values;
        py::list l = dataframe[colname.c_str()].attr("tolist")();
        for (const auto &it : l)
        {
          values.push_back(py::str(it));
        }
        table.add_column<string>(colname, values);
      }
    }

    return table;
  }

  void write_table(const string &data_product, const string &component,
                   const Table &table)
  {
    map<string, py::array> _map; // pybind automatically recognises a map as a dict

    for (const auto &colname : table.get_column_names())
    {

      string dtype = table.get_column_type(colname);
      py::list l;
      if (dtype == "float64")
      {
        l = py::cast(table.get_column<double>(colname));
      }
      else if (dtype == "int64")
      {
        l = py::cast(table.get_column<int64_t>(colname));
      }
      else if (dtype == "bool")
      {
        l = py::cast(table.get_column<bool>(colname));
      }
      else if (dtype == "string" || dtype == "object")
      {
        l = py::cast(table.get_column<std::string>(colname));
      }
      else
      {
        cout << "WARNING: skip column " << colname << " from unsupported type " << dtype << endl;
      }
      _map[colname] = l;
    }

    py::module pd = py::module::import("pandas");
    py::object _df = pd.attr("DataFrame")(_map);
    _df.attr("to_hdf")(data_product, component);
    //api.attr("write_table")(data_product, estc_df);
  }

} // namespace local

void test_array_local()
{
  // current write file in truncate mode, existing file will be moved
  const std::string TEST_ARRAY_FILENAME = "test_npy.h5";
  const std::string TEST_ARRAY_FILENAME1 = "test_double_array.h5";
  const std::string TEST_ARRAY_FILENAME2 = "test_bool_array.h5";
  const char *TEST_ARRAY_COMPONENT_NAME = "nparray";

  auto ip = local::create_int64_array();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME, "int64array", *ip);
  local::read_array_T<int64_t>(TEST_ARRAY_FILENAME, "int64array");

  auto dp = local::create_array<double>();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME1, "float64array", *dp);
  local::read_array_T<double>(TEST_ARRAY_FILENAME1, "float64array");

  auto bp = local::create_bool_array();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME2, "boolarray", *bp);
  /// NOTE:  can not use BoolArray, specialized type, not impl yet
  //local::read_bool_array(TEST_ARRAY_FILENAME2, "boolarray");
}

/// test without data pipeline, test passed for std::string as column type
void test_table_local()
{
  py::exec("import pandas as pd\n"
           "df = pd.DataFrame([[1, 1.1, True, 'str1'], [2, 2.2,  False, 'str2']], columns=['x', 'y', 'z', 's'])\n"
           "df.to_hdf('" +
           TEST_HDF5_FILENAME + "', '" + TEST_DATASET_NAME + "')\n");

  Table h5table = local::read_table(TEST_HDF5_FILENAME, TEST_DATASET_NAME);
  h5table.set_column_units({"unit1", "unit2", "unit3", "unit4"});
  local::write_table(TEST_HDF5_FILENAME2, TEST_DATASET_NAME, h5table);
}

void test_dp_table(DataPipeline &dp)
{
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
}

void test_dp_array(DataPipeline &dp)
{
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
  ap->unit() = "unknown";
  ap->dim_unit(0) = "second";
  ap->dim_unit(1) = "mm";
  ap->dim_values(0) = {1, 4};

  const std::string TEST_HDF5_DATAPRODUCT = "test_npy";
  // error: Unable to create link (name already exists)
  // when I create_dataset from the given group object,  it may be caused by
  // `get_write_group()`
  dp.write_array(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME, *ap);
  dp.read_array(TEST_HDF5_DATAPRODUCT, TEST_DATASET_NAME);
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

  test_array_local(); // without data pipeline
  //test_dp_array(dp);

  test_table_local(); // without data pipeline
  //test_dp_table(dp);

  std::cout << "data pipeline C++ api tested pass successfully\n";

  return 0;
}
