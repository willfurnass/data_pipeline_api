#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

#include "table.hh"
#include "datapipeline.hh"

using namespace std;

const std::string TEST_HDF5_FILENAME = "test_table.h5";
const std::string TEST_HDF5_FILENAME2 = "test_table2.h5";
const std::string TEST_DATASET_NAME = "table_ds";

/// hdf5 impl can add attributes/metadata while csv can not
void write_table_metadata_to_hdf5(const Table &table)
{
  // read back and append
  py::module h5py = py::module::import("h5py");
  auto h5file = h5py.attr("File")(TEST_HDF5_FILENAME, "w"); // a+ mode?
  auto dataset = h5file.attr("__getitem__")(TEST_DATASET_NAME);
  py::object attrs = dataset.attr("attrs");
  //attrs.attr("__getitem__")(py::str("units"));
}

/// todo:  copy these 2 functions back to datapipeline.cc after testing
Table read_table(const string &data_product, const string &component)
{
  //  using namespace pyglobals;

  Table table;

  // pandas is cable to read HDF5, but need extra parameter, component
  // need test the file format
  //py::object dataframe = api.attr("read_table")(data_product);
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
      cout << "WARNING: skip the column " << colname << " for unsupported type " << dtype << endl;
      // cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;
      // this cast to string does not compile, just skip the column
      //vector<string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<string>>();
      //table.add_column<string>(colname, values);
    }
  }

  return table;
}

//void DataPipeline::write_array(const string &data_product, const string &component, vector<double> array);
// TODO: need to decide on a class for arrays.  Will return as vector of doubles for now

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

int main()
{
  pybind11::scoped_interpreter guard{}; // start the interpreter and keep it alive

#if 0 // CSV test data, outdated API may not work with latest python API

  // todo: a better way to get example data from the repo
  DataPipeline dp("../../../examples/test_data_2/config.yaml");
  // read_table
  Table table = dp.read_table("human/mixing-matrix");
  cout << "human/mixing-matrix:" << endl
       << table.to_string() << endl;
  vector<double> mixing = table.get_column<double>("mixing");
  cout << "mixing = [" << mixing.at(0) << "," << mixing.at(1) << ", ... ]" << endl;
#endif

  // python API does not support hdf5 read and write yet
  py::exec("import pandas as pd\n"
           "df = pd.DataFrame([[1, 1.1, True, 'str1'], [2, 2.2,  False, 'str2']], columns=['x', 'y', 'z', 's'])\n"
           "df.to_hdf('" +
           TEST_HDF5_FILENAME + "', '" + TEST_DATASET_NAME + "')\n");

  Table h5table = read_table(TEST_HDF5_FILENAME, TEST_DATASET_NAME);
  write_table(TEST_HDF5_FILENAME2, TEST_DATASET_NAME, h5table);

  return 0;
}
