#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

namespace py = pybind11;
using namespace std;

namespace pyglobals
{
  py::object  pd;
  py::object  SimpleNetworkSimAPI;
  py::object  api;
}

void example_data_access()
{
  using namespace pyglobals;

  cout << (string) py::str(api.attr("read_table")("human/mixing-matrix")) << endl;

  map<string,vector<double>>  estc_map; // pybind automatically recognises a map as a dict

  estc_map["a"] = vector<double>{1,2};
  estc_map["b"] = vector<double>{3,4};

  py::object estc_df = pd.attr("DataFrame")(estc_map);

  api.attr("write_table")("human/estimatec", estc_df);
}

class Column 
{
  public:
  virtual string get_value_as_string(int i)=0;
};

template<typename T>
class ColumnT : public Column
{
  public:
  ColumnT(const vector<T> &vals_in) : vals(vals_in) {};
  string get_value_as_string(int i);

  vector<T> vals;
};

template<typename T>
string ColumnT<T>::get_value_as_string(int i)
{
  stringstream ss;
  ss << vals.at(i);
  return ss.str();
}

class Table
{
  public:

  Table() : m_size(0) {};

  template<typename T>
  void add_column(const string &colname, const vector<T> &values);

  template<typename T>
  vector<T> &get_column(const string &colname);

  const vector<string> &get_column_names();

  string to_string();

  private:
  map<string, shared_ptr<Column>>  columns;
  vector<string>        colnames;
  size_t                m_size;
};

string python_type(py::object obj)
{
  return (string) py::str(obj.get_type());
}

template<typename T>
void Table::add_column(const string &colname, const vector<T> &values)
{
  if (m_size > 0)  {
    if (values.size() != m_size) {
      throw invalid_argument("Column size mismatch in add_column");
    }
  }
  else {
    m_size = values.size();
  }

  columns[colname].reset(new ColumnT<T>(values));
  colnames.push_back(colname);
  cout << "  Added table column " << colname << endl;
}

template<typename T>
vector<T> &Table::get_column(const string &colname)
{
  if (columns.find(colname) == columns.end()) {
    throw out_of_range("There is no column named " + colname + " in this table");
  }

  return dynamic_cast<T>(*columns[colname]); // throws std::bad_cast if type mismatch
}

const vector<string> &Table::get_column_names()
{
  return colnames;
}

string Table::to_string()
{
  stringstream ss;
  vector<string> colnames = get_column_names();
  vector<int> colwidths;
  int total_width = 0;

  for (size_t j = 0; j < colnames.size(); j++) {
    int width = colnames.at(j).size();

    for (size_t i = 0; i < m_size; i++) {
      int this_width = columns[colnames.at(j)]->get_value_as_string(i).size();
      width = max(width, this_width);
    }
    colwidths.push_back(width);
    total_width += width+1;
  }

  string sep = string(total_width, '=');

  ss << sep << endl;

  for (size_t j = 0; j < colnames.size(); j++) {
    ss << setw(colwidths.at(j)+1) << colnames.at(j);
  }

  ss << endl;
  ss << sep << endl;

  for (size_t i = 0; i < m_size; i++) {
    for (size_t j = 0; j < colnames.size(); j++) {
      ss << setw(colwidths.at(j)+1) << columns[colnames.at(j)]->get_value_as_string(i);
    }
    ss << endl;
  }

  ss << sep << endl;

  return ss.str();
}

Table read_table(const string &data_product)
{
  using namespace pyglobals;

  Table table;

  py::object dataframe = api.attr("read_table")(data_product);

  vector<string> colnames = dataframe.attr("columns").attr("tolist")().cast<vector<string>>();

  for (const auto &colname: colnames) {
    
    string dtype = py::str(dataframe.attr("dtypes").attr(colname.c_str()));

    if (dtype == "float64") {
      vector<double> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<double>>();
      table.add_column<double>(colname, values);
    } else {
      cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;

      vector<string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<string>>();
      table.add_column<string>(colname, values);
    }
  }

  return table;
}

void example_data_access_wrapped()
{
  Table table = read_table("human/mixing-matrix");

  cout << "Table:" << endl;
  cout << table.to_string();
}

int main()
{
  py::scoped_interpreter guard{}; // start the interpreter and keep it alive

  using namespace pyglobals;

  pd = py::module::import("pandas");

  SimpleNetworkSimAPI = py::module::import(
    "data_pipeline_api.simple_network_sim_api").attr("SimpleNetworkSimAPI");

  api = SimpleNetworkSimAPI("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  // example_data_access();

  example_data_access_wrapped();

  pd.release();
  SimpleNetworkSimAPI.release();
  api.release();

  cout << "Done." << endl;
  return 0;

  py::object pandas = py::module::import("pandas");
  py::object Path = py::module::import("pathlib").attr("Path");
  // py::object FileAPI = py::module::import("data_pipeline_api.file_api").attr("FileAPI");
  // py::object StandardAPI = py::module::import("data_pipeline_api.standard_api").attr("StandardAPI");
  py::object SimpleNetworkSimAPI = py::module::import("data_pipeline_api.simple_network_sim_api").attr("SimpleNetworkSimAPI");

  py::object api = SimpleNetworkSimAPI("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  cout << (string) py::str(api.attr("read_table")("human/mixing-matrix")) << endl;

  map<string,vector<double>>  estc_map; // pybind automatically recognises a map as a dict

  estc_map["a"] = vector<double>{1,2};
  estc_map["b"] = vector<double>{3,4};

  py::object estc_df = pandas.attr("DataFrame")(estc_map);

  api.attr("write_table")("human/estimatec", estc_df);

  // py::object data_path = Path("examples/test_data_2");
  // py::object file_api = FileAPI(data_path, Path(data_path.attr("joinpath")("config.toml")), Path(data_path.attr("joinpath")("/access.yaml")));

  // py::object api = SimpleNetworkSimAPI(file_api);

  // py::object human_estimate = api.attr("read_csv")("human/estimate");
  // auto human_estimate_t = human_estimate.get_type();

  // cout << (string) py::str(human_estimate_t) << endl;
  // cout << (string) py::str(human_estimate) << endl;

  // cout << endl;

  // py::array_t<double> human_estimate_columnB = py::array_t<double>(human_estimate.attr("columnB").attr("to_numpy")());

  // cout << (string) py::str(human_estimate_columnB.get_type()) << endl;

  // // https://stackoverflow.com/questions/49582252/pybind-numpy-access-2d-nd-arrays/49693704
  // auto buf = human_estimate_columnB.request();
  // double *ptr = (double *) buf.ptr;
  // cout << ptr[0] << endl;
  // cout << ptr[1] << endl;

  // cout << (string) py::str(human_estimate_columnB.get_type()) << endl;

//   py::exec(R"(
// import pandas as pd
// from pathlib import Path
// from data_pipeline_api.file_api import FileAPI
// from data_pipeline_api.standard_api import StandardAPI
// from data_pipeline_api.csv_api import CsvAPI

// data_path = Path("examples/test_data_2")
// file_api = FileAPI(data_path, data_path / "config.toml", data_path / "access.yaml")

// api = CsvAPI(file_api)
// print(api.read_csv("human/estimate"))
// api.write_csv("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

// api = StandardAPI(file_api)
// print(api.read_estimate("human/estimate"))
// api.write_estimate("human/estimateb", 0.5)

// file_api.write_access_file()
//     )");

//   py::object standard_api  = py::module::import("data_pipeline_api.standard_api");

//   standard_api.attr("StandardAPI")




//   py::api.read_estimate("human/estimateb");




  // std::cout << "Hello World!" << std::endl;
  return 0;
}
