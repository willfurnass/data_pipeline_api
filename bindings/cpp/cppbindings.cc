#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

#include "table.hh"
#include "datapipeline.hh"

namespace py = pybind11;
using namespace std;

namespace pyglobals
{
  py::object  pd;
  py::object  SimpleNetworkSimAPI;
  py::object  api;
  py::object  StandardAPI;
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

//  cout << (string) py::str(api.attr("read_estimate")("human/infection/SARS-CoV-2/latent-period")) << endl;
}

string python_type(py::object obj)
{
  return (string) py::str(obj.get_type());
}


void example_data_access_wrapped(DataPipeline &dp)
{
  Table table = dp.read_table("human/mixing-matrix");

  cout << "human/mixing-matrix:" << endl;
  cout << table.to_string();
  cout << endl;

  // No data in the repository yet
  // cout << "estimate: " << dp.read_estimate("TODO","point-estimate");
}

int main()
{
  py::scoped_interpreter guard{}; // start the interpreter and keep it alive

  using namespace pyglobals;

  // example_data_access();

  DataPipeline dp("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  example_data_access_wrapped(dp);

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
