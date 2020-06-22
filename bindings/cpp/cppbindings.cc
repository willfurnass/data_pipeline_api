#include <iostream>
#include <string>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
//#include "embed.h"

namespace py = pybind11;
using namespace std;

int main()
{
  py::scoped_interpreter guard{}; // start the interpreter and keep it alive

  #ifdef NDEBUG
  cout << "NDEBUG is defined" << endl;
  #endif

  // py::print("Hello, World!"); // use the Python API

  // py::exec(R"(
  //       kwargs = dict(name="World", number=42)
  //       message = "Hello, {name}! The answer is {number}".format(**kwargs)
  //       print(message)
  //   )");

  py::object pandas = py::module::import("pandas");
  py::object Path = py::module::import("pathlib").attr("Path");
  py::object FileAPI = py::module::import("data_pipeline_api.file_api").attr("FileAPI");
  py::object StandardAPI = py::module::import("data_pipeline_api.standard_api").attr("StandardAPI");
  py::object CsvAPI = py::module::import("data_pipeline_api.csv_api").attr("CsvAPI");

  py::object data_path = Path("examples/test_data_2");
  py::object file_api = FileAPI(data_path, Path(data_path.attr("joinpath")("config.toml")), Path(data_path.attr("joinpath")("/access.yaml")));

  py::object api = CsvAPI(file_api);

  py::object human_estimate = api.attr("read_csv")("human/estimate");
  auto human_estimate_t = human_estimate.get_type();

  cout << (string) py::str(human_estimate_t) << endl;
  cout << (string) py::str(human_estimate) << endl;

  cout << endl;

  py::array_t<double> human_estimate_columnB = py::array_t<double>(human_estimate.attr("columnB").attr("to_numpy")());

  cout << (string) py::str(human_estimate_columnB.get_type()) << endl;

  // https://stackoverflow.com/questions/49582252/pybind-numpy-access-2d-nd-arrays/49693704
  auto buf = human_estimate_columnB.request();
  double *ptr = (double *) buf.ptr;
  cout << ptr[0] << endl;
  cout << ptr[1] << endl;

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
