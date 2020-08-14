
#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

#include "datapipeline.hh"
#include "array.hh"

using namespace std;
namespace py = pybind11;

DataPipeline::DataPipeline(const string &config_file, const string &uri, const string &git_sha) :
  api(py::module::import("data_pipeline_api.standard_api").attr("StandardAPI").attr("from_config")(
    config_file, uri, git_sha)) {}

double DataPipeline::read_estimate(string data_product, const string &component)
{
  // TODO: can we assume all estimate are floats? Should we check it?
  double est = py::float_ (api.attr("read_estimate")(data_product, component));
  return est;
}

Distribution DataPipeline::read_distribution(const string &data_product, const string &component)
{
  throw runtime_error("Not implemented");
  // py::object d_py = api.attr("read_distribution")(data_product, component);


  // Distribution d;

  // d.name = py::str(d_py.attr("dist").attr("name"));

  // map<string, double> params = d_py.attr("params").cast<map<string, double>>();

  // // The distribution returned is a scipy distribution, and it's not
  // // straightforward to extract the needed parameters. Ideally this
  // // would all be handled on the Python side.

  // if (d.name == "gamma") {
    
  //   params["shape"] = 0; // ???
  //   params["scale"] = 0; // ???
  // } else {
  //   throw domain_error("Unrecognised distribution "+d.name);
  // }

  // d.params = params;

  // // cout << "distribution name = " << d.name << endl;

  // return d;
}


vector<double> DataPipeline::read_sample(const string &data_product, const string &component)
{
  return api.attr("read_samples")(data_product, component).cast<vector<double>>();
}

void DataPipeline::write_estimate(const string &data_product, const string &component, double estimate)
{
  api.attr("write_estimate")(data_product, component, estimate);
}

void DataPipeline::write_distribution(const string &data_product, const string &component,
                        const Distribution &d)
{
  throw runtime_error("Not implemented");
}

void DataPipeline::write_samples(const string &data_product, const string &component, const vector<int> &samples)
{
  py::module np = py::module::import("numpy");
  api.attr("write_samples")(data_product, component, np.attr("array")(samples));
}


Table DataPipeline::read_table(const string &data_product, const string &component)
{
//  using namespace pyglobals;

  Table table;

  py::object dataframe = api.attr("read_table")(data_product, component);

  vector<string> colnames = dataframe.attr("columns").attr("tolist")().cast<vector<string>>();

  for (const auto &colname: colnames) {
    
    string dtype = py::str(dataframe.attr("dtypes")[colname.c_str()]);

    if (dtype == "float64") {
      vector<double> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<double>>();
      table.add_column<double>(colname, values);
    } else if (dtype == "int64") {
      // TODO: long isn't 64 bits on Windows or 32 bit systems
      vector<long> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<long>>();
      table.add_column<long>(colname, values);
    } else {
      cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;

      vector<string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<string>>();
      table.add_column<string>(colname, values);
    }
  }

  return table;
}


// TODO: template this over the array type, though I suspect we only need doubles
Array<double> DataPipeline::read_array(const string &data_product, const string &component)
{
  py::module np = py::module::import("numpy");
  py::object array_np = api.attr("read_array")(data_product, component).attr("data");
  vector<int> shape = py::list(array_np.attr("shape")).cast<vector<int>>();
  Array<double> array(shape.size(),shape);

  switch(shape.size()) {
    case 1:
      for (int i = 0; i < shape.at(0); i++) {
        array(i) = py::float_(array_np.attr("item")(i));
      }
      break;
    case 2:
      for (int i = 0; i < shape.at(0); i++) {
        for (int j = 0; j < shape.at(1); j++) {
          array(i,j) = py::float_(array_np.attr("item")(make_tuple(i,j)));
        }
      }
      break;
  default:
    throw domain_error("Unsupported array dimensionality in read_array");
  }
  return array;
}

// TODO: template this over the array type, though I suspect we only need doubles
void DataPipeline::write_array(const string &data_product, const string &component, 
                               const Array<double> &array)
{
  py::module np = py::module::import("numpy");
  vector<int> shape = array.size();

  // TODO: the example dataset is an int, and if we try to write a
  // float64 over the top, this results in an error
  const py::array array_np = np.attr("zeros")(shape,"int64");

  switch(shape.size()) {
    case 1:
      for (int i = 0; i < shape.at(0); i++) {
        array_np.attr("itemset")(i, array(i));
      }
      break;
    case 2:
      for (int i = 0; i < shape.at(0); i++) {
        for (int j = 0; j < shape.at(1); j++) {
          array_np.attr("itemset")(make_tuple(i,j), array(i,j)); // TODO: check index ordering etc here
        }
      }
      break;
  default:
    throw domain_error("Unsupported array dimensionality in write_array");
  }

  py::object Array = py::module::import("data_pipeline_api.standard_api").attr("Array");

  api.attr("write_array")(data_product, component, Array(array_np));
}

// void DataPipeline::write_table(const string &data_product, const string &component,
//                                const Table &table)
// {
//   map<string,vector<double>>  estc_map; // pybind automatically recognises a map as a dict

//   estc_map["a"] = vector<double>{1,2};
//   estc_map["b"] = vector<double>{3,4};

//   py::object estc_df = pd.attr("DataFrame")(estc_map);

//   api.attr("write_table")("human/estimatec", estc_df);
// }
