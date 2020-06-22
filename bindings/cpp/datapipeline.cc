
#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

#include "datapipeline.hh"

using namespace std;
namespace py = pybind11;

DataPipeline::DataPipeline(const string &config_file)
{
  pd = py::module::import("pandas");

  SimpleNetworkSimAPI = py::module::import(
    "data_pipeline_api.simple_network_sim_api").attr("SimpleNetworkSimAPI");

  api = SimpleNetworkSimAPI("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  py::object StandardAPIClass = py::module::import("data_pipeline_api.standard_api").attr("StandardAPI");

  StandardAPI = StandardAPIClass("repos/data_pipeline_api/examples/test_data_2/config.yaml");
}

double DataPipeline::read_estimate(string data_product, const string &component)
{
//  using namespace pyglobals;
  double est = py::float_ (StandardAPI.attr("read_estimate")(data_product));
  // TODO: what about component?
  return est;
}

//Distribution DataPipeline::read_distribution(const string &data_product, const string &component);

double DataPipeline::read_sample(const string *data_product, const string &component)
{
//  using namespace pyglobals;
  double est = py::float_(StandardAPI.attr("read_sample")(data_product));
  return est;
}

//void DataPipeline::write_estimate(const string &data_product, const string &component, double estimate);
//void DataPipeline::write_distribution(const string &data_product, const string &component,
//                        const Distribution &d);
//void DataPipeline::write_sample(const string &data_product, const string &component, const vector<double> &samples);

//vector<double> DataPipeline::read_array(const string &data_product, const string &component);

Table DataPipeline::read_table(const string &data_product)
{
//  using namespace pyglobals;

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

//void DataPipeline::write_array(const string &data_product, const string &component, vector<double> array);
// TODO: need to decide on a class for arrays.  Will return as vector of doubles for now

//void DataPipeline::write_table(const string &data_product, const string &component, const Table &table);
