
#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

#include "datapipeline.hh"

using namespace std;
namespace py = pybind11;

DataPipeline::DataPipeline(const string &config_file, const string &uri, const string &git_sha) :
  api(py::module::import("data_pipeline_api.standard_api").attr("StandardAPI").attr("from_config")(
    config_file, uri, git_sha)) {}

DataPipeline::~DataPipeline()
{
  api.attr("file_api").attr("close")();
}

double DataPipeline::read_estimate(string data_product, const string &component)
{
  // TODO: can we assume all estimate are floats? Should we check it?
  double est = pybind11::float_ (api.attr("read_estimate")(data_product, component));
  return est;
}

Distribution DataPipeline::read_distribution(const string &data_product, const string &component)
{
  pybind11::object d_py = api.attr("read_distribution")(data_product, component);

  return get_distribution(d_py);
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
                        const pybind11::object &distribution)
{
  api.attr("write_distribution")(data_product, component, distribution);
}

void DataPipeline::write_samples(const string &data_product, const string &component, const vector<int> &samples)
{
  pybind11::module np = pybind11::module::import("numpy");
  api.attr("write_samples")(data_product, component, np.attr("array")(samples));
}


Table DataPipeline::read_table(const string &data_product, const string &component)
{
//  using namespace pyglobals;

  Table table;

  pybind11::object dataframe = api.attr("read_table")(data_product, component);

  vector<string> colnames = dataframe.attr("columns").attr("tolist")().cast<vector<string>>();

  for (const auto &colname: colnames) {
    
    string dtype = pybind11::str(dataframe.attr("dtypes")[colname.c_str()]);

    if (dtype == "float64") {
      vector<double> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<double>>();
      table.add_column<double>(colname, values);
    } else if (dtype == "int64") {
      // TODO: long isn't 64 bits on Windows or 32 bit systems
      vector<long> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<long>>();
      table.add_column<long>(colname, values);
    } else if (dtype == "int32") {
      vector<int> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<int>>();
      table.add_column<int>(colname, values);
    } else {
      cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;

      vector<string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<string>>();
      table.add_column<string>(colname, values);
    }
  }

  return table;
}

void DataPipeline::write_table(const string &data_product, const string &component,
                               Table &table)
{

  pybind11::module pd = pybind11::module::import("pandas");
  pybind11::dict _dict{};
  pybind11::object df = pd.attr("DataFrame")();

  for(int i{0}; i < table.get_n_columns(); ++i)
  {
    const string col = table.get_column_names()[i];

    std::vector<int> _vals_int;
    std::vector<long> _vals_long;
    std::vector<double> _vals_double;
    std::vector<string> _vals_string;

    const std::string type_name = get_type_name(table.get_column_type(col));

    if(type_name == "int")
    {
      _vals_int = table.get_column<int>(col);
      df[pybind11::str(col)] = pybind11::cast(_vals_int);
    }
    else if(type_name == "long")
    {
      _vals_long = table.get_column<long>(col);
      df[pybind11::str(col)] = pybind11::cast(_vals_long);
    }
    else if(type_name == "double")
    {
      _vals_double = table.get_column<double>(col);
      df[pybind11::str(col)] = pybind11::cast(_vals_double);
    }
    else if(type_name == "string")
    {
      _vals_string = table.get_column<string>(col);
      df[pybind11::str(col)] = pybind11::cast(_vals_string);
    }
    else
    {
      throw runtime_error("Cannot add column to dataframe: Unknown type");
    }
  }

  api.attr("write_table")(data_product, component, df);
}
