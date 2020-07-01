
#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"

#include "datapipeline.hh"

using namespace std;
namespace py = pybind11;

DataPipeline::DataPipeline(const string &config_file)
{
  // TODO: tidy up these variable names
  pd = py::module::import("pandas");

  // TODO: StandardAPI needs here
  SimpleNetworkSimAPI = py::module::import(
                            "data_pipeline_api.simple_network_sim_api")
                            .attr("SimpleNetworkSimAPI");

  api = SimpleNetworkSimAPI(config_file);

  py::object StandardAPIClass = py::module::import("data_pipeline_api.standard_api").attr("StandardAPI");

  StandardAPI = StandardAPIClass(config_file);
}

double DataPipeline::read_estimate(string data_product, const string &component)
{
  double est = py::float_(StandardAPI.attr("read_estimate")(data_product));
  // TODO: what about component?
  return est;
}

//Distribution DataPipeline::read_distribution(const string &data_product, const string &component);

double DataPipeline::read_sample(const string *data_product, const string &component)
{
  double est = py::float_(StandardAPI.attr("read_sample")(data_product));
  return est;
}

//void DataPipeline::write_estimate(const string &data_product, const string &component, double estimate);
//void DataPipeline::write_distribution(const string &data_product, const string &component,
//                        const Distribution &d);
//void DataPipeline::write_sample(const string &data_product, const string &component, const vector<double> &samples);

/// std api applies here, but python open(hdf5 file is not impl, use h5py to open file
void DataPipeline::write_array(const string &data_product, const string &component, const Array &da)
{
  py::object group = api.attr("get_write_group")(data_product, component);
  da.encode(group);
}

template <typename DT>
typename std::shared_ptr<ArrayT<DT>> DataPipeline::read_array_T(const string &data_product, const string &component)
{
  /// TODO:  file open should be done by python get_read_group(), to have access check
  py::object group = api.attr("get_read_group")(data_product, component);

  typename ArrayT<DT>::Ptr ap = ArrayT<DT>::decode(group);

  // no need to close dataset?
  //h5file.attr("close")();
  return ap;
}

typename std::shared_ptr<Array> DataPipeline::read_array(const string &data_product, const string &component)
{
  py::object group = api.attr("get_read_group")(data_product, component);
  return DataDecoder::decode_array(group);
}

/// todo: use python standard API to write out
Table DataPipeline::read_table(const string &data_product, const string &component)
{

  Table table;
  py::object dataframe = api.attr("read_table")(data_product, component);

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
    /// TODO: this probably not working for std::string
    // if (dtype == "object")
    // {
    //   vector<std::string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<std::string>>();
    //   table.add_column<std::string>(colname, values);
    // }
    else
    {
      cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;

      /// TODO:  this cast to string does not work, just skip the column
      //vector<string> values = dataframe[colname.c_str()].attr("tolist")().cast<vector<string>>();
      //table.add_column<string>(colname, values);
    }
  }

  py::object group = api.attr("get_read_group")(data_product, component);
  py::str _title = group.attr("__getitem__")(py::str("row_title"));
  py::array _names = group.attr("__getitem__")(py::str("row_names"));
  py::str _units = group.attr("__getitem__")(py::str("column_units"));
  /// TODO: Table has not define fields to save these meta data

  return table;
}

void DataPipeline::write_table(const string &data_product, const string &component,
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
    // else if (dtype == "string"  || dtype == "object")
    // {
    //   l = py::cast(table.get_column<std::string>(colname));
    // }
    else
    {
      cout << "WARNING: Converting column " << colname << " from unsupported type " << dtype << " to string" << endl;
    }
    _map[colname] = l;
  }

  // py::module pd = py::module::import("pandas");   // has been imported in Pipeline ctor()
  py::object _df = pd.attr("DataFrame")(_map);
  //_df.attr("to_hdf")(data_product, component);
  api.attr("write_table")(data_product, component, _df);

  /// TODO: meta data saving
}