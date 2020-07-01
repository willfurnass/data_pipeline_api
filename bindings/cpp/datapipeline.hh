
#pragma once

#include <string>
#include <vector>

#include "pybind11/embed.h"

#include "table.hh"
#include "array.h"

using namespace std;

// define a representation for distributions
#include "ParameterTypes.h"
using namespace data;

class DataPipeline
{
public:
  DataPipeline(const string &config_file);
  double read_estimate(string data_product, const string &component);
  Distribution read_distribution(const string &data_product, const string &component);
  double read_sample(const string *data_product, const string &component);
  void write_estimate(const string &data_product, const string &component, double estimate);
  void write_distribution(const string &data_product, const string &component,
                          const Distribution &d);
  void write_sample(const string &data_product, const string &component, const vector<double> &samples);

  std::shared_ptr<Array> read_array(const string &data_product, const string &component);
  template <typename DT>
  typename std::shared_ptr<ArrayT<DT>> read_array_T(const string &data_product, const string &component);
  void write_array(const string &data_product, const string &component, Array::Ptr array);

  Table read_table(const string &data_product);
  void write_table(const string &data_product, const string &component, const Table &table);

private:
  pybind11::object pd;
  pybind11::object SimpleNetworkSimAPI;
  pybind11::object api;
  pybind11::object StandardAPI;
  // TODO: fix "declared with greater visibility than the type of its field" warning
};
