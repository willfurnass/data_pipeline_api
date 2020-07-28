
#pragma once

#include <string>
#include <vector>

#include "pybind11/embed.h"

#include "table.hh"

using namespace std;

class Distribution
{
  public:
  string name;
  map<string, double> params;
};

// TODO: define a representation for distributions

template <typename T>
class Array;

class DataPipeline
{
  public:
  DataPipeline(const string &config_file, const string &uri, const string &git_sha);
  double read_estimate(string data_product, const string &component);
  Distribution read_distribution(const string &data_product, const string &component);
  vector<double> read_sample(const string &data_product, const string &component);
    void write_estimate(const string &data_product, const string &component, double estimate);
  void write_distribution(const string &data_product, const string &component,
                          const Distribution &d);
  void write_samples(const string &data_product, const string &component, const vector<int> &samples);

  Array<double> read_array(const string &data_product, const string &component);
  Table read_table(const string &data_product, const string &component);
  void write_array(const string &data_product, const string &component, const Array<double> &array);
  void write_table(const string &data_product, const string &component, const Table &table);

  private:
  pybind11::object  api;
  // TODO: fix "declared with greater visibility than the type of its field" warning
};
