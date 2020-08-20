
#pragma once

#include <string>
#include <vector>

#include "pybind11/embed.h"

#include "table.hh"
#include "array.hh"
#include "distributions.hh"

using namespace std;

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
  PYBIND11_EXPORT void write_distribution(const string &data_product, const string &component,
                          const pybind11::object &distribution);
  void write_samples(const string &data_product, const string &component, const vector<int> &samples);

  Table read_table(const string &data_product, const string &component);
  void write_table(const string &data_product, const string &component, Table &table);

  template<typename T>
  Array<T> read_array(const string &data_product, const string &component)
  {
    pybind11::module np = pybind11::module::import("numpy");
    pybind11::object array_np = api.attr("read_array")(data_product, component).attr("data");
    vector<int> shape = pybind11::list(array_np.attr("shape")).cast<vector<int>>();
    Array<T> array(shape.size(),shape);

    switch(shape.size()) {
      case 1:
        for (int i = 0; i < shape.at(0); i++) {
          if constexpr(std::is_same_v<T, float> || std::is_same_v<T, double>)
          {
            array(i) = pybind11::float_(array_np.attr("item")(i));
          }
          else if constexpr(std::is_same_v<T, int>)
          {
            array(i) = pybind11::int_(array_np.attr("item")(i));
          }
        }
        break;
      case 2:
        for (int i = 0; i < shape.at(0); i++) {
          for (int j = 0; j < shape.at(1); j++) {
            if constexpr(std::is_same_v<T, float> || std::is_same_v<T, double>)
            {
              array(i,j) = pybind11::float_(array_np.attr("item")(make_tuple(i,j)));
            }
            else if constexpr(std::is_same_v<T, int>)
            {
              array(i,j) = pybind11::int_(array_np.attr("item")(make_tuple(i,j)));
            }
          }
        }
        break;
    default:
      throw domain_error("Unsupported array dimensionality in read_array");
    }
    return array;
  }
  
  template<typename T>
  void write_array(const string &data_product, const string &component, 
                                const Array<T> &array)
  {
    pybind11::module np = pybind11::module::import("numpy");
    vector<int> shape = array.size();

    // TODO: the example dataset is an int, and if we try to write a
    // float64 over the top, this results in an error
    const pybind11::array array_np = np.attr("zeros")(shape,"int64");

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

    pybind11::object Array = pybind11::module::import("data_pipeline_api.standard_api").attr("Array");

    api.attr("write_array")(data_product, component, Array(array_np));
  }


  private:
  pybind11::object  api;
  // TODO: fix "declared with greater visibility than the type of its field" warning
};
