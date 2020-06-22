#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

using namespace std;

int main()
{
  pybind11::scoped_interpreter guard{}; // start the interpreter and keep it alive

  DataPipeline dp("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  // read_table
  Table table = dp.read_table("human/mixing-matrix");
  cout << "human/mixing-matrix:" << endl << table.to_string() << endl;
  vector<double> mixing = table.get_column<double>("mixing");
  cout << "mixing = [" << mixing.at(0) << "," << mixing.at(1) << ", ... ]" << endl;

  return 0;
}
