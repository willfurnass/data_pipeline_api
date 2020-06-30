#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

#include "array.hh"

using namespace std;

void test_array()
{
  Array<double>  a(1, {10});

  for (int i = 0; i < 10; i++) {
    a(i) = i;
  }

  cout << "a(0) == " << a(0) << endl;

  a(3) = 99;

  cout << "a(3) == " << a(3) << endl;

  bool caught_ok = false;
  try {
    cout << "a(11) == " << a(11) << endl;
  } catch(out_of_range e) {
    caught_ok = true;
  } catch(exception e) {
    throw(logic_error(string("Unexpected exception ")+e.what()+" caught in test"));
  }
  if (!caught_ok) {
    throw(logic_error("Expected exception not thrown"));
  }
}


int main()
{
  pybind11::scoped_interpreter guard{}; // start the interpreter and keep it alive

  DataPipeline dp("repos/data_pipeline_api/examples/test_data_2/config.yaml");

  // read_table
  Table table = dp.read_table("human/mixing-matrix");
  cout << "human/mixing-matrix:" << endl << table.to_string() << endl;
  vector<double> mixing = table.get_column<double>("mixing");
  cout << "mixing = [" << mixing.at(0) << "," << mixing.at(1) << ", ... ]" << endl;

  Array<double>  a(2, {2,3});

  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 3; j++) {
      a(i,j) = i*j;
    }
  }

  test_array();

  dp.write_array("human/test_array", "", a);

  return 0;
}
