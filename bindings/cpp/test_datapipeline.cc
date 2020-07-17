#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

#include "array.hh"
#include "gitversion.hh"

using namespace std;

void test_array()
{
  cout << "test_array:" << endl;
  Array<double>  a(1, {10});

  for (int i = 0; i < 10; i++) {
    a(i) = i;
  }

  cout << "  a(0) == " << a(0) << endl;

  a(3) = 99;

  cout << "  a(3) == " << a(3) << endl;

  bool caught_ok = false;
  try {
    cout << "  a(11) == " << a(11) << endl;
  } catch(out_of_range e) {
    caught_ok = true;
    cout << "Correct exception when accessing out of bounds" << endl;
  } catch(exception e) {
    throw(logic_error(string("Unexpected exception ")+e.what()+" caught in test"));
  }
  if (!caught_ok) {
    throw(logic_error("Expected exception not thrown"));
  }
}


int main()
{
  cout << "Data pipeline test program version " << GIT_VERSION << endl;

  pybind11::scoped_interpreter guard{}; // start the interpreter and keep it alive

  DataPipeline dp("../../tests/data/config.yaml", "https://whatever", GIT_VERSION);

  // read_estimate
  cout << "read_estimate:" << endl;
  cout << "  parameter/example-estimate -> " << dp.read_estimate("parameter", "example-estimate") << endl;
  cout << "  parameter/example-distribution -> " << dp.read_estimate("parameter", "example-distribution") << endl;
  cout << "  parameter/example-samples -> " << dp.read_estimate("parameter", "example-samples") << endl;

  // Distribution par_dist = dp.read_distribution("parameter", "example-distribution");
  // Gives error: AttributeError: 'rv_frozen' object has no attribute 'name'
  // cout << "parameter/example-distribution = " << par_dist << endl;

  // read_sample
  cout << "read_sample:" << endl;
  cout << "  parameter/example-estimate -> " << dp.read_sample("parameter", "example-estimate") << endl;
  cout << "  parameter/example-distribution -> " << dp.read_sample("parameter", "example-distribution") << endl;
  cout << "  parameter/example-samples -> " << dp.read_sample("parameter", "example-samples") << endl;

  // read_table
  Table table = dp.read_table("object", "example-table");
  cout << "object/example-table:" << endl << table.to_string() << endl;
  // vector<double> mixing = table.get_column<double>("mixing");
  // cout << "mixing = [" << mixing.at(0) << "," << mixing.at(1) << ", ... ]" << endl;

  // read_array
  Array<double> array = dp.read_array("object", "example-array");

  vector<int> shape = array.size();

  cout << "read_array -> " << "[";
  for (int i = 0; i < shape.at(0); i++) {
    cout << " " << array(i);
  }
  cout << " ]" << endl;
    

  // write_array
  Array<double>  a(2, {2,3});

  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 3; j++) {
      a(i,j) = i*j;
    }
  }

  // Fails with ValueError: No name (no name) in h5py/h5g.pyx(161): h5py.h5g.create
  //dp.write_array("human/test_array", "", a);

  // Test array class
  test_array();

  return 0;
}
