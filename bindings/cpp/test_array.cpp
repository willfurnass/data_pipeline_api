#include "array.h"
//#include "datapipeline.hh"

// this API in pipeline.hh must be changed before impl
//void DataPipeline::write_array(const string &data_product, const string &component, vector<double> array);
//void DataPipeline::read_array(const string &data_product, const string &component);

#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

const std::string TEST_HDF5_FILENAME = "test_npy.h5";
const char *TEST_DATASET_NAME = "nparray";

/// this can also be written in pure c++ conveniently
void write_array_to_hdf5(const IArray &da)
{
  py::module h5py = py::module::import("h5py");
  const py::array pya = da.encode();
  py::print(pya);
  auto h5file = h5py.attr("File")(TEST_HDF5_FILENAME, "w");

  py::list shape = py::cast(da.shape());
  //auto tk = pya.dtype().kind(); // maybe must be a py::arg()
  //py::object dataset = h5file.attr("create_dataset")("nparray", shape, pya.dtype());
  py::object dataset = h5file.attr("create_dataset")(TEST_DATASET_NAME, "data"_a = pya);
  //
  dataset.attr("write_direct")(pya);
  // appending metadata as attributes "append"
  py::object attrs = dataset.attr("attrs");
  //attrs.attr("append")("unit", "unknown");  // error
  // no need to close dataset?
  h5file.attr("close")();
}

template <typename DT>
typename Array<DT>::Ptr read_array_from_hdf5()
{
  py::module h5py = py::module::import("h5py");

  auto h5file = h5py.attr("File")(TEST_HDF5_FILENAME, "r");

  py::object dataset = h5file[TEST_DATASET_NAME];
  //py::print("get dataset ", dataset.get_type());  // fine here

  py::module np = py::module::import("numpy");
  const py::array mat = np.attr("zeros")(dataset.attr("shape"),
                                         "dtype"_a = dataset.attr("dtype"));
  dataset.attr("read_direct")(mat);
  py::print("read nparray from dataset", mat.dtype().kind(), mat.nbytes(), mat);
  typename Array<DT>::Ptr ap = Array<DT>::decode(mat);

  // todo: metadata extraction

  // no need to close dataset?
  h5file.attr("close")();
  return ap;
}

/// this will be converted to unit test later
int main()
{
  py::scoped_interpreter guard{}; // start the interpreter and keep it alive
  // make test input array data by py
  py::module np = py::module::import("numpy");

  typedef int64_t DT;
  // np.arange()  return the int64_t type? not double
  py::array a = np.attr("arange")(12); // implicitly downcast from py::object to py::array
  std::vector<size_t> _s = {3, 4};
  py::list s = py::cast(_s);
  py::array mat = np.attr("reshape")(a, s);
  // np.attr("ascontiguousarray")();  make no diff
  // Return a contiguous array (ndim >= 1) in memory (C order), x.flags['C_CONTIGUOUS']
  py::print(mat.dtype().kind(), mat.nbytes(), mat);

  // py::array to Array<T> bug !  first 2 rows correct, but third all zero
  Array<DT>::Ptr ap = Array<DT>::decode(mat);
  std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
  std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;

  // write_array is working, confirmed by h5dump
  write_array_to_hdf5(*ap);
  read_array_from_hdf5<DT>();

  return 0;
}
