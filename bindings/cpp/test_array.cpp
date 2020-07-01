#include "array.h"
#include "datapipeline.hh"

#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

const std::string TEST_HDF5_FILENAME = "test_npy.h5";
const std::string TEST_HDF5_FILENAME2 = "test_npy2.h5";
const char *TEST_DATASET_NAME = "nparray";

/// std api applies here, but python open(hdf5 file is not impl, use h5py to open file
void write_array(const string &data_product, const string &component, const Array &da)
{
  py::module h5py = py::module::import("h5py");
  auto h5file = h5py.attr("File")(data_product, "w");

  auto group = h5file.attr("create_group")(component);
  da.encode(group);

  // no need to close dataset?
  h5file.attr("close")();
}

template <typename DT>
typename ArrayT<DT>::Ptr read_array_T(const string &data_product, const string &component)
{
  py::module h5py = py::module::import("h5py");
  auto h5file = h5py.attr("File")(data_product, "r");

  py::object group = h5file[py::str(component)];

  typename ArrayT<DT>::Ptr ap = ArrayT<DT>::decode(group);

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
  ArrayT<DT>::Ptr ap = ArrayT<DT>::decode_array(mat);
  std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
  std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;
  ap->unit() = "unknown";
  ap->dim_unit(0) = "second";
  ap->dim_unit(1) = "mm";
  ap->dim_values(0) = {1, 4};

  // write_array is working, confirmed by h5dump
  write_array(TEST_HDF5_FILENAME, TEST_DATASET_NAME, *ap);
  read_array_T<DT>(TEST_HDF5_FILENAME, TEST_DATASET_NAME);

  return 0;
}
