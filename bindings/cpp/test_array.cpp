#include "array.h"
#include "datapipeline.hh"

#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

const std::string TEST_HDF5_FILENAME = "test_npy.h5";
const char *TEST_DATASET_NAME = "nparray";

/// std api applies here, but python open(hdf5 file is not impl, use h5py to open file
void write_array(const string &data_product, const string &component, const Array &da)
{
  py::module h5py = py::module::import("h5py");
  const py::array pya = da.encode();
  //py::print(pya);
  auto h5file = h5py.attr("File")(data_product, "w");

  py::list shape = py::cast(da.shape());
  //auto tk = pya.dtype().kind(); // maybe must be a py::arg()
  //py::object dataset = h5file.attr("create_dataset")("nparray", shape, pya.dtype());
  py::object dataset = h5file.attr("create_dataset")(component, "data"_a = pya);
  //
  dataset.attr("write_direct")(pya);

  py::object attrs = dataset.attr("attrs");
  attrs.attr("__setitem__")(py::str("unit"), da.unit());
  //  attrs.attr("__setitem__")(py::str("title"), da.title());
  auto dims = ((ArrayT<int64_t> &)da).dims();
  for (size_t i = 0; i < dims.size(); i++)
  {
    std::string dn = "dim_" + std::to_string(i);
    py::array _dv = py::cast(dims[i].values);
    attrs.attr("__setitem__")(py::str(dn + "_values"), _dv);
    attrs.attr("__setitem__")(py::str(dn + "_unit"), py::cast(dims[i].unit));
    py::str dtitle = py::cast(dims[i].title);
    attrs.attr("__setitem__")(py::str(dn + "_title"), dtitle);
  }
  // no need to close dataset?
  h5file.attr("close")();
}

template <typename DT>
typename ArrayT<DT>::Ptr read_array(const string &data_product, const string &component)
{
  py::module h5py = py::module::import("h5py");
  auto h5file = h5py.attr("File")(data_product, "r");

  py::object dataset = h5file[py::str(component)];
  //py::print("get dataset ", dataset.get_type());  // fine here

  py::module np = py::module::import("numpy");
  const py::array mat = np.attr("zeros")(dataset.attr("shape"),
                                         "dtype"_a = dataset.attr("dtype"));
  dataset.attr("read_direct")(mat);
  py::print("read nparray from dataset", mat.dtype().kind(), mat.nbytes(), mat);
  typename ArrayT<DT>::Ptr ap = ArrayT<DT>::decode(mat);

  // if the date type is unknown
  //Array::Ptr ip = DataDecoder::decode_array(mat);  // working

  py::object attrs = dataset.attr("attrs");
  py::str _unit = attrs.attr("__getitem__")(py::str("unit"));
  ap->unit() = _unit;
  for (size_t i = 0; i < mat.ndim(); i++)
  {
    std::string dn = "dim_" + std::to_string(i);
    py::str dtitle = attrs.attr("__getitem__")(py::str(dn + "_title"));
    py::array _dv = attrs.attr("__getitem__")(py::str(dn + "_values"));
    py::str dunit = attrs.attr("__getitem__")(py::str(dn + "_unit"));
    Dimension<DT> d{dtitle, _dv.cast<std::vector<DT>>(), dunit};
    ap->dims().push_back(d);
  }

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
  ArrayT<DT>::Ptr ap = ArrayT<DT>::decode(mat);
  std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
  std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;
  ap->unit() = "unknown";
  ap->dim_unit(0) = "second";
  ap->dim_unit(1) = "mm";
  ap->dim_values(0) = {1, 4};

  // write_array is working, confirmed by h5dump
  write_array(TEST_HDF5_FILENAME, TEST_DATASET_NAME, *ap);
  read_array<DT>(TEST_HDF5_FILENAME, TEST_DATASET_NAME);

  return 0;
}
