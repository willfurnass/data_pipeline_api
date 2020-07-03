#include "array.h"
#include "datapipeline.hh"

#include <iostream>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;
using namespace pybind11::literals;

namespace local
{
  /// use h5py to open file and create a H5Group PyObject
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

  ArrayT<int64_t>::Ptr create_int64_array()
  {
    typedef int64_t DT;

    // make test input array data by pybind11
    py::module np = py::module::import("numpy");
    // np.arange()  return the int64_t type? not double
    py::array a = np.attr("arange")(12); // implicitly downcast from py::object to py::array
    std::vector<size_t> _s = {3, 4};
    py::list s = py::cast(_s);
    py::array mat = np.attr("reshape")(a, s);
    // np.attr("ascontiguousarray")();  make no diff
    // Return a contiguous array (ndim >= 1) in memory (C order), x.flags['C_CONTIGUOUS']
    py::print(mat.dtype().kind(), mat.nbytes(), mat);

    ArrayT<DT>::Ptr ap = ArrayT<DT>::decode_array(mat);
    std::cout << "decoded Array from NumpyArray with dim = " << ap->dimension() << std::endl;
    std::cout << "values of the Array<> : " << (*ap)[0] << (*ap)(1, 0) << std::endl;
    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

  template <typename DT>
  typename ArrayT<DT>::Ptr create_array()
  {
    std::vector<DT> values(12);
    ShapeType s = {3, 4};
    typename ArrayT<DT>::Ptr ap = std::make_shared<ArrayT<DT>>(s, values);

    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

  BoolArray::Ptr create_bool_array()
  {
    typedef uint8_t DT;

    ShapeType s = {3, 4};
    std::vector<DT> cbools(12); // HDF5 save bool as unsigned byte
    //BoolArray::Ptr ap = new BoolArray(s, cbools);
    BoolArray::Ptr ap = std::make_shared<BoolArray>(s, cbools);

    ap->unit() = "unknown";
    ap->dim_unit(0) = "second";
    ap->dim_unit(1) = "mm";
    ap->dim_values(0) = {1, 4};

    return ap;
  }

} // namespace local

void test_array_local()
{
  // current write file in truncate mode, existing file will be moved
  const std::string TEST_ARRAY_FILENAME = "test_npy.h5";
  const std::string TEST_ARRAY_FILENAME1 = "test_double_array.h5";
  const std::string TEST_ARRAY_FILENAME2 = "test_bool_array.h5";
  const char *TEST_ARRAY_COMPONENT_NAME = "nparray";
  auto ip = local::create_int64_array();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME, "int64array", *ip);
  local::read_array_T<int64_t>(TEST_ARRAY_FILENAME, "int64array");

  auto dp = local::create_array<double>();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME2, "float64array", *dp);
  local::read_array_T<double>(TEST_ARRAY_FILENAME, "float64array");

  auto bp = local::create_bool_array();
  // write_array is working, confirmed by h5dump
  local::write_array(TEST_ARRAY_FILENAME2, "boolarray", *bp);
  /// NOTE:  can not use BoolArray, specialized type, not impl yet
  //local::read_bool_array(TEST_ARRAY_FILENAME, "boolarray");
}

/// this will be converted to unit test later
int main()
{
  py::scoped_interpreter guard{}; // start the interpreter and keep it alive

  test_array_local();  /// test without data pipeline

  return 0;
}
