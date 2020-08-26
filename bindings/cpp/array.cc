
#include "array.hh"
#include <vector>
#include <string>
#include <cassert>
#include <stdexcept>

using namespace std;

template<typename T>
Array<T>::Array(int ndims_p, vector<int> dims_p) : dims(dims_p), ndims(ndims_p)
{
  int nvals = 1;

  assert(ndims_p > 0);

  for (int d = 0; d < ndims; d++) {
    nvals *= dims_p.at(d);
  }

  data.resize(nvals);
}

template<typename T>
const T &Array<T>::operator()(int i) const
{
  if (ndims == 1) {
    return data.at(i);
  } else {
    throw logic_error("Array dimension mismatch");
  }
}

template<typename T>
T &Array<T>::operator()(int i)
{
  return const_cast<T&>(const_cast<const Array<T>*>(this)->operator()(i));
}

template<typename T>
const T &Array<T>::operator()(int i, int j) const
{
  if (ndims == 2) {
    if ((i < 0 || i >= dims.at(0)) ||
        (j < 0 || j >= dims.at(1))) {
      throw domain_error("Attempt to access element ("+to_string(i)+","+to_string(j)+
                         " of an array of dimensions ("+to_string(dims.at(0))+","+to_string(dims.at(1))+")");
    }

    // This is column-major (Fortran) ordering, so that looping over x is fastest
    return data.at(j*dims.at(0)+i); // TODO: decide on row-major or column-major
  } else {
    throw logic_error("Array dimension mismatch");
  }
}

template<typename T>
T &Array<T>::operator()(int i, int j)
{
  return const_cast<T&>(const_cast<const Array<T>*>(this)->operator()(i,j));
}

template<typename T>
vector<int> Array<T>::size() const
{
  return dims;
}

template class Array<double>;
template class Array<int>;
template class Array<float>;
