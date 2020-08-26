#pragma once

#include <vector>

template <typename T>
class Array
{
  public:
  Array(int ndims_p, std::vector<int> dims_p);

  T &operator()(int i, int j);
  T &operator()(int i);

  const T &operator()(int i, int j) const;
  const T &operator()(int i) const;

  std::vector<int> size() const;

  private:
  std::vector<T> data;
  const std::vector<int> dims;
  const int ndims;
};
