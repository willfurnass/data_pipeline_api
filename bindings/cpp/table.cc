
#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>

#include "table.hh"

using namespace std;

class Column
{
public:
  virtual string get_value_as_string(int i) = 0;
  virtual const string &get_dtype_name() const = 0;

  /// this function is copied from array.h,
  template <typename DT>
  static const char *to_dtype_name()
  {
    /// TODO: using DT = std::remove_cv<DType>::type;
    /// decay() from pointer or reference to type

    /// NOTE: std::byte, std::complex, not necessary
    /// `if constexpr ()` or constexpr template is only for C++17
    if (std::is_same<DT, int64_t>::value)
      return "int64";
    else if (std::is_same<DT, int32_t>::value)
      return "int32";
    else if (std::is_same<DT, int16_t>::value)
      return "int16";
    else if (std::is_same<DT, int8_t>::value)
      return "int8";
    else if (std::is_same<DT, uint64_t>::value)
      return "uint64";
    else if (std::is_same<DT, uint32_t>::value)
      return "uint32";
    else if (std::is_same<DT, uint16_t>::value)
      return "uint16";
    else if (std::is_same<DT, uint8_t>::value)
      return "uint8";
    else if (std::is_same<DT, float>::value)
      return "float32";
    else if (std::is_same<DT, double>::value)
      return "float64";
    else if (std::is_same<DT, bool>::value)
      return "bool";
    else if (std::is_same<DT, std::string>::value)
      return "string";
    else
    {
      throw std::runtime_error("data type valid as Array element or Atomic value");
    }
  }
};

template <typename T>
class ColumnT : public Column
{
public:
  ColumnT(const vector<T> &vals_in) : vals(vals_in)
  {
    dtype_name = Column::to_dtype_name<T>();
  };
  string get_value_as_string(int i);

  string dtype_name;
  virtual const string &get_dtype_name() const override
  {
    return dtype_name;
  }
  vector<T> vals;
};

template <typename T>
string ColumnT<T>::get_value_as_string(int i)
{
  stringstream ss;
  ss << vals.at(i);
  return ss.str();
}

template <typename T>
void Table::add_column(const string &colname, const vector<T> &values)
{
  if (m_size > 0)
  {
    if (values.size() != m_size)
    {
      throw invalid_argument("Column size mismatch in add_column");
    }
  }
  else
  {
    m_size = values.size();
  }

  columns[colname].reset(new ColumnT<T>(values));
  colnames.push_back(colname);
}

template <typename T>
vector<T> &Table::get_column(const string &colname)
{
  if (columns.find(colname) == columns.end())
  {
    throw out_of_range("There is no column named " + colname + " in this table");
  }

  return dynamic_cast<ColumnT<T> *>(&*columns[colname])->vals; // throws std::bad_cast if type mismatch
}

const vector<string> &Table::get_column_names()
{
  return colnames;
}

string Table::to_string()
{
  stringstream ss;
  vector<string> colnames = get_column_names();
  vector<int> colwidths;
  int total_width = 0;

  for (size_t j = 0; j < colnames.size(); j++)
  {
    int width = colnames.at(j).size();

    for (size_t i = 0; i < m_size; i++)
    {
      int this_width = columns[colnames.at(j)]->get_value_as_string(i).size();
      width = max(width, this_width);
    }
    colwidths.push_back(width);
    total_width += width + 1;
  }

  string sep = string(total_width, '=');

  ss << sep << endl;

  for (size_t j = 0; j < colnames.size(); j++)
  {
    ss << setw(colwidths.at(j) + 1) << colnames.at(j);
  }

  ss << endl;
  ss << sep << endl;

  for (size_t i = 0; i < m_size; i++)
  {
    for (size_t j = 0; j < colnames.size(); j++)
    {
      ss << setw(colwidths.at(j) + 1) << columns[colnames.at(j)]->get_value_as_string(i);
    }
    ss << endl;
  }

  ss << sep << endl;

  return ss.str();
}

/// I do not understand why this is needed, because, Column<T> is not in header file?
template class ColumnT<double>;
template vector<double> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<double> &values);

template class ColumnT<int64_t>;
template vector<int64_t> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<int64_t> &values);

template class ColumnT<bool>;
template vector<bool> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<bool> &values);

template class ColumnT<string>;
template vector<string> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<string> &values);
