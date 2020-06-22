
#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>

#include "Table.hh"

using namespace std;

class Column 
{
  public:
  virtual string get_value_as_string(int i)=0;
};

template<typename T>
class ColumnT : public Column
{
  public:
  ColumnT(const vector<T> &vals_in) : vals(vals_in) {};
  string get_value_as_string(int i);

  vector<T> vals;
};

template<typename T>
string ColumnT<T>::get_value_as_string(int i)
{
  stringstream ss;
  ss << vals.at(i);
  return ss.str();
}

template<typename T>
void Table::add_column(const string &colname, const vector<T> &values)
{
  if (m_size > 0)  {
    if (values.size() != m_size) {
      throw invalid_argument("Column size mismatch in add_column");
    }
  }
  else {
    m_size = values.size();
  }

  columns[colname].reset(new ColumnT<T>(values));
  colnames.push_back(colname);
  cout << "  Added table column " << colname << endl;
}

template<typename T>
vector<T> &Table::get_column(const string &colname)
{
  if (columns.find(colname) == columns.end()) {
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

  for (size_t j = 0; j < colnames.size(); j++) {
    int width = colnames.at(j).size();

    for (size_t i = 0; i < m_size; i++) {
      int this_width = columns[colnames.at(j)]->get_value_as_string(i).size();
      width = max(width, this_width);
    }
    colwidths.push_back(width);
    total_width += width+1;
  }

  string sep = string(total_width, '=');

  ss << sep << endl;

  for (size_t j = 0; j < colnames.size(); j++) {
    ss << setw(colwidths.at(j)+1) << colnames.at(j);
  }

  ss << endl;
  ss << sep << endl;

  for (size_t i = 0; i < m_size; i++) {
    for (size_t j = 0; j < colnames.size(); j++) {
      ss << setw(colwidths.at(j)+1) << columns[colnames.at(j)]->get_value_as_string(i);
    }
    ss << endl;
  }

  ss << sep << endl;

  return ss.str();
}

template class ColumnT<double>;
template vector<double> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<double> &values);

template class ColumnT<string>;
template vector<string> &Table::get_column(const string &colname);
template void Table::add_column(const string &colname, const vector<string> &values);
