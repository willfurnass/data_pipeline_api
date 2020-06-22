
#pragma once

#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>
#include <memory>

using namespace std;

class Column;

class Table
{
  public:

  Table() : m_size(0) {};

  template<typename T>
  void add_column(const string &colname, const vector<T> &values);

  template<typename T>
  vector<T> &get_column(const string &colname);

  const vector<string> &get_column_names();

  string to_string();

  private:
  map<string, shared_ptr<Column>>  columns;
  vector<string>        colnames;
  size_t                m_size;
};
