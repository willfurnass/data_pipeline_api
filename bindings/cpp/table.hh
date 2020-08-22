
#pragma once

#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <iomanip>
#include <memory>
#include <typeinfo>
#include <typeindex>
#include <algorithm>

using namespace std;

class Column;

class Table
{
  public:

  Table();

  template<typename T>
  void add_column(const string &colname, const vector<T> &values);

  template<typename T>
  vector<T> &get_column(const string &colname);

  type_index get_column_type(const string &colname);

  vector<string> get_column_as_string(const string &colname);

  vector<string> get_column_names() const;

  int get_n_columns() const {return colnames.size();}
  size_t get_column_size() {return m_size;};

  string to_string();

  private:
  map<string, shared_ptr<Column>>  columns;
  vector<string>        colnames;
  size_t                m_size;
};

string get_type_name(type_index ti);
