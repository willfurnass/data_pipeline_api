#include "table.hh"

using namespace std;

class Column 
{
  public:
  explicit Column() : type(typeid(void)) {};
  virtual ~Column() {};
  virtual string get_value_as_string(int i)=0;
  type_index type;
};

template<typename T>
class ColumnT : public Column
{
  public:
  explicit ColumnT(const vector<T> &vals_in) : vals(vals_in) {type = typeid(T);};
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


map<type_index, string> type_names;

#define REGISTER_TYPE_NAME(x) \
  type_names[type_index(typeid(x))] = #x

void register_type_names()
{
  REGISTER_TYPE_NAME(int);
  REGISTER_TYPE_NAME(long);
  REGISTER_TYPE_NAME(double);
  REGISTER_TYPE_NAME(string);
}

string get_type_name(type_index ti)
{
  if (type_names.find(ti) != type_names.end()) {
    return type_names[ti];
  } else {
    return ti.name();
  }
}

Table::Table() : m_size(0)
{
  register_type_names();
}

template<typename T>
void Table::add_column(const string &colname, const vector<T> &values)
{
  if (columns.find(colname) != columns.end())
  {
    throw invalid_argument("Column '"+colname+"' already exists");
  }

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
}

template<typename T>
vector<T> &Table::get_column(const string &colname)
{
  if (columns.find(colname) == columns.end()) {
    throw out_of_range("There is no column named '" + colname + "' in this table");
  }

  const type_index  &stored_type = columns[colname]->type;
  const type_index  &requested_type = type_index(typeid(T));

  if (requested_type != stored_type) {
    throw invalid_argument(
      "Column \"" + colname + "\" of type " + get_type_name(stored_type) +
      " accessed as " + get_type_name(requested_type));
  }

  return dynamic_cast<ColumnT<T> *>(&*columns[colname])->vals; // throws std::bad_cast if type mismatch
}

vector<string> Table::get_column_as_string(const string &colname)
{
  if (columns.find(colname) == columns.end()) {
    throw out_of_range("There is no column named " + colname + " in this table");
  }

  Column &col = *columns[colname];

  vector<string> result;

  for (size_t i = 0; i < m_size; i++) {
    result.push_back(col.get_value_as_string(i));
  }
  return result;
}

vector<string> Table::get_column_names() const
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

type_index Table::get_column_type(const string &colname)
{
  const type_index col_type = columns[colname]->type;
  return col_type;
}

#define INSTANTIATE_TABLE(type) \
template class ColumnT<type>; \
template vector<type> &Table::get_column(const string &colname); \
template void Table::add_column(const string &colname, const vector<type> &values);

INSTANTIATE_TABLE(double);
INSTANTIATE_TABLE(string);
INSTANTIATE_TABLE(long);
INSTANTIATE_TABLE(int);
