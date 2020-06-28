#include "schema.h"
#include <stdexcept>
#include <sstream>

Schema::Schema()
{
}

Schema::Schema(string name)
{
  this->name = name;
  // First column is always timestamp
  addColumn("ts", ColumnType::TIMESTAMP);
}

Schema::Schema(string name, vector<pair<string, ColumnType>> columns)
  : Schema(name)
{
  for (const pair<string, ColumnType> col : columns)
    addColumn(col.first, col.second);
}

void Schema::addColumn(Column c)
{
  columns.push_back(c);
}

void Schema::addColumn(string name, ColumnType type)
{
  addColumn(Column({ name, type }));
}

string Schema::getColumnTypeName(ColumnType c)
{
  return ColumnTypeNames[(int)c];
}

ColumnType Schema::getColumnType(string s)
{
  for (int i = 0; i < sizeof(ColumnTypeNames) / sizeof(ColumnTypeNames[0]); i++)
    if (ColumnTypeNames[i] == s)
      return ColumnType(i);

  throw runtime_error("Column type " + s + " is invalid");
}

Schema& Schema::operator=(const Schema& other)
{
  name = string(other.name);
  columns = other.columns;
  return *this;
}
