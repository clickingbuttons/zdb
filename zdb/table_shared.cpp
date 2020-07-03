#include "table.h"
#include "log.h"
#include <fmt/core.h>
#include <sstream>

path getDir(const string& tableName)
{
  string dbPath = Config::getGlobal().getOption("filesystem", "path", current_path().string());
  path dir = path(dbPath).append("data").append(tableName);
  create_directories(dir);

  return dir;
}

path Table::getColumnFile(Column column)
{
  string columnExt = schema.getColumnTypeName(column.type);
  transform(columnExt.begin(), columnExt.end(), columnExt.begin(),
      [](unsigned char c) { return tolower(c); });

  path ret = path(dir).append(column.name + '.' + columnExt);
  return ret;
}

void Table::init(string const& tableName)
{
  dir = getDir(tableName);
  meta = Config(path(dir).append("_meta"));
  symbolPath = path(dir).append("_symbols");
  readSymbolFile();
  rowCount = stoi(meta.getOption("rows", "count", "0"));
}

// Long member initializer to avoid using pointers in class members
Table::Table(const Schema& s)
{
  init(s.name);
  schema = s;

  ostringstream columnOrder;
  for (int i = 0; i < schema.columns.size(); i++)
  {
    Column c = schema.columns[i];
    meta.setOption("columns", c.name, schema.getColumnTypeName(c.type));
    columnPaths.emplace_back(getColumnFile(c));
    columnOrder << c.name;
    if (i != schema.columns.size() - 1)
      columnOrder << ",";
  }

  meta.setOption("columnOrder", "order", columnOrder.str());
}

Table::Table(const string& tableName)
{
  init(tableName);
  schema = Schema(tableName);

  string columnOrder;
  try
  {
    columnOrder = meta.getOption("columnOrder", "order");
  }
  catch (const out_of_range&)
  {
    NoTableException ex(tableName);
    zlog::error(ex.what());
    throw ex;
  }
  stringstream order(columnOrder);
  string columnName;

  while (getline(order, columnName, ','))
  {
    string columnType = meta.getOption("columns", columnName);
    Column c {
      columnName,
      schema.getColumnType(columnType)
    };
    // Skip first column which is always "ts"
    if (columnName != "ts")
      schema.addColumn(c);

    columnPaths.emplace_back(getColumnFile(c));
  }
}
