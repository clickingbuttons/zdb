#pragma once

#include "row.h"
#include "schema.h"
#include "config.h"
#include <exception>
#include <filesystem>
#include <fstream>
#include <fmt/core.h>
#include <unordered_map>
#include <vector>

using namespace std;
using namespace filesystem;

enum class PartitionBy
{
  DAY,
  WEEK,
  MONTH,
  YEAR
};

class NoTableException : public exception
{
  public:
  string tableName;
  NoTableException(const string& tableName)
      : tableName(tableName)
  {
  }
  virtual const char* what() const throw()
  {
    return fmt::format("Table {} does not exist.", tableName).c_str();
  }
};

class Table
{
// ========= table_shared.cpp ==========
public:
  Table(const Schema& schema);
  Table(const string& tableName);
  Schema schema;
private:
  // Shared code between constructors
  void init(string const& tableName);
  // Directory this is stored on disk
  path dir;
  // Metadata saved to _meta
  Config meta;
  // Symbol table saved to _symbols. Stored twice in RAM since there is no array-backed map
  path symbolPath;
  // TODO: Support strings longer than 8 bytes on heap
  unordered_map<string, uint32> symbolSet;
  vector<string> symbols;
  // Helper to get path for column based on its type
  path getColumnFile(Column column);
  // Cache column files to avoid open/close on every read/write
  vector<path> columnPaths;


// ========= table_read.cpp ==========
public:
  vector<Row> read(size_t fromRow, size_t toRow);
  vector<Row> read();
private:
  void readSymbolFile();


// ========= table_write.cpp ==========
public:
  void write(Row row);
  void write(vector<Row> rows);
  void write(VariantRow variantRow);
  void write(vector<VariantRow> variantRows);
  void flush();
private:
  // Used to hold `write`s until `flush`
  vector<Row> rowBuffer;
  // Used to hold row count until `flush`
  size_t rowCount;
};
