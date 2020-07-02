#pragma once

#include "schema.h"
#include "variantrow.h"
#include <exception>
#include <fmt/core.h>
#include <string>
#include <vector>

using namespace std;

class SymbolTooLongException : public exception
{
public:
  string sym;
  SymbolTooLongException(const string& symbol)
    : sym(symbol)
  {
  }
  virtual const char* what() const throw()
  {
    return fmt::format("Symbol {} must be {} or less characters long\n", sym, sizeof(RowValue().sym) - 1).c_str();
  }
};

class Row
{
public:
  Row(Timestamp timestamp);
  Row(VariantRow variantRow, Schema const& schema);
  void put(RowValue const& value);
  bool operator < (const Row& other) const;
  string toString(Schema schema);
  // Main data structure
  vector<RowValue> columns;
};
