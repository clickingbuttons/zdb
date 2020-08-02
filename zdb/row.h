#pragma once

#include "schema.h"
#include "variantrow.h"
#include <exception>
#include <string>
#include <vector>

using namespace std;

class SymbolTooLongException : public exception
{
public:
  string sym;
  string message;
  SymbolTooLongException(const string& symbol)
    : sym(symbol)
  {
    message = "Symbol {} must be {} or less characters long\n";
  }
  virtual const char* what() const throw()
  {
    return message.c_str();
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
