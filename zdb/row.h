#pragma once

#include "schema.h"
#include "variantrow.h"
#include <vector>
#include <string>

using namespace std;

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
