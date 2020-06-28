#pragma once

#include "schema.h"
#include <memory>
#include <vector>
#include <string>

using namespace std;

class VariantRow
{
public:
  VariantRow(Timestamp timestamp);
  // Takes ownership of rowValues
  VariantRow(Timestamp timestamp, vector<RowValueVariant> rowValues);
  vector<RowValueVariant> columns;
};
