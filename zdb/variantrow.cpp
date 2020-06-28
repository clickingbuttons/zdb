#include "variantrow.h"

VariantRow::VariantRow(Timestamp timestamp)
{
  columns.push_back(RowValueVariant(timestamp));
}

VariantRow::VariantRow(Timestamp timestamp, vector<RowValueVariant> rowValues)
  : VariantRow(timestamp)
{
  columns.insert(columns.end(), rowValues.begin(), rowValues.end());
}
