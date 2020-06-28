#include "variantrow.h"

VariantRow::VariantRow(Timestamp timestamp)
{
	columns.push_back(RowValueVariant(timestamp));
}

VariantRow::VariantRow(Timestamp timestamp, vector<RowValueVariant> rowValues)
	: VariantRow(timestamp)
{
	for (RowValueVariant rv : rowValues)
		columns.push_back(rv);
}
