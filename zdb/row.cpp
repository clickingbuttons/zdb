#include "row.h"
#include <iostream>

Row::Row(int size)
{
	columns.reserve(size);
}

Row::Row(long long timestamp)
{
	columns.push_back(timestamp);
}

Row::Row(long long timestamp, vector<RowValue> rowValues)
	: Row(timestamp)
{	
	columns.insert(end(columns), begin(rowValues), end(rowValues));
}

Row::Row(vector<RowValue> rowValues)
{
	columns.insert(end(columns), begin(rowValues), end(rowValues));
}

void Row::put(RowValue value)
{
	columns.push_back(value);
}

ostream& operator<<(ostream& os, Row const& row)
{
	for (RowValue val : row.columns)
	{
		visit([&](auto&& arg) {
			os << arg;
			os << string(" ");
		}, val);
	}

	return os;
}
