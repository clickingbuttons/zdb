#include "row.h"

Row::Row(long long timestamp)
{
	values.push_back(pair<ColumnType, long long>(ColumnType::TIMESTAMP, (long long) timestamp));
}

void Row::putTimestamp(long long value)
{
	put(ColumnType::TIMESTAMP, (long long) value);
}

void Row::putInt(int value)
{
	put(ColumnType::INT, (int) value);
}

void Row::putLong(long long value)
{
	put(ColumnType::LONG, (long long) value);
}

void Row::putDouble(double value)
{
	put(ColumnType::DOUBLE, (double) value);
}

void Row::putSymbol(string value)
{
	put(ColumnType::SYMBOL, value);
}

void Row::putString(string value)
{
	put(ColumnType::STRING, value);
}

vector<pair<ColumnType, RowValue>> Row::getValues()
{
	return values;
}

void Row::put(ColumnType type, RowValue value)
{
	values.push_back(pair(type, value));
}
