#pragma once

#include "schema.h"
#include <string>
#include <variant>
#include <utility>

using namespace std;

typedef variant<int, long long, float, double, string> RowValue;

class Row
{
public:
	Row(long long timestamp);
	void putTimestamp(long long value);
	void putInt(int value);
	void putLong(long long value);
	void putDouble(double value);
	void putSymbol(string value);
	void putString(string value);
	vector<pair<ColumnType, RowValue>> getValues();
private:
	vector<pair<ColumnType, RowValue>> values;
	void put(ColumnType type, RowValue value);
};
