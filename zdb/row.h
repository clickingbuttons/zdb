#pragma once

#include <string>
#include <variant>
#include <utility>
#include <vector>

using namespace std;

using RowValue = variant<int, long long, float, double, string>;

class Row
{
public:
	Row(int size);
	Row(long long timestamp);
	Row(long long timestamp, vector<RowValue> rowValues);
	Row(vector<RowValue> rowValues);
	void put(RowValue value);
	vector<RowValue> columns;
};

ostream& operator<<(ostream& os, Row const& row);
