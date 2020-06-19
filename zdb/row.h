#pragma once

#include "schema.h"
#include <string>
#include <vector>

using namespace std;

class Row
{
public:
	Row(int size);
	Row(long long timestamp);
	Row(long long timestamp, vector<RowValue> rowValues);
	Row(vector<RowValue> rowValues);
	void put(RowValue value);
	vector<RowValue> columns;
	bool operator < (const Row& other) const;
	string toString(Schema const& schema);
};
