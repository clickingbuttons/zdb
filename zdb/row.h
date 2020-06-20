#pragma once

#include "schema.h"
#include <string>
#include <memory>
#include <vector>

using namespace std;

class Row
{
public:
	Row(long long timestamp);
	Row(long long timestamp, shared_ptr<Schema> schema);
	Row(long long timestamp, shared_ptr<Schema> schema, vector<RowValue> rowValues);
	void put(RowValue value);
	vector<RowValue> columns;
	bool operator < (const Row& other) const;
	// Needed for printing
	shared_ptr<Schema> schema;
};

ostream& operator<<(ostream& os, Row const& row);
