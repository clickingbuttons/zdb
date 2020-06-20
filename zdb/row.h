#pragma once

#include "schema.h"
#include <string>
#include <memory>
#include <vector>

using namespace std;

class Row
{
public:
	Row(Timestamp timestamp);
	Row(Timestamp timestamp, shared_ptr<Schema> schema);
	Row(Timestamp timestamp, shared_ptr<Schema> schema, vector<RowValueVariant> rowValues);
	void put(RowValue const& value);
	vector<RowValue> columns;
	bool operator < (const Row& other) const;
	// Needed for printing
	shared_ptr<Schema> schema;
};

ostream& operator<<(ostream& os, Row const& row);
