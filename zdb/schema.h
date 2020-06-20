#pragma once

#include <string>
#include <vector>
#include <map>
#include <variant>

using namespace std;

enum class ColumnType
{
	TIMESTAMP,
	CURRENCY,
	SYMBOL,
	INT32,
	UINT32, // Good for up to 4.29B volume
	DOUBLE
};

// Nanoseconds since epoch, formatted in time.h
using Timestamp = long long;
// Decimal numbers, formatted using << fixed << setprecision(4)
using Currency = float;
// Strings mapped to unsigned ints
using Symbol = string;

using RowValue = variant<Timestamp, Currency, Symbol, int, unsigned int, double>;

struct Column
{
	string name;
	ColumnType type;
};


class Schema {
public:
	Schema(string name);
	void addColumn(Column c);
	void addColumn(string name, ColumnType type);
	vector<Column> columns;
	string name;
	static string getColumnTypeName(ColumnType c);
	// Copy assignment operator.
	Schema& operator=(const Schema& other);
private:
	static map<ColumnType, string> columnTypeStrings;
};
