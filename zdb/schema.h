#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <variant>

using namespace std;

enum class ColumnType
{
	TIMESTAMP,
	CURRENCY,
	SYMBOL,
	INT32,
	UINT32, // Good for up to 4.29B volume
	INT64,
	UINT64,
	FLOAT32,
	FLOAT64
};

// Nanoseconds since epoch, formatted in time.h
using Timestamp = long long;
// Decimal numbers stored as floats but use int64 in calculation
using Currency = float;
using PreciseCurrency = long long;
// char[8] unsigned ints
using Symbol = const char*;

// Simple data types
using int32 = int;
using uint32 = unsigned int;
using int64 = long long;
using uint64 = unsigned long long;
using float32 = float;
using float64 = double;

// This allows for pretty Row initializers. Row will convert it to structs...
using RowValueVariant = variant<Symbol, int64, uint64, float64, int32, uint32, float32>;

// 8 byte value
// no need to store an `int index;` for each row like std::variant does (we have the schema anyways)
// union is possibly 2x faster than std::variant when visiting
// http://quick-bench.com/mC1ICqB8eqeBPNDZfKCDmnjPz7s
union RowValue
{
	Timestamp ts;
	Currency cur;
	PreciseCurrency pcur;
	int32 i32;
	uint32 ui32;
	int64 i64;
	uint64 ui64;
	float32 f32;
	float64 f64;
	// For storing symbols (string storage not yet implemented)
	char sym[8];
	RowValue() { ts = 0; };
	~RowValue() {};
	RowValue(const RowValue& other)
	{
		memcpy(sym, other.sym, sizeof(sym));
	};
};

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
