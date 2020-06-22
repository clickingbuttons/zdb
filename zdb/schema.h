#pragma once

#include <map>
#include <string>
#include <utility>
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
// Decimal numbers stored as float32 but read as int64 for higher precision
// I decided float32s are better than uint32 since we get a range bigger than [0, 4294967295]
// The cost is 5-10% slower conversions when loading from disk: http://quick-bench.com/0ELlRfnA1hq4UDBDeyq4uvE9W1g
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
// http://quick-bench.com/qghyMggY7DzUKtKP4B85DHJmPg4
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
	RowValue(Timestamp timestamp) { ts = timestamp; };
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
	Schema();
	Schema(string name);
	Schema(string name, vector<pair<string, ColumnType>> columns);
	void addColumn(Column c);
	void addColumn(string name, ColumnType type);
	vector<Column> columns;
	string name;
	static string getColumnTypeName(ColumnType c);
	static ColumnType getColumnType(string c);
	// Copy assignment operator.
	Schema& operator=(const Schema& other);
private:
	static vector<pair<ColumnType, string>> columnTypes;
};
