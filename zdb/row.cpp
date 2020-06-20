#include "row.h"
#include "time.h"
#include <iomanip>
#include <iostream>
#include <sstream>
#include <variant>

Row::Row(Timestamp timestamp)
{
	RowValue val;
	val.ts = timestamp;
	columns.push_back(val);
}

// Delegating constructors seems okay
// https://godbolt.org/z/SpWHv7
Row::Row(Timestamp timestamp, shared_ptr<Schema> schema)
	: Row(timestamp)
{
	this->schema = schema;
}

Row::Row(Timestamp timestamp, shared_ptr<Schema> schema, vector<RowValueVariant> rowValues)
	: Row(timestamp, schema)
{
	for (size_t i = 0; i < rowValues.size(); i++)
	{
		RowValue val;
		constexpr size_t maxStrLength = sizeof(val.sym) - 1;
		switch (schema->columns[i + 1].type)
		{
		// 8 byte types
		case ColumnType::INT64:
		case ColumnType::TIMESTAMP:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.i64 = var;
				}, rowValues[i]);
			break;
		case ColumnType::UINT64:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.ui64 = var;
				}, rowValues[i]);
			break;
		case ColumnType::FLOAT64:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.f64 = var;
				}, rowValues[i]);
			break;
		// 4 byte types
		case ColumnType::INT32:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.i32 = var;
				}, rowValues[i]);
			break;
		case ColumnType::UINT32:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.ui32 = var;
				}, rowValues[i]);
			break;
		case ColumnType::CURRENCY:
		case ColumnType::FLOAT32:
			visit([&](auto const& var) {
				using type = decay_t<decltype(var)>;
				if constexpr (!is_same<type, Symbol>::value)
					val.f32 = var;
				}, rowValues[i]);
			break;
		case ColumnType::SYMBOL:
		{
			Symbol sym = get<Symbol>(rowValues[i]);
			if (strlen(sym) > maxStrLength)
			{
				throw runtime_error("Symbol \"" + string(sym) + "\"" + " must be " + to_string(maxStrLength) + " characters or shorter");
			}
			strcpy_s(val.sym, sizeof(val.sym), sym);
			break;
		}
		default:
			throw runtime_error("Unable to convert to " + schema->getColumnTypeName(schema->columns[i].type));
			break;
		}
		columns.push_back(val);
	}
}

void Row::put(RowValue const &value)
{
	columns.push_back(value);
}

bool Row::operator<(const Row& other) const
{
	return columns[0].ts < other.columns[0].ts;
}

constexpr double micros_to_cents = 1000000;

ostream& operator<<(ostream& os, Row const& row)
{
	char buffer[8];
	for (size_t i = 0; i < row.schema->columns.size(); i++)
	{
		switch (row.schema->columns[i].type) {
		case ColumnType::TIMESTAMP:
			os << formatNanos(row.columns[i].ts);
			break;
		case ColumnType::CURRENCY:
		{
			int64 microCents = row.columns[i].i64;
			float64 dollars = microCents / micros_to_cents;
			sprintf_s(buffer, sizeof(buffer), "%-5g", dollars);
			os << buffer;
			if (strlen(buffer) == 5)
			{
				os << " ";
			}
			break;
		}
		case ColumnType::SYMBOL:
			os << left << setw(6) << row.columns[i].sym;
			break;
		case ColumnType::UINT32:
			os << left << setw(6) << row.columns[i].ui32;
			break;
		default:
			os << "unknown"; // row.columns[i];
			break;
		}
		os << " ";
	}

	return os;
}
