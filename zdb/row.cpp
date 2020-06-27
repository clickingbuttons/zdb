#include "row.h"
#include "time.h"
#include <variant>
#include <fmt/core.h>

Row::Row(Timestamp timestamp)
{
	columns.push_back(RowValue(timestamp));
}

// Delegating constructors seems okay
// https://godbolt.org/z/SpWHv7
Row::Row(Timestamp timestamp, shared_ptr<Schema> schema)
	: Row(timestamp)
{
	this->schema = schema;
}

#pragma warning(push)
#pragma warning(disable: 4244)
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
				throw length_error(fmt::format("Symbol {} must be {} or less characters long\n", sym, maxStrLength));
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
#pragma warning(pop)

void Row::put(RowValue const &value)
{
	columns.push_back(value);
}

bool Row::operator<(const Row& other) const
{
	return columns[0].ts < other.columns[0].ts;
}
