#include "row.h"
#include "time.h"
#include <iomanip>
#include <sstream>
#include <variant>

Row::Row(long long timestamp)
{
	columns.push_back(timestamp);
}

Row::Row(long long timestamp, shared_ptr<Schema> schema)
	: Row(timestamp) // TODO: how efficient is this?
{
	this->schema = schema;
}

Row::Row(long long timestamp, shared_ptr<Schema> schema, vector<RowValue> rowValues)
	: Row(timestamp, schema)
{
	columns.insert(end(columns), begin(rowValues), end(rowValues));
}

void Row::put(RowValue value)
{
	columns.push_back(value);
}

bool Row::operator<(const Row& other) const
{
	return columns[0] < other.columns[0];
}

constexpr double micros_to_cents = 1000000;

ostream& operator<<(ostream& os, Row const& row)
{
	char buffer[8];
	int size = row.schema->columns.size();
	for (int i = 0; i < size; i++)
	{
		switch (row.schema->columns[i].type) {
		case ColumnType::TIMESTAMP:
		{
			long long nanos = get<long long>(row.columns[i]);
			os << formatNanos(nanos);
			break;
		}
		case ColumnType::CURRENCY:
		{
			long long microCents = get<long long>(row.columns[i]);
			double dollars = microCents / micros_to_cents;
			sprintf(buffer, "%-5g", dollars);
			os << buffer;
			if (strlen(buffer) == 5)
			{
				os << " ";
			}
			break;
		}
		case ColumnType::SYMBOL:
		{
			os << left << setw(6) << get<string>(row.columns[i]);
			break;
		}
		default:
			visit([&](auto&& arg) {
				os << arg;
			}, row.columns[i]);
			break;
		}
		os << " ";
	}

	return os;
}
