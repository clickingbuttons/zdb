#include "row.h"
#include "time.h"
#include <iomanip>
#include <sstream>
#include <variant>

Row::Row(int size)
{
	columns.reserve(size);
}

Row::Row(long long timestamp)
{
	columns.push_back(timestamp);
}

Row::Row(long long timestamp, vector<RowValue> rowValues)
	: Row(timestamp)
{	
	columns.insert(end(columns), begin(rowValues), end(rowValues));
}

Row::Row(vector<RowValue> rowValues)
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


string Row::toString(Schema const& schema)
{
	ostringstream os;
	char buffer[8];
	for (int i = 0; i < schema.columns.size(); i++)
	{
		switch (schema.columns[i].type) {
		case ColumnType::TIMESTAMP:
		{
			long long nanos = get<long long>(columns[i]);
			os << formatNanos(nanos);
			break;
		}
		case ColumnType::CURRENCY:
		{
			float decimal = get<float>(columns[i]);
			sprintf(buffer, "%-5g", decimal);
			os << buffer;
			if (strlen(buffer) == 5)
			{
				os << " ";
			}
			break;
		}
		case ColumnType::SYMBOL:
		{
			os << left << setw(6) << get<string>(columns[i]);
			break;
		}
		default:
			visit([&](auto&& arg) {
				os << arg;
			}, columns[i]);
			break;
		}
		os << " ";
	}

	return os.str();
}
