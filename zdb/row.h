#pragma once

#include "schema.h"
#include <memory>
#include <vector>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <sstream>
#include <string>

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

template <>
struct fmt::formatter<Row> : fmt::formatter<string_view> {
	string formatCurrency(float64 dollars, size_t sigFigs = 7)
	{
		string res(sigFigs + 4, '0'); // Fill with 0s
		if (dollars >= pow(10, sigFigs))
		{
			res = fmt::format("{:1.{}e}", dollars, sigFigs - 4);
			// Replace e+0 with e
			size_t start_pos = res.find("e+0");
			if (start_pos != string::npos)
				res.replace(start_pos + 1, 2, "");
			else
			{
				// Replace e-0 with e-
				start_pos = res.find("e-0");
				if (start_pos != string::npos)
					res.replace(start_pos + 2, 1, "");
			}
		}
		else
		{
			int numDigits = 0;
			float64 tmpDollars = dollars;
			while (tmpDollars > 1)
			{
				tmpDollars /= 10;
				numDigits++;
			}
			res = fmt::format("{:<{}.{}f}", dollars, numDigits, sigFigs - numDigits);
			// Remove trailing zeros
			if (sigFigs - numDigits > 0)
			{
				res.erase(find_if(res.rbegin(), res.rend(), [](int ch) {
					return ch != '0';
				}).base(), res.end());
			}
		}

		return fmt::format("{:<{}}", res, sigFigs + 1);
	}

	double micros_to_cents = 1000000;

	template <typename FormatContext>
	auto format(Row row, FormatContext& ctx) {
		ostringstream os;
		for (size_t i = 0; i < row.schema->columns.size(); i++)
		{
			switch (row.schema->columns[i].type) {
			case ColumnType::TIMESTAMP:
				fmt::print(os, formatNanos(row.columns[i].ts));
				break;
			case ColumnType::CURRENCY:
			{
				int64 microCents = row.columns[i].i64;
				float64 dollars = microCents / micros_to_cents;
				fmt::print(os, formatCurrency(dollars));
				break;
			}
			case ColumnType::SYMBOL:
				fmt::print(os, "{:>7}", row.columns[i].sym);
				break;
			case ColumnType::UINT32:
				fmt::print(os, formatCurrency(row.columns[i].ui32));
				break;
			case ColumnType::UINT64:
				fmt::print(os, formatCurrency(row.columns[i].ui64));
				break;
			default:
				fmt::print(os, "?");
				break;
			}
			if (i != row.schema->columns.size() - 1)
				fmt::print(os, " ");
		}
		return formatter<string_view>::format(os.str(), ctx);
	}
};
