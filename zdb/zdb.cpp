#include "table.h"
#include <iostream>
#include <set>
#include <sstream>

using namespace std;

int main()
{
	Schema agg1dSchema("agg1d");
	agg1dSchema.addColumn("sym", ColumnType::SYMBOL);
	agg1dSchema.addColumn("open", ColumnType::DOUBLE);
	agg1dSchema.addColumn("high", ColumnType::DOUBLE);
	agg1dSchema.addColumn("low", ColumnType::DOUBLE);
	agg1dSchema.addColumn("close", ColumnType::DOUBLE);
	agg1dSchema.addColumn("close_unadjusted", ColumnType::DOUBLE);
	agg1dSchema.addColumn("volume", ColumnType::LONG);

	// Global config
	Config config = Config("zdb.conf");
	config.read();

	// Transfer schema ownership to table
	Table agg1d = Table(agg1dSchema, config);

	agg1d.write({
		//  ts  ,   sym   , open, high, low, close, close^, volume
		// ^close unadjusted
		Row(27001, { "MSFT", 40.0, 50.0, 30.0, 44.0, 44.0, 44000LL}),
		Row(27000, { "AAPL", 30.0, 40.0, 20.0, 34.0, 34.0, 34000LL}),
		Row(27000, { "AAPL", 40.0, 50.0, 30.0, 44.0, 44.0, 44000LL}),
	});
	agg1d.flush();

	vector<Row> myRows = agg1d.read();
	for (Row row: myRows)
	{
		cout << row << endl;
	}

	cin.get();
	return 0;
}
