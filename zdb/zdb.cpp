#include "table.h"
#include <iostream>

using namespace std;

int main()
{
	unique_ptr<Schema> agg1dSchema(make_unique<Schema>("agg1d"));
	agg1dSchema->addColumn("sym", ColumnType::SYMBOL);
	agg1dSchema->addColumn("open", ColumnType::DOUBLE);
	agg1dSchema->addColumn("high", ColumnType::DOUBLE);
	agg1dSchema->addColumn("low", ColumnType::DOUBLE);
	agg1dSchema->addColumn("close", ColumnType::DOUBLE);
	agg1dSchema->addColumn("close_unadjusted", ColumnType::DOUBLE);
	agg1dSchema->addColumn("volume", ColumnType::LONG);

	// Global config
	Config config = Config("zdb.conf");
	config.read();

	// Transfer schema ownership to table
	Table agg1d = Table(move(agg1dSchema), config);

	Row newRow = Row(27000);
	newRow.putSymbol("AAPL"); // sym
	newRow.putDouble(30); // open
	newRow.putDouble(40); // high
	newRow.putDouble(20); // low
	newRow.putDouble(34); // close
	newRow.putDouble(34); // close_unadjusted
	newRow.putLong(34000); // volume
	agg1d.write(newRow);
	agg1d.flush();

	vector<Row> myRows = agg1d.read(0, 1);

	cin.get();
	return 0;
}
