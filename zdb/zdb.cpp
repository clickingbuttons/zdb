#include "table.h"
#include "time.h"
#include <iostream>

using namespace std;

int main()
{
	Schema agg1dSchema("agg1d");
	agg1dSchema.addColumn("sym", ColumnType::SYMBOL);
	agg1dSchema.addColumn("open", ColumnType::CURRENCY);
	agg1dSchema.addColumn("high", ColumnType::CURRENCY);
	agg1dSchema.addColumn("low", ColumnType::CURRENCY);
	agg1dSchema.addColumn("close", ColumnType::CURRENCY);
	agg1dSchema.addColumn("close_unadjusted", ColumnType::CURRENCY);
	agg1dSchema.addColumn("volume", ColumnType::UINT32);

	// Global config
	Config config = Config("zdb.conf");
	config.read();

	// Transfer schema ownership to table
	Table agg1d = Table(agg1dSchema, config);

	shared_ptr<Schema> sharedSchema = make_shared<Schema>(agg1d.schema);
	agg1d.write({
		//					ts ,				 sym   , open,  high,    low,  close, close^,     volume
		Row(1073077200000054742, sharedSchema, { "MSFT", 40.23,		50,		30,		44,		44,		1000 }),
		Row(1073077200001234556, sharedSchema, { "AAPL", 300,		400,	200,	340,	340,	2000 }),
		Row(1073077212356789012, sharedSchema, { "AMZN", 40.234,	50,		30,		44,		44,		3000 }),
		Row(1073077212356789012, sharedSchema, { "BEVD", 1.2345,	50,		30,		44,		44,		4000 }),
		Row(1073077212356789012, sharedSchema, { "BKSH", 256789,	50,		30,		44,		44,		5000 }),
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
