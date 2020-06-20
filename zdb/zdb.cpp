#include "table.h"
#include "time.h"
#include <iostream>

using namespace std;

int main()
{
	// Global config
	Config config = Config("zdb.conf");
	config.read();

	// TODO: Lookup table on disk
	Schema agg1dSchema("agg1d", {
		{"sym", ColumnType::SYMBOL},
		{"open", ColumnType::CURRENCY},
		{"high", ColumnType::CURRENCY},
		{"low", ColumnType::CURRENCY},
		{"close", ColumnType::CURRENCY},
		{"close_unadjusted", ColumnType::CURRENCY},
		{"volume", ColumnType::UINT32}
	});

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
