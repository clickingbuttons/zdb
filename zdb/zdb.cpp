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

	agg1d.write({
		//			  ts  ,   sym   , open, high, low, close, close^, volume
		Row(1073077200000054742, { "MSFT", 40.23f, 50.f, 30.f, 44.f, 44.f, 1000U}),
		Row(1073077200001234556, { "AAPL", 300.f, 400.f, 200.f, 340.f, 340.f, 2000U}),
		Row(1073077212356789012, { "AMZN", 40.234f, 50.f, 30.f, 44.f, 44.f, 3000U}),
		Row(1073077212356789012, { "BEVD", 1.2345f, 50.f, 30.f, 44.f, 44.f, 4000U}),
		Row(1073077212356789012, { "BKSH", 256789.f, 50.f, 30.f, 44.f, 44.f, 5000U}),
	});
	agg1d.flush();

	vector<Row> myRows = agg1d.read();
	for (Row row: myRows)
	{
		cout << row.toString(agg1dSchema) << endl;
	}

	cin.get();
	return 0;
}