#include "table.h"
#include "time.h"
#include <iostream>
#include <fmt/core.h>
#include <locale>

using namespace std;

int main()
{
	// Speed
	ios::sync_with_stdio(false);
	locale::global(locale("en_US.UTF-8"));

	// Global config
	Config config = Config("zdb.conf");
	config.read();

	Schema agg1dSchema = Schema("agg1d", {
		{"sym", ColumnType::SYMBOL},
		{"open", ColumnType::CURRENCY},
		{"high", ColumnType::CURRENCY},
		{"low", ColumnType::CURRENCY},
		{"close", ColumnType::CURRENCY},
		{"close_unadjusted", ColumnType::CURRENCY},
		{"volume", ColumnType::UINT64}
		});

	Table agg1d = Table(config, agg1dSchema);

	shared_ptr<Schema> sharedSchema = make_shared<Schema>(agg1d.schema);
	agg1d.write({
		//					ts ,					sym, open,	  high,    low,  close, close^,     volume
		Row(1073077200000054742, sharedSchema, { "MSFT", 40.23,		50,		30,		44,		44,		10445300 }),
		Row(1073077200001234556, sharedSchema, { "AAPL", 300,		400,	200,	340,	340,	212312000 }),
		Row(1073077212356789012, sharedSchema, { "AMZN", 40.234,	50,		30,		44,		44,		30312300 }),
		Row(1073077212356789012, sharedSchema, { "BEVD", 1.2345,	50,		30,		44,		44,		161000000 }),
		Row(1073077212356789012, sharedSchema, { "BKSH", 2567890,	50,		30,		44,		44,		5194967296 }),
	});
	agg1d.flush();

	vector<Row> myRows = agg1d.read();
	for (Row row: myRows)
	{
		fmt::print("{}\n", row);
	}

	cin.get();
	return 0;
}
