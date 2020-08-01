#include "table.h"
#include "log.h"
#include <iostream>

using namespace std;

int main()
{
  Schema agg1dSchema = Schema("agg1d", {
    {"sym", ColumnType::SYMBOL},
    {"open", ColumnType::CURRENCY},
    {"high", ColumnType::CURRENCY},
    {"low", ColumnType::CURRENCY},
    {"close", ColumnType::CURRENCY},
    {"close_unadjusted", ColumnType::CURRENCY},
    {"volume", ColumnType::UINT64}
  });

  Table agg1d(agg1dSchema);

  agg1d.write({
    //                          ts,      sym,    open,   high,    low,  close, close2, volume
    VariantRow(1073077200000054742, { "MSFT",   40.23,     50,     30,     44,     44, 10445300 }),
    VariantRow(1073077200001234556, { "AAPL",     300,    400,    200,    340,    340, 212312000 }),
    VariantRow(1073077212356789012, { "AMZN",  40.234,     50,     30,     44,     44, 30312300 }),
    VariantRow(1073077212356789012, { "BEVD",  1.2345,     50,     30,     44,     44, 161000000 }),
    VariantRow(1073077212356789012, { "BKSH",  256789,     50,     30,     44,     44, 5194967296ULL }),
  });
  agg1d.flush();

  vector<Row> rows = agg1d.read();

  for (Row row : agg1d.read())
    cout << row.toString(agg1d.schema) << '\n';

  cin.get();
  return 0;
}
