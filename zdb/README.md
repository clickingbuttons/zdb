# zdb

Time-series database especially designed to store ticks and OHLCV aggs for US equities. Still in development.

## Data types
Has 3 special data types:
- Timestamp
  - int64 representing nanoseconds since epoch
- Currency
  - int64 representing a decimal number
  - float32 on disk for better compression at the cost of only 7 sig figs of precision
    - most exchanges store int64 but given tick size and significance float32 works well
- Symbol
  - char[8] representing a ticker
    - TODO: move to heap and remove limitation

Has normal data types:
- INT32,
- UINT32, // Good for up to 4.29B volume
- INT64,
- UINT64,
- FLOAT32,
- FLOAT64

## API
```c++
#include "table.h"
#include <iostream>

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
  //                          ts,      sym,    open, high, low, close, close2, volume
  VariantRow(1073077200000054742, { "MSFT",   40.23,   50,  30,    44,     44, 10445300 }),
  VariantRow(1073077200001234556, { "AAPL",     300,  400, 200,   340,    340, 212312000 }),
  VariantRow(1073077212356789012, { "AMZN",  40.234,   50,  30,    44,     44, 30312300 }),
  VariantRow(1073077212356789012, { "BEVD",  1.2345,   50,  30,    44,     44, 161000000 }),
  VariantRow(1073077212356789012, { "BKSH", 2567890,   50,  30,    44,     44, 5194967296 }),
});
agg1d.flush();

for (Row row : agg1d.read())
  cout << row.toString(agg1d.schema) << '\n';
```

## Todo
API TODO:
- [x] ~~Use templates to avoid `sharedSchema`~~ Add class RowVariant instead.
- [x] ~~Override ostream for Row so <fmt/core.h> not necessary~~ Added `Row::toString(Schema  &s)`

Feature TODO:
- [ ] Logging
  - [ ] Debug, info, error
  - [ ] To file with level
- [ ] Error messages
  - [ ] Missing meta file
  - [ ] Symbol too large
- [ ] Out-of-order insertions
  - [ ] Warning when not in order
- [ ] Scan forward/backward by timestamp
- [ ] Query
  - [ ] List of symbols [matching critera](https://github.com/clickingbuttons/questdb_bench/blob/master/src/main/java/Main.java#L43)
  - [ ] Language?
