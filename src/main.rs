use zdb::schema::*;
use zdb::table::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn main() {
  let schema = Schema::new("agg1d").add_cols(vec!(
      Column::new("symbol", ColumnType::SYMBOL),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ))
    .partition_by(PartitionBy::YEAR);

  let agg1d = Table::create_or_open(schema)
    .expect("Could not open table");

  

  let rows = vec!(
    (1073077200000054742u64, "MSFT",   40.23,   50,  30,    44,     44, 10445300),
    (1073077200001234556, "AAPL",     300.,  400, 200,   340,    340, 212312000),
    (1073077212356789012, "AMZN",  40.234,   50,  30,    44,     44, 30312300),
    (1073077212356789012, "BEVD",  1.2345,   50,  30,    44,     44, 161000000),
    (1073077212356789012, "BKSH", 2567890.,   50,  30,    44,     44, 5194967296u64)
  );

  // Maybe one day we can do this dynamically...
  // for r in rows {
  //   add1d.write(r);
  // }

  // agg1d.write(rows);

  println!("{:?}", agg1d);
}
