use zdb::schema::*;
use zdb::table::*;

fn main() {
  let schema = Schema::new("agg1d").add_cols(vec!(
      Column::new("ticker", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ))
    .partition_by(PartitionBy::YEAR);

  let mut agg1d = Table::create_or_open(schema)
    .expect("Could not open table");

  let rows = vec!(
    (1073077200000054742u64, "MSFT",   40.23,     50.,  30.,    44.,     44.,   10445300u64),
    (1073077200001234556u64, "AAPL",     300.,   400., 200.,   340.,    340.,  212312000u64),
    (1073077212356789012u64, "AMZN",  40.234,     50.,  30.,    44.,     44.,   30312300u64),
    (1073077212356789012u64, "BEVD",  1.2345,     50.,  30.,    44.,     44.,  161000000u64),
    (1073077212356789012u64, "BKSH", 2567890.,    50.,  30.,    44.,     44., 5194967296u64)
  );

  // Maybe one day we can do this dynamically...
  for r in rows {
    agg1d.puttimestamp(r.0);
    agg1d.putsymbol(r.1);
    agg1d.putf32(r.2);
    agg1d.putf32(r.3);
    agg1d.putf32(r.4);
    agg1d.putf32(r.5);
    agg1d.putf32(r.6);
    agg1d.putu64(r.7);
    agg1d.write();
  }

  agg1d.flush();

  println!("{:?}", agg1d);
}
