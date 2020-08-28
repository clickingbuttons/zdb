use zdb::schema::*;
use zdb::table::*;

fn main() {
  let schema = Schema::new("agg1d").add_cols(vec!(
      Column::new("symbol", ColumnType::SYMBOL),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::UINT32),
    ))
    .partition_by(PartitionBy::YEAR);

  let agg1d = Table::create_or_open(schema)
    .expect("Could not open table");

  println!("{:?}", agg1d);
}
