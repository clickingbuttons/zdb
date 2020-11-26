use chrono::NaiveDate;
use fastrand;
use zdb::{
  schema::*,
  table::{scan::RowValue, Table},
  test_symbols::SYMBOLS
};

static ROW_COUNT: usize = 20_000;

struct OHLCV {
  ts:       i64,
  sym:      String,
  open:     f32,
  high:     f32,
  low:      f32,
  close:    f32,
  close_un: f32,
  volume:   u64
}

fn generate_symbol() -> String {
  let rand_index = fastrand::usize(0..SYMBOLS.len());

  String::from(SYMBOLS[rand_index])
}

fn generate_row(ts: i64) -> OHLCV {
  OHLCV {
    ts,
    sym: generate_symbol(),
    open: fastrand::f32(),
    high: fastrand::f32(),
    low: fastrand::f32(),
    close: fastrand::f32(),
    close_un: fastrand::f32(),
    volume: fastrand::u64(0..1_000_000_000)
  }
}

fn generate_rows(row_count: usize) -> Vec<OHLCV> {
  let mut res = Vec::with_capacity(row_count);

  for i in 0..row_count {
    let row = generate_row((i * 300_000) as i64);
    res.push(row);
  }

  res
}

fn write_rows(table: &mut Table, rows: Vec<OHLCV>) {
  // Maybe one day we can do this dynamically...
  for r in rows {
    let ts = match table.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    table.put_timestamp(ts + r.ts);
    table.put_symbol(r.sym);
    table.put_currency(r.open);
    table.put_currency(r.high);
    table.put_currency(r.low);
    table.put_currency(r.close);
    table.put_currency(r.close_un);
    table.put_u64(r.volume);
    table.write();
  }
  table.flush();
}

fn main() {
  let schema = Schema::new("agg1m")
    .add_cols(vec![
      Column::new("ticker", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ])
    .data_dirs(vec!["data2", "data3"])
    .partition_by(PartitionBy::Day);
  let table_name = schema.name.clone();
  {
    let mut table = Table::create_or_open(schema).expect("Could not create/open table");
    println!("Generating {} rows", ROW_COUNT);
    let rows = generate_rows(ROW_COUNT);

    println!("Writing rows");
    write_rows(&mut table, rows);
  }

  {
    println!("Scanning rows");
    let table = Table::open(&table_name).expect("Could not open table");
    let mut sum = 0.0;

    table.scan(
      0,
      NaiveDate::from_ymd(1972, 1, 1)
        .and_hms(0, 0, 0)
        .timestamp_nanos(),
      vec!["ts", "ticker", "close", "volume"],
      |row: Vec<RowValue>| {
        if row[1].get_symbol() == "TX" {
          sum += row[2].get_currency() as f64;
        }
      }
    );
    println!("Sum {}", sum);
  }
}
