use chrono::NaiveDate;
use rand::{prelude::ThreadRng, Rng};
use zdb::{
  schema::*,
  table::{scan::RowValue, Table}
};
use zdb::test_symbols::SYMBOLS;

static ROW_COUNT: usize = 20_000_000;

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

fn generate_symbol(rng: &mut ThreadRng) -> String {
  let rand_index = rng.gen_range(0, SYMBOLS.len());

  String::from(SYMBOLS[rand_index])
}

fn generate_row(ts: i64, rng: &mut ThreadRng) -> OHLCV {
  OHLCV {
    ts,
    sym: generate_symbol(rng),
    open: rng.gen(),
    high: rng.gen(),
    low: rng.gen(),
    close: rng.gen(),
    close_un: rng.gen(),
    volume: rng.gen()
  }
}

fn generate_rows(row_count: usize, rng: &mut ThreadRng) -> Vec<OHLCV> {
  let mut res = Vec::with_capacity(row_count);

  for i in 0..row_count {
    let row = generate_row((i * 100) as i64, rng);
    res.push(row);
  }

  res
}

fn write_rows(agg1d: &mut Table, rows: Vec<OHLCV>) {
  // Maybe one day we can do this dynamically...
  for r in rows {
    let ts = match agg1d.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    agg1d.put_timestamp(ts + r.ts);
    agg1d.put_symbol(r.sym);
    agg1d.put_currency(r.open);
    agg1d.put_currency(r.high);
    agg1d.put_currency(r.low);
    agg1d.put_currency(r.close);
    agg1d.put_currency(r.close_un);
    agg1d.put_u64(r.volume);
    agg1d.write();
  }
  agg1d.flush();
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
    .partition_by(PartitionBy::Day);
  let table_name = schema.name.clone();
  {
    let mut agg1d = Table::create_or_open(schema).expect("Could not create/open table");
    println!("Generating {} rows", ROW_COUNT);
    let rows = generate_rows(ROW_COUNT, &mut rand::thread_rng());

    println!("Writing rows");
    write_rows(&mut agg1d, rows);
  }

  {
    println!("Scanning rows");
    let agg1d = Table::open(&table_name).expect("Could not open table");
    let mut sum = 0.0;

    agg1d.scan(
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
