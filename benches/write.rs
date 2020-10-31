#![feature(test)]
extern crate test;

use rand::{prelude::ThreadRng, Rng};
use test::Bencher;
use zdb::{schema::*, table::*};

static ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#[derive(Debug, Clone)]
struct OHLCV {
  ts: i64,
  symbol: String,
  open: f32,
  high: f32,
  low: f32,
  close: f32,
  close_un: f32,
  volume: u64
}

fn generate_symbol(num_chars: usize, rng: &mut ThreadRng) -> String {
  let mut res = String::with_capacity(num_chars);
  for _ in 0..num_chars {
    let rand_index = rng.gen_range(0, ALPHABET.len());
    res += &ALPHABET[rand_index..rand_index + 1];
  }

  res
}

fn generate_row(
  ts: i64,
  rng: &mut ThreadRng
) -> OHLCV {
  OHLCV {
    ts,
    symbol: generate_symbol(rng.gen_range(1, 5), rng),
    open: rng.gen(),
    high: rng.gen(),
    low: rng.gen(),
    close: rng.gen(),
    close_un: rng.gen(),
    volume: rng.gen()
  }
}

fn generate_rows(
  row_count: usize,
  rng: &mut ThreadRng
) -> Vec<OHLCV> {
  let mut res = Vec::with_capacity(row_count);

  for i in 0..row_count {
    let row = generate_row(i as i64, rng);
    res.push(row);
  }

  res
}

fn write_rows(rows: Vec<OHLCV>, index: i64) {
  let schema = Schema::new(&format!("agg1d{}", index))
    .add_cols(vec![
      Column::new("ticker", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ])
    .partition_by(PartitionBy::Year);

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");
  // Maybe one day we can do this dynamically...
  for r in rows {
    let ts = match agg1d.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    agg1d.put_timestamp(ts + r.ts);
    agg1d.put_symbol(r.symbol);
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

#[bench]
fn write_bench(bencher: &mut Bencher) {
  let rows = generate_rows(1_000, &mut rand::thread_rng());

  let mut i: i64 = 0;
  bencher.iter(|| {
    write_rows(rows.clone(), i);
    i += 1;
  });
}
