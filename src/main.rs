use rand::{prelude::ThreadRng, Rng};
use time::date;
use zdb::{schema::*, table::{*, scan::*}};

static ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn generate_symbol(num_chars: usize, rng: &mut ThreadRng) -> String {
  let mut res = String::with_capacity(num_chars);
  for _ in 0..num_chars {
    let rand_index = rng.gen_range(0, ALPHABET.len());
    res += &ALPHABET[rand_index..rand_index + 1];
  }

  res
}

fn generate_row(from_ts: i64, rng: &mut ThreadRng) -> (i64, String, f32, f32, f32, f32, f32, u64) {
  let low = from_ts;
  let high = low + 24 * 60 * 60 * 1_000_000_000;
  let nanoseconds = rng.gen_range(low, high);
  (
    nanoseconds,
    generate_symbol(rng.gen_range(1, 5), rng),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen()
  )
}

fn generate_rows(
  from_ts: i64,
  row_count: usize,
  rng: &mut ThreadRng
) -> Vec<(i64, String, f32, f32, f32, f32, f32, u64)> {
  let mut res = Vec::with_capacity(row_count);

  let mut from_ts = from_ts;
  for _ in 0..row_count {
    let row = generate_row(from_ts, rng);
    from_ts = row.0;
    res.push(row);
  }

  res
}

fn main() {
  let schema = Schema::new("agg1d")
    .add_cols(vec![
      Column::new("ticker", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ])
    // Specifiers: https://docs.rs/time/0.2.22/time/index.html#formatting
    .partition_by("%Y");

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");
  let last_ts = agg1d.get_last_ts();

  let rows = generate_rows(last_ts, 100, &mut rand::thread_rng());

  // Maybe one day we can do this dynamically...
  for r in rows {
    agg1d.put_timestamp(r.0);
    agg1d.put_symbol(&r.1);
    agg1d.put_currency(r.2);
    agg1d.put_currency(r.3);
    agg1d.put_currency(r.4);
    agg1d.put_currency(r.5);
    agg1d.put_currency(r.6);
    agg1d.put_u64(r.7);
    agg1d.write();
  }
  agg1d.flush();

  let mut rows = Vec::<Vec<RowValue>>::new();
  let mut sum = 0.0;

  agg1d.scan(
    0,
    date!(1972 - 02 - 01).nanoseconds(),
    vec!["ts", "ticker", "close", "volume"],
    |row: Vec<RowValue>| {
      if row[1].get_symbol() == "TX" {
        sum += row[2].get_currency() as f64;
        rows.push(row);
      }
    }
  );

  for r in rows {
    println!(
      "{} {:7} {:<9} {:>10}",
      r[0].get_timestamp().format("%Y-%m-%d %H:%M:%S.%N"),
      r[1].get_symbol(),
      r[2].get_f32().format_currency(7),
      r[3].get_u64()
    );
  }
  println!("Sum {}", sum);
}
