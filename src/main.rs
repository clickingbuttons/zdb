use rand::Rng;
use time::date;
use zdb::{schema::*, table::*};

fn generate_row() -> (i64, &'static str, f32, f32, f32, f32, f32, u64) {
  let mut rng = rand::thread_rng();
  let from = date!(2003 - 01 - 01).midnight().assume_utc().timestamp() * 1_000_000_000;
  let to = date!(2023 - 01 - 01).midnight().assume_utc().timestamp() * 1_000_000_000;
  let nanoseconds = rng.gen_range(from, to);
  (
    nanoseconds,
    "AAPL",
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen()
  )
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
    .partition_by("%Y");

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");

  let mut rows = vec![
    (
      1073077200000054742,
      "MSFT",
      40.23,
      50.,
      30.,
      44.,
      44.,
      10445300u64
    ),
    (
      1073077200001234556,
      "AAPL",
      300.,
      400.,
      200.,
      340.,
      340.,
      212312000u64
    ),
    (
      1073077212356789012,
      "AMZN",
      40.234,
      50.,
      30.,
      44.,
      44.,
      30312300u64
    ),
    (
      1073077220000000000,
      "BEVD",
      1.2345,
      50.,
      30.,
      44.,
      44.,
      161000000u64
    ),
    (
      1073077230000000000,
      "BKSH",
      2567890.,
      50.,
      30.,
      44.,
      44.,
      5194967296u64
    ),
  ];

  for _ in 0..5 {
    rows.push(generate_row());
  }

  // Maybe one day we can do this dynamically...
  for r in rows {
    agg1d.put_timestamp(r.0);
    agg1d.put_symbol(r.1);
    agg1d.put_f32(r.2);
    agg1d.put_f32(r.3);
    agg1d.put_f32(r.4);
    agg1d.put_f32(r.5);
    agg1d.put_f32(r.6);
    agg1d.put_u64(r.7);
    agg1d.write();
  }

  agg1d.flush();

  agg1d.read(0, 0);
}
