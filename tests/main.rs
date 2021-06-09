use fastrand;
use std::slice::from_raw_parts;
use zdb::{
  schema::*,
  server::{
    julia::{init_julia, jl_array_t, jl_get_nth_field, jl_unbox_int64},
    query::{run_query, Query}
  },
  table::Table,
  test_symbols::SYMBOLS
};

pub fn initialize_agg1m() -> Table {
  match Table::open(&TABLE_NAME) {
    Ok(t) => t,
    Err(_e) => write_ohlcv(TABLE_NAME, 60, ROW_COUNT).expect("Could not open table")
  }
}

pub fn initialize_trades() -> Table {
  match Table::open(&TICKS_NAME) {
    Ok(t) => t,
    Err(_e) => write_ohlcv(TICKS_NAME, 1, ROW_COUNT * 10).expect("Could not open table")
  }
}

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

fn generate_rows(from_ts: i64, row_count: usize, freq: usize) -> Vec<OHLCV> {
  let mut res = Vec::with_capacity(row_count);

  for i in 1..row_count + 1 {
    let row = generate_row(from_ts + (i * freq * 1_000_000_000) as i64);
    res.push(row);
  }

  res
}

fn write_rows(table: &mut Table, rows: Vec<OHLCV>) {
  // Maybe one day we can do this dynamically...
  for r in rows {
    table.put_timestamp(r.ts);
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

fn write_ohlcv(table_name: &str, freq: usize, row_count: usize) -> std::io::Result<Table> {
  fastrand::seed(0);

  let schema = Schema::new(table_name)
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp).with_resolution(freq as i64 * 1_000_000_000),
      Column::new("ticker", ColumnType::Symbol16),
      Column::new("open", ColumnType::Currency),
      Column::new("high", ColumnType::Currency),
      Column::new("low", ColumnType::Currency),
      Column::new("close", ColumnType::Currency),
      Column::new("close_un", ColumnType::Currency),
      Column::new("volume", ColumnType::U64),
    ])
    .partition_by(PartitionBy::Day);
  let mut table = Table::create(schema);
  if let Ok(ref mut t) = table {
    println!("Generating {} rows", row_count);
    let ts = match t.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    let rows = generate_rows(ts, row_count, freq);

    println!("Writing rows");
    write_rows(t, rows);
  }

  table
}

fn get_f64_sum(slice: &[f32]) -> f64 { slice.iter().map(|v| *v as f64).sum::<f64>() }

static TABLE_NAME: &str = "agg1m_test";
static ROW_COUNT: usize = 24 * 60 * 60 + 100;
static FROM_TS: i64 = 0;
static TO_TS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;

#[test]
fn sum_ohlcv_rust() {
  let table = initialize_agg1m();

  let mut sums = (0 as u64, 0.0, 0.0, 0.0, 0.0, 0 as u64);
  let mut total = 0;
  let partitions = table.partition_iter(FROM_TS, TO_TS, vec![
    "ts", "open", "high", "low", "close", "volume",
  ]);
  for partition in partitions {
    sums.0 += partition[0]
      .get_u16()
      .iter()
      .map(|ts| *ts as u64)
      .sum::<u64>();
    sums.1 += get_f64_sum(partition[1].get_currency());
    sums.2 += get_f64_sum(partition[2].get_currency());
    sums.3 += get_f64_sum(partition[3].get_currency());
    sums.4 += get_f64_sum(partition[4].get_currency());
    sums.5 += partition[5].get_u64().iter().sum::<u64>();
    total += partition[5].get_u64().iter().len();
  }
  assert_eq!(sums.0, 62169850);
  assert_eq!(sums.1, 43112.65845346451);
  assert_eq!(sums.2, 43207.75330758095);
  assert_eq!(sums.3, 43227.65141046047);
  assert_eq!(sums.4, 43257.26396346092);
  assert_eq!(sums.5, 43414679816093);
  assert_eq!(total, ROW_COUNT);
}

#[test]
fn sum_ohlcv_julia() {
  init_julia();
  initialize_agg1m();
  let query = "sums = [0.0, 0.0, 0.0, 0.0, 0.0]
    total = 0
    function scan(
      open::Vector{Float32},
      high::Vector{Float32},
      low::Vector{Float32},
      close::Vector{Float32},
      volume::Vector{UInt64}
    )
      global total += size(close, 1)
      global sums[1] += sum(map((x) -> convert(Float64, x), open))
      global sums[2] += sum(map((x) -> convert(Float64, x), high))
      global sums[3] += sum(map((x) -> convert(Float64, x), low))
      global sums[4] += sum(map((x) -> convert(Float64, x), close))
      global sums[5] += sum(volume)
      (total, sums)
    end";

  let query = Query {
    table: TABLE_NAME.to_string(),
    from:  FROM_TS,
    to:    TO_TS,
    query: query.to_string()
  };

  let ans = run_query(&query);
  assert!(ans.is_ok());
  let ans = ans.unwrap();
  unsafe {
    let total = jl_unbox_int64(jl_get_nth_field(ans, 0));
    assert_eq!(total, ROW_COUNT as i64);
    let sums = *(jl_get_nth_field(ans, 1) as *const jl_array_t);
    let sums = from_raw_parts(sums.data as *const f64, sums.length);
    assert_eq!(sums[0], 43112.65845346451);
    assert_eq!(sums[1], 43207.75330758095);
    assert_eq!(sums[2], 43227.65141046047);
    assert_eq!(sums[3], 43257.26396346092);
    assert_eq!(sums[4], 43414679816093.0);
  }
}

static TICKS_NAME: &str = "ticks_agg1m";

#[test]
fn sum_ticks_rust() {
  let table = initialize_trades();

  let mut sum = 0.0;
  let mut total = 0;
  let partitions = table.partition_iter(FROM_TS, TO_TS, vec!["open"]);
  for partition in partitions {
    sum += get_f64_sum(partition[0].get_currency());
    total += partition[0].get_currency().iter().len();
  }
  assert_eq!(sum, 431907.90271890163);
  assert_eq!(total, ROW_COUNT * 10);
}

#[test]
fn sum_ticks_julia() {
  init_julia();
  initialize_trades();
  let row_count = ROW_COUNT * 10;

  let query = "sums = [0.0, 0.0, 0.0, 0.0, 0.0]
    total = 0
    function scan(
      open::Vector{Float32},
      high::Vector{Float32},
      low::Vector{Float32},
      close::Vector{Float32},
      volume::Vector{UInt64}
    )
      global total += size(close, 1)
      global sums[1] += sum(map((x) -> convert(Float64, x), open))
      global sums[2] += sum(map((x) -> convert(Float64, x), high))
      global sums[3] += sum(map((x) -> convert(Float64, x), low))
      global sums[4] += sum(map((x) -> convert(Float64, x), close))
      global sums[5] += sum(volume)
      (total, sums)
    end";
  let query = Query {
    table: TICKS_NAME.to_string(),
    from:  FROM_TS,
    to:    TO_TS,
    query: query.to_string()
  };

  let ans = run_query(&query);
  assert!(ans.is_ok());
  let ans = ans.unwrap();
  unsafe {
    let total = jl_unbox_int64(jl_get_nth_field(ans, 0));
    assert_eq!(total, row_count as i64);
    let sums = *(jl_get_nth_field(ans, 1) as *const jl_array_t);
    let sums = from_raw_parts(sums.data as *const f64, sums.length);
    assert_eq!(sums[0], 431907.90271890163);
    assert_eq!(sums[1], 432641.35292732716);
    assert_eq!(sums[2], 432500.71698760986);
    assert_eq!(sums[3], 432572.70821523666);
    assert_eq!(sums[4], 4.32761664812548e14);
  }
}
