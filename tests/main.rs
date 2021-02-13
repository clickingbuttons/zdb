use fastrand;
use zdb::{schema::*, table::Table, test_symbols::SYMBOLS};
use jlrs::prelude::*;
use std::sync::Arc;

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

fn write_ohlcv(table_name: &str, freq: usize, row_count: usize) {
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
    .partition_dirs(vec!["test_data"])
    .partition_by(PartitionBy::Day);
  if let Ok(mut table) = Table::create(schema) {
    println!("Generating {} rows", row_count);
    let ts = match table.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    let rows = generate_rows(ts, row_count, freq);

    println!("Writing rows");
    write_rows(&mut table, rows);
  }
}

fn get_f64_sum(slice: &[f32]) -> f64 {
  slice.iter().map(|v| *v as f64).sum::<f64>()
}

static TABLE_NAME: &str = "agg1m_test";
static ROW_COUNT: usize = 24 * 60 * 60 + 100;
static FROM_TS: i64 = 0;
static TO_TS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;

use std::cell::RefCell;
thread_local! {
  static JULIA: RefCell<Julia> = RefCell::new(unsafe {
    println!("init");
    let mut julia = Julia::init(32).unwrap();
    julia
      .include("ScanValidate.jl")
      .expect("Could not load ScanValidate.jl");
    julia
      .dynamic_frame(|_global, frame| {
        Value::eval_string(frame, "using Serialization")
          .unwrap()
          .unwrap();
        Ok(())
      })
      .unwrap();
    julia
  });
}

#[test]
fn sum_ohlcv_rust() {
  write_ohlcv(TABLE_NAME, 60, ROW_COUNT); 

  let table = Table::open(&TABLE_NAME).expect("Could not open table");

  let mut sums = (0 as u64, 0.0, 0.0, 0.0, 0.0, 0 as u64);
  let mut total = 0;
  let partitions = table.partition_iter(
    FROM_TS,
    TO_TS,
    vec!["ts", "open", "high", "low", "close", "volume"]
  );
  for partition in partitions {
    sums.0 += partition[0].get_u16().iter().map(|ts| *ts as u64).sum::<u64>();
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
  write_ohlcv(TABLE_NAME, 60, ROW_COUNT); 
  let table = Table::open(&TABLE_NAME).expect("Could not open table");
	JULIA.with(|j| {
    let mut julia = j.borrow_mut();
    let bytes = unsafe {
      table.scan_julia(
        FROM_TS,
        TO_TS,
        vec!["open", "high", "low", "close", "volume"],
        &mut julia,
        "sums = [0.0, 0.0, 0.0, 0.0, UInt64(0)]
        total = 0
        function scan(
          open::Array{Float32,1},
          high::Array{Float32,1},
          low::Array{Float32,1},
          close::Array{Float32,1},
          volume::Array{UInt64,1}
        )
          global total += size(close, 1)
          global sums[1] += sum(map((x) -> convert(Float64, x), open))
          global sums[2] += sum(map((x) -> convert(Float64, x), high))
          global sums[3] += sum(map((x) -> convert(Float64, x), low))
          global sums[4] += sum(map((x) -> convert(Float64, x), close))
          global sums[5] += sum(volume)
          (total, sums)
        end"
      ).unwrap()
    };
    // (86500, [43112.65845346451, 43207.75330758095, 43227.65141046047, 43257.26396346092, 4.3414679816093e13])
    assert_eq!(bytes, [55, 74, 76, 10, 4, 0, 0, 0, 20, 2, 49, 228, 81, 1, 0, 21, 0, 14, 228, 0, 0, 13, 18, 21, 13, 229, 64, 0, 128, 24, 27, 248, 24, 229, 64, 0, 192, 90, 216, 116, 27, 229, 64, 0, 128, 99, 114, 40, 31, 229, 64, 128, 206, 195, 72, 34, 190, 195, 66]);
  })
}

#[test]
fn sum_ticks_rust() {
  let table_name = "ticks";
  let row_count = ROW_COUNT * 10;
  write_ohlcv(table_name, 1, row_count); 

  let table = Table::open(&table_name).expect("Could not open table");

  let mut sum = 0.0;
  let mut total = 0;
  let partitions = table.partition_iter(
    FROM_TS,
    TO_TS,
    vec!["open"]
  );
  for partition in partitions {
    sum += get_f64_sum(partition[0].get_currency());
    total += partition[0].get_currency().iter().len();
  }
  assert_eq!(sum, 431907.90271890163);
  assert_eq!(total, row_count);
}

#[test]
fn sum_ticks_julia() {
  let table_name = "ticks";
  let row_count = ROW_COUNT * 10;
  write_ohlcv(table_name, 1, row_count); 

  let table = Table::open(&table_name).expect("Could not open table");

	JULIA.with(|j| {
    let mut julia = j.borrow_mut();
    let bytes = unsafe {
      table.scan_julia(
        FROM_TS,
        TO_TS,
        vec!["open", "high", "low", "close", "volume"],
        &mut julia,
        "sums = [0.0, 0.0, 0.0, 0.0, UInt64(0)]
        total = 0
        function scan(
          open::Array{Float32,1},
          high::Array{Float32,1},
          low::Array{Float32,1},
          close::Array{Float32,1},
          volume::Array{UInt64,1}
        )
          global total += size(close, 1)
          global sums[1] += sum(map((x) -> convert(Float64, x), open))
          global sums[2] += sum(map((x) -> convert(Float64, x), high))
          global sums[3] += sum(map((x) -> convert(Float64, x), low))
          global sums[4] += sum(map((x) -> convert(Float64, x), close))
          global sums[5] += sum(volume)
          (total, sums)
        end"
      ).unwrap()
    };
    // (865000, [431907.90271890163, 432641.35292732716, 432500.71698760986, 432572.70821523666, 4.32761664812548e14])
    assert_eq!(bytes, [55, 74, 76, 10, 4, 0, 0, 0, 20, 2, 49, 232, 50, 13, 0, 21, 0, 14, 228, 0, 88, 98, 156, 143, 92, 26, 65, 0, 200, 101, 105, 5, 104, 26, 65, 0, 0, 50, 222, 210, 101, 26, 65, 0, 96, 54, 213, 242, 102, 26, 65, 64, 96, 219, 212, 130, 153, 248, 66]);
  });
}
