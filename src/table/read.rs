use crate::{schema::ColumnType, table::Table};
use std::{convert::TryInto, mem::size_of, cmp::max};
use time::{NumericalDuration,PrimitiveDateTime,date};

macro_rules! read_bytes {
  ($_type:ty, $bytes:expr, $i:expr) => {{
    let size = size_of::<$_type>();
    <$_type>::from_le_bytes($bytes[$i * size..$i * size + size].try_into().unwrap())
  }}
}

fn format_currency(dollars: f32, sig_figs: usize) -> String {
  let mut res = String::with_capacity(sig_figs + 4);

  if dollars as i32 >= i32::pow(10, sig_figs as u32) {
    res += &format!("{:.width$e}", dollars, width=sig_figs - 4);
  }
  else {
    let mut num_digits = 0;
    let mut tmp_dollars = dollars;
    while tmp_dollars > 1. {
      tmp_dollars /= 10.;
      num_digits += 1;
    }
    res += &format!("{:<width1$.width2$}", dollars, width1=num_digits, width2=max(sig_figs - num_digits, 1));
  }

  String::from(res.trim_end_matches('0').trim_end_matches('.'))
}

impl Table {
  pub fn read(&self, _from_ts: i64, _to_ts: i64) {
    for i in 0..self.row_index {
      for c in &self.columns {
        match c.r#type {
          ColumnType::TIMESTAMP => {
            let nanoseconds = read_bytes!(i64, c.file, i);
            let time: PrimitiveDateTime = date!(1970-01-01).midnight() + nanoseconds.nanoseconds();

            print!("{}", time.format("%Y-%m-%d %H:%M:%S.%N"));
          }
          ColumnType::CURRENCY => {
            print!("{:>9}", format_currency(read_bytes!(f32, c.file, i), 7));
          }
          ColumnType::SYMBOL8 => {
            let my = read_bytes!(u8, c.file, i);
            print!("{:7}", c.symbols[my as usize - 1]);
          }
          ColumnType::SYMBOL16 => {
            let my = read_bytes!(u16, c.file, i);
            print!("{:7}", c.symbols[my as usize - 1]);
          }
          ColumnType::SYMBOL32 => {
            let my = read_bytes!(u32, c.file, i);
            print!("{:7}", c.symbols[my as usize - 1]);
          }
          ColumnType::I32 => {
            print!("{}", read_bytes!(i32, c.file, i));
          }
          ColumnType::U32 => {
            print!("{}", read_bytes!(u32, c.file, i));
          }
          ColumnType::F32 => {
            print!("{:.2}", read_bytes!(f32, c.file, i));
          }
          ColumnType::I64 => {
            print!("{}", read_bytes!(i64, c.file, i));
          }
          ColumnType::U64 => {
            print!("{:>10}", read_bytes!(u64, c.file, i));
          }
          ColumnType::F64 => {
            print!("{}", read_bytes!(f64, c.file, i));
          }
        }
        print!(" ")
      }
      println!("")
    }
  }
}