use crate::{schema::ColumnType, table::Table};
use std::{cmp::max, convert::TryInto, mem::size_of, path::PathBuf};
use time::{date, NumericalDuration, PrimitiveDateTime};

macro_rules! read_bytes {
  ($_type:ty, $bytes:expr, $i:expr) => {{
    let size = size_of::<$_type>();
    <$_type>::from_le_bytes($bytes[$i * size..$i * size + size].try_into().unwrap())
  }};
}

fn format_currency(dollars: f32, sig_figs: usize) -> String {
  let mut res = String::with_capacity(sig_figs + 4);

  if dollars as i32 >= i32::pow(10, sig_figs as u32) {
    res += &format!("{:.width$e}", dollars, width = sig_figs - 4);
  } else {
    let mut num_digits = 0;
    let mut tmp_dollars = dollars;
    while tmp_dollars > 1. {
      tmp_dollars /= 10.;
      num_digits += 1;
    }
    res += &format!(
      "{:<width1$.width2$}",
      dollars,
      width1 = num_digits,
      width2 = max(sig_figs - num_digits, 1)
    );
  }

  String::from(res.trim_end_matches('0').trim_end_matches('.'))
}

impl Table {
  pub fn scan(&mut self, from_ts: i64, to_ts: i64) {
    let mut partitions = self
      .partition_meta
      .iter()
      .filter(|(_data_folder, partition_meta)| {
        if partition_meta.from_ts < from_ts || partition_meta.from_ts > to_ts {
          return false;
        }
        true
      })
      .collect::<Vec<_>>();
    partitions.sort_by_key(|(_data_folder, partition_meta)| partition_meta.from_ts);
    for (data_folder, _partition_meta) in partitions {
      self.data_folder = data_folder.to_owned();
      let mut partition_path = PathBuf::from(&self.data_path);
      partition_path.push(&self.data_folder);
      let columns = self.open_columns(&partition_path, 0);
      let row_count = self.get_row_count();
      for i in 0..row_count {
        for (j, c) in columns.iter().enumerate() {
          match c.r#type {
            ColumnType::TIMESTAMP => {
              let nanoseconds = read_bytes!(i64, c.data, i);
              if j == 0 && nanoseconds > to_ts {
                return;
              }
              let time: PrimitiveDateTime =
                date!(1970 - 01 - 01).midnight() + nanoseconds.nanoseconds();

              print!("{}", time.format("%Y-%m-%d %H:%M:%S.%N"));
            }
            ColumnType::CURRENCY => {
              print!("{:>9}", format_currency(read_bytes!(f32, c.data, i), 7));
            }
            ColumnType::SYMBOL8 => {
              let symbol_index = read_bytes!(u8, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::SYMBOL16 => {
              let symbol_index = read_bytes!(u16, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::SYMBOL32 => {
              let symbol_index = read_bytes!(u32, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::I32 => {
              print!("{}", read_bytes!(i32, c.data, i));
            }
            ColumnType::U32 => {
              print!("{}", read_bytes!(u32, c.data, i));
            }
            ColumnType::F32 => {
              print!("{:.2}", read_bytes!(f32, c.data, i));
            }
            ColumnType::I64 => {
              print!("{}", read_bytes!(i64, c.data, i));
            }
            ColumnType::U64 => {
              print!("{:>10}", read_bytes!(u64, c.data, i));
            }
            ColumnType::F64 => {
              print!("{}", read_bytes!(f64, c.data, i));
            }
          }
          print!(" ")
        }
        println!("")
      }
    }
  }

  pub fn scan_all(&mut self) { self.scan(std::i64::MIN, std::i64::MAX) }
}
