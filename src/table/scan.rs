use crate::{
  calendar::ToNaiveDateTime,
  schema::{Column, ColumnType},
  table::Table
};
use chrono::NaiveDateTime;
use std::{cmp::max, convert::TryInto, fmt::Debug, mem::size_of, path::PathBuf};

// Important that this fits in single register.
#[derive(Copy, Clone)]
pub union RowValue<'a> {
  pub sym: &'a String,
  pub i32: i32,
  pub u32: u32,
  pub f32: f32,
  pub i64: i64,
  pub u64: u64,
  pub f64: f64
}

impl Debug for RowValue<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("{:x}", self.get_i64()))
  }
}

impl RowValue<'_> {
  pub fn get_timestamp(&self) -> NaiveDateTime {
    let nanoseconds = unsafe { self.i64 };
    nanoseconds.to_naive_date_time()
  }

  pub fn get_currency(&self) -> f32 { unsafe { self.f32 } }

  pub fn get_symbol(&self) -> &String { unsafe { self.sym } }

  pub fn get_i32(&self) -> i32 { unsafe { self.i32 } }

  pub fn get_u32(&self) -> u32 { unsafe { self.u32 } }

  pub fn get_f32(&self) -> f32 { unsafe { self.f32 } }

  pub fn get_i64(&self) -> i64 { unsafe { self.i64 } }

  pub fn get_u64(&self) -> u64 { unsafe { self.u64 } }

  pub fn get_f64(&self) -> f64 { unsafe { self.f64 } }
}

pub trait FormatCurrency {
  fn format_currency(self, sig_figs: usize) -> String;
}

impl FormatCurrency for f32 {
  fn format_currency(self, sig_figs: usize) -> String {
    let mut res = String::with_capacity(sig_figs + 4);

    if self as i32 >= i32::pow(10, sig_figs as u32) {
      res += &format!("{:.width$e}", self, width = sig_figs - 4);
    } else {
      let mut num_digits = 0;
      let mut tmp_dollars = self;
      while tmp_dollars > 1. {
        tmp_dollars /= 10.;
        num_digits += 1;
      }
      res += &format!(
        "{:<width1$.width2$}",
        self,
        width1 = num_digits,
        width2 = max(sig_figs - num_digits, 1)
      );
    }

    String::from(res.trim_end_matches('0').trim_end_matches('.'))
  }
}

macro_rules! read_bytes {
  ($_type:ty, $bytes:expr, $i:expr) => {{
    let size = size_of::<$_type>();
    <$_type>::from_le_bytes($bytes[$i * size..$i * size + size].try_into().unwrap())
  }};
}

struct TableColumn<'a> {
  column:  Column,
  symbols: &'a Vec<String>
}

impl Table {
  fn get_union<'a>(&'a self, columns: &Vec<&str>) -> Vec<TableColumn<'a>> {
    columns
      .iter()
      .map(|col_name| {
        let index = self
          .schema
          .columns
          .iter()
          .position(|col| &col.name == col_name)
          .expect(&format!("Column {} does not exist", col_name));
        TableColumn {
          column:  self.schema.columns[index].clone(),
          symbols: &self.column_symbols[index].symbols
        }
      })
      .collect::<Vec<_>>()
  }

  pub fn scan<F>(&self, from_ts: i64, to_ts: i64, columns: Vec<&str>, mut accumulator: F)
  where
    F: FnMut(Vec<RowValue>)
  {
    let mut partitions = self
      .partition_meta
      .iter()
      .filter(|(_data_folder, partition_meta)| {
        partition_meta.from_ts >= from_ts && partition_meta.from_ts < to_ts
      })
      .collect::<Vec<_>>();
    partitions.sort_by_key(|(_data_folder, partition_meta)| partition_meta.from_ts);

    let columns = self.get_union(&columns);

    for (data_folder, partition_meta) in partitions {
      let mut partition_path = PathBuf::from(&self.data_path);
      partition_path.push(&data_folder);
      let row_count = partition_meta.row_count;

      let data_columns = columns
        .iter()
        .map(|column| self.open_column(&partition_path, row_count, &column.column))
        .collect::<Vec<_>>();
      for row_index in 0..row_count {
        let mut row = Vec::<RowValue>::with_capacity(data_columns.len());
        for (col_index, table_column) in data_columns.iter().enumerate() {
          let data = &table_column.data;
          match table_column.r#type {
            ColumnType::TIMESTAMP => {
              let nanoseconds = read_bytes!(i64, data, row_index);
              if col_index == 0 && nanoseconds > to_ts {
                return;
              }
              row.push(RowValue { i64: nanoseconds });
            }
            ColumnType::CURRENCY => {
              let f32 = read_bytes!(f32, data, row_index);
              row.push(RowValue { f32 });
            }
            ColumnType::SYMBOL8 => {
              let symbol_index = read_bytes!(u8, data, row_index) as usize;
              let sym = &columns[col_index].symbols[symbol_index - 1];
              row.push(RowValue { sym });
            }
            ColumnType::SYMBOL16 => {
              let symbol_index = read_bytes!(u16, data, row_index) as usize;
              let sym = &columns[col_index].symbols[symbol_index - 1];
              row.push(RowValue { sym });
            }
            ColumnType::SYMBOL32 => {
              let symbol_index = read_bytes!(u32, data, row_index) as usize;
              let sym = &columns[col_index].symbols[symbol_index - 1];
              row.push(RowValue { sym });
            }
            ColumnType::I32 => {
              let i32 = read_bytes!(i32, data, row_index);
              row.push(RowValue { i32 });
            }
            ColumnType::U32 => {
              let u32 = read_bytes!(u32, data, row_index);
              row.push(RowValue { u32 });
            }
            ColumnType::F32 => {
              let f32 = read_bytes!(f32, data, row_index);
              row.push(RowValue { f32 });
            }
            ColumnType::I64 => {
              let i64 = read_bytes!(i64, data, row_index);
              row.push(RowValue { i64 });
            }
            ColumnType::U64 => {
              let u64 = read_bytes!(u64, data, row_index);
              row.push(RowValue { u64 });
            }
            ColumnType::F64 => {
              let f64 = read_bytes!(f64, data, row_index);
              row.push(RowValue { f64 });
            }
          }
        }
        accumulator(row);
      }
    }
  }

  // pub fn scan_from(&mut self, from_ts: i64, to_ts: i64) { self.scan_filters(from_ts, to_ts, vec![])}
  // pub fn scan_all(&mut self) { self.scan_from(std::i64::MIN, std::i64::MAX) }
}
