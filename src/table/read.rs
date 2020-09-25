use crate::{schema::ColumnType, schema::Schema, table::Table};
use std::{path::PathBuf};
use std::{
  convert::TryInto,
  mem::size_of,
  cmp::max,
};
use time::{NumericalDuration,PrimitiveDateTime,date};
use std::iter::FromIterator;

use super::columns::{TableColumnSymbols, get_column_symbols, get_symbols_path};

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
  pub fn read(&mut self, _from_ts: i64, _to_ts: i64) {
    let mut partitions = Vec::from_iter(self.row_counts.keys().cloned());
    partitions.sort();
    for partition in partitions {
      self.partition_folder = partition;
      let mut partition_path = PathBuf::from(&self.data_path);
      partition_path.push(&self.partition_folder);
      let columns = self.get_columns(&partition_path, 0);
      let row_count = self.get_row_count();
      for i in 0..row_count {
        for (j, c) in columns.iter().enumerate() {
          match c.r#type {
            ColumnType::TIMESTAMP => {
              let nanoseconds = read_bytes!(i64, c.data, i);
              let time: PrimitiveDateTime = date!(1970-01-01).midnight() + nanoseconds.nanoseconds();
  
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
}

pub fn read_column_symbols(data_path: &PathBuf, schema: &Schema) -> Vec<TableColumnSymbols> {
  let mut res = Vec::new();

  for column in &schema.columns {
    let path = get_symbols_path(&data_path, &column);
    let col_syms = TableColumnSymbols {
      symbols: get_column_symbols(&path, &column),
      path,
    };
    res.push(col_syms);
  }

  res
}
