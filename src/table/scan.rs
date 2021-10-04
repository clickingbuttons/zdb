use crate::{
  schema::{Column, ColumnType},
  table::{PartitionMeta, Table, TableColumn}
};
use std::{cmp::max, fmt::Debug, slice::from_raw_parts_mut};

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

#[derive(Debug)]
struct TableColumnMeta<'a> {
  column:  Column,
  symbols: &'a Vec<String>
}

impl Table {
  fn get_union<'a>(&'a self, columns: &Vec<&str>) -> Vec<TableColumnMeta<'a>> {
    columns
      .iter()
      .map(|col_name| {
        let index = self
          .schema
          .columns
          .iter()
          .position(|col| &col.name == col_name)
          .unwrap_or_else(|| panic!("Column {} does not exist", col_name));
        TableColumnMeta {
          column:  self.schema.columns[index].clone(),
          symbols: &self.column_symbols[index].symbols
        }
      })
      .collect::<Vec<_>>()
  }

  /* Inclusive of from and to */
  pub fn partition_iter(&self, from_ts: i64, to_ts: i64, columns: Vec<&str>) -> PartitionIterator {
    assert!(to_ts >= from_ts);
    let mut partitions = self
      .partition_meta
      .iter()
      .filter(|(_partition_dir, partition_meta)| {
        // Start
        (from_ts >= partition_meta.from_ts && from_ts <= partition_meta.to_ts) ||
        // Middle
        (from_ts < partition_meta.from_ts && to_ts > partition_meta.to_ts) ||
        // End
        (to_ts >= partition_meta.from_ts && to_ts <= partition_meta.to_ts)
      })
      .collect::<Vec<(&String, &PartitionMeta)>>();
    partitions.sort_by_key(|(_partition_dir, partition_meta)| partition_meta.from_ts);
    let ts_column = self.schema.columns[0].clone();

    PartitionIterator {
      from_ts,
      to_ts,
      ts_column,
      columns: self.get_union(&columns),
      partitions,
      partition_index: 0,
      table_name: self.schema.name.clone()
    }
  }
}

#[derive(Debug)]
pub struct PartitionColumn<'a> {
  pub column:    TableColumn,
  pub slice:     &'a mut [u8],
  pub symbols:   &'a Vec<String>,
  pub meta:      &'a PartitionMeta,
  pub row_count: usize
}

macro_rules! get_partition_slice {
  ($slice: expr, $_type: ty) => {
    unsafe {
      from_raw_parts_mut(
        $slice.as_ptr() as *mut $_type,
        $slice.len() / std::mem::size_of::<$_type>()
      )
    }
  };
}

impl<'a> PartitionColumn<'_> {
  pub fn get_i8(&self) -> &mut [i8] { get_partition_slice!(self.slice, i8) }

  pub fn get_u8(&self) -> &mut [u8] { get_partition_slice!(self.slice, u8) }

  pub fn get_i16(&self) -> &mut [i16] { get_partition_slice!(self.slice, i16) }

  pub fn get_u16(&self) -> &mut [u16] { get_partition_slice!(self.slice, u16) }

  pub fn get_i32(&self) -> &mut [i32] { get_partition_slice!(self.slice, i32) }

  pub fn get_u32(&self) -> &mut [u32] { get_partition_slice!(self.slice, u32) }

  pub fn get_i64(&self) -> &mut [i64] { get_partition_slice!(self.slice, i64) }

  pub fn get_u64(&self) -> &mut [u64] { get_partition_slice!(self.slice, u64) }

  pub fn get_f32(&self) -> &mut [f32] { get_partition_slice!(self.slice, f32) }

  pub fn get_f64(&self) -> &mut [f64] { get_partition_slice!(self.slice, f64) }

  pub fn get_symbol(&self, row_index: usize) -> &str {
    match self.column.r#type {
      ColumnType::Symbol8 => &self.symbols[self.get_u8()[row_index] as usize],
      ColumnType::Symbol16 => &self.symbols[self.get_u16()[row_index] as usize],
      ColumnType::Symbol32 => &self.symbols[self.get_u32()[row_index] as usize],
      ctype => panic!("ColumnType {:?} is not a Symbol", ctype)
    }
  }

  pub fn to_timestamp(&self, v: i64) -> i64 {
    match self.column.size {
      8 => v,
      4 | 2 => v * self.column.resolution + self.meta.min_ts,
      csize => panic!("Size {:?} is not a supported Timestamp size", csize)
    }
  }

  pub fn get_timestamp(&self, row_index: usize) -> i64 {
    if self.column.r#type != ColumnType::Timestamp {
      panic!("ColumnType {:?} is not a Timestamp", self.column.r#type);
    }

    match self.column.size {
      8 => self.get_i64()[row_index],
      4 => self.to_timestamp(self.get_u32()[row_index] as i64),
      2 => self.to_timestamp(self.get_u16()[row_index] as i64),
      csize => panic!("Size {:?} is not a supported Timestamp size", csize)
    }
  }
}

#[derive(Debug)]
pub struct PartitionIterator<'a> {
  from_ts: i64,
  to_ts: i64,
  ts_column: Column,
  columns: Vec<TableColumnMeta<'a>>,
  table_name: String,
  pub partitions: Vec<(&'a String, &'a PartitionMeta)>,
  partition_index: usize
}

macro_rules! binary_search_seek {
  ($ts_column: expr, $len: expr, $needle: expr, $seek_start: expr, $_type: ty) => {{
    let needle = $needle as $_type;
    unsafe {
      let data = from_raw_parts_mut($ts_column.data.as_ptr() as *mut $_type, $len);
      let mut index = data.binary_search(&needle);
      if let Ok(ref mut i) = index {
        // Seek to beginning/end
        if $seek_start {
          while *i > 0 && data[*i - 1] == needle {
            *i -= 1;
          }
        } else {
          while *i < data.len() - 1 && data[*i + 1] == needle {
            *i += 1;
          }
          // This is going to be used as an end index
          *i += 1;
        }
      }
      index
    }
  }};
}

fn find_ts(ts_column: &TableColumn, ts: i64, seek_start: bool) -> usize {
  let needle = ts / ts_column.resolution;
  let len = ts_column.data.len() / ts_column.size;
  let search_results = match ts_column.size {
    8 => binary_search_seek!(ts_column, len, needle, seek_start, i64),
    4 => binary_search_seek!(ts_column, len, needle, seek_start, u32),
    2 => binary_search_seek!(ts_column, len, needle, seek_start, u16),
    1 => binary_search_seek!(ts_column, len, needle, seek_start, u8),
    s => panic!("Invalid column size {}", s)
  };
  match search_results {
    Ok(n) => n,
    Err(n) => n
  }
}

impl<'a> Iterator for PartitionIterator<'a> {
  type Item = Vec<PartitionColumn<'a>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.partition_index == self.partitions.len() {
      return None;
    }
    let (partition_dir, partition_meta) = self.partitions.get(self.partition_index)?;
    let start_row = if self.partition_index == 0 {
      let ts_column = Table::open_column(
        &partition_meta.dir,
        &self.table_name,
        &partition_dir,
        partition_meta.row_count,
        &self.ts_column
      );
      let needle = if ts_column.resolution == 1 { self.from_ts } else { self.from_ts - partition_meta.min_ts };
      find_ts(&ts_column, needle, true)
    } else {
      0
    };
    let end_row = if self.partition_index == self.partitions.len() - 1 {
      let ts_column = Table::open_column(
        &partition_meta.dir,
        &self.table_name,
        &partition_dir,
        partition_meta.row_count,
        &self.ts_column
      );
      let needle = if ts_column.resolution == 1 { self.to_ts } else { self.to_ts - partition_meta.min_ts };
      find_ts(&ts_column, needle, false)
    } else {
      partition_meta.row_count
    };
    let data_columns = self
      .columns
      .iter()
      .map(|column| {
        let table_column = Table::open_column(
          &partition_meta.dir,
          &self.table_name,
          &partition_dir,
          partition_meta.row_count,
          &column.column
        );
        let slice = unsafe {
          from_raw_parts_mut(
            table_column
              .data
              .as_ptr()
              .add(start_row * table_column.size) as *mut u8,
            (end_row - start_row) * table_column.size
          )
        };

        PartitionColumn {
          slice,
          column: table_column,
          symbols: column.symbols,
          meta: partition_meta,
          row_count: end_row - start_row
        }
      })
      .collect::<Vec<_>>();

    self.partition_index += 1;
    return Some(data_columns);
  }
}

#[cfg(test)]
mod tests {
  use std::slice::from_raw_parts_mut;
  struct TestColumn<'a> {
    data: &'a [i64]
  }
  #[test]
  fn test_binary_search_seek() {
    let data = TestColumn {
      data: &[1, 2, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5, 5, 5, 6, 7, 8, 10]
    };
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 2, true, i64),
      Ok(1)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 2, false, i64),
      Ok(5 + 1)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 5, true, i64),
      Ok(8)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 5, false, i64),
      Ok(13 + 1)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 9, false, i64),
      Err(data.data.len() - 1)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 10, false, i64),
      Ok(data.data.len())
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 21, false, i64),
      Err(data.data.len())
    );

    let data = TestColumn { data: &[1] };
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 1, true, i64),
      Ok(0)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 1, false, i64),
      Ok(1)
    );

    let data = TestColumn { data: &[] };
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 1, true, i64),
      Err(0)
    );
    assert_eq!(
      binary_search_seek!(data, data.data.len(), 1, false, i64),
      Err(0)
    );
  }
}
