use crate::{
  calendar::ToNaiveDateTime,
  schema::{ColumnType, PartitionBy},
  table::Table
};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, MAX_DATETIME, MIN_DATETIME};
use memmap;
use std::{
  fs::{create_dir_all, OpenOptions},
  io::Write
};

use super::PartitionMeta;

impl Table {
  // TODO: Use const generics once stable.
  // https://github.com/rust-lang/rust/issues/44580
  fn put_bytes(&mut self, bytes: &[u8]) {
    let size = bytes.len();
    let offset = self.cur_partition_meta.row_count * size;
    // println!("put_bytes {} {}", self.column_index, self.cur_partition_meta.row_count);
    self.columns[self.column_index].data[offset..offset + size].copy_from_slice(bytes);
    self.column_index += 1;
  }

  fn get_partition_dir(&self, val: i64) -> String {
    let datetime: NaiveDateTime = val.to_naive_date_time();

    // Specifiers: https://docs.rs/chrono/0.3.1/chrono/format/strftime/index.html
    match self.schema.partition_by {
      PartitionBy::None => String::from("all"),
      PartitionBy::Year => datetime.format("%Y").to_string(),
      PartitionBy::Month => datetime.format("%Y-%m").to_string(),
      PartitionBy::Day => datetime.format("%Y-%m-%d").to_string()
    }
  }

  fn get_partition_ts(&self, date: NaiveDateTime, offset: i32) -> i64 {
    match self.schema.partition_by {
      PartitionBy::None => {
        if offset == 0 {
          MIN_DATETIME.naive_utc()
        } else {
          MAX_DATETIME.naive_utc()
        }
      }
      PartitionBy::Year => NaiveDate::from_ymd(date.year() + offset, 1, 1).and_hms(0, 0, 0),
      PartitionBy::Month => {
        let mut year = date.year();
        let mut month = date.month() + offset as u32;
        if month > 12 || month < 1 {
          month = month % 12;
          year += offset;
        }
        NaiveDate::from_ymd(year, month, 1).and_hms(0, 0, 0)
      }
      PartitionBy::Day => (date.date() + Duration::days(offset as i64)).and_hms(0, 0, 0)
    }
    .timestamp_nanos()
  }

  pub fn put_timestamp(&mut self, mut val: i64) {
    let resolution = self.schema.columns[self.column_index].resolution;
    // Round off for partition calculation
    val = val / resolution * resolution;
    if self.column_index == 0 {
      // New partition?
      if val > self.cur_partition_meta.max_ts
        || val < self.cur_partition_meta.min_ts
        || self.cur_partition_meta.row_count == 0
      {
        // Save old partition meta
        self.save_cur_partition_meta();
        // Load new partition meta
        let is_first_partition = self.cur_partition.is_empty();
        self.cur_partition = self.get_partition_dir(val);
        self.cur_partition_meta = match self.partition_meta.get_mut(&self.cur_partition) {
          Some(meta) => {
            if val < meta.to_ts {
              panic!(format!(
                "Timestamp {} is out of order (previous ts is {})",
                val, meta.to_ts
              ));
            }
            meta.clone()
          }
          None => {
            self.dir_index = if is_first_partition {
              0
            } else {
              (self.dir_index + 1) % self.schema.partition_dirs.len()
            };
            let mut partition_dir = self.schema.partition_dirs[self.dir_index].clone();
            partition_dir.push(&self.schema.name);
            partition_dir.push(&self.cur_partition);
            let date = val.to_naive_date_time();
            let min_ts = self.get_partition_ts(date, 0);
            let max_ts = self.get_partition_ts(date, 1) - 1;
            PartitionMeta {
              dir: partition_dir,
              from_ts: val,
              to_ts: val,
              min_ts,
              max_ts,
              row_count: 0
            }
          }
        };
        // Open new columns
        create_dir_all(&self.cur_partition_meta.dir)
          .unwrap_or_else(|_| panic!("Cannot create dir {:?}", &self.cur_partition_meta.dir));
        // Expect 10m more rows in partition
        self.columns = self.open_columns(&self.cur_partition_meta.dir, 10_000_000);
      }
      self.cur_partition_meta.to_ts = val;
    }
    match self.schema.columns[self.column_index].size {
      8 => self.put_i64(val),
      4 => self.put_u32(((val - self.cur_partition_meta.min_ts) / resolution) as u32),
      2 => self.put_u16(((val - self.cur_partition_meta.min_ts) / resolution) as u16),
      1 => self.put_u8(((val - self.cur_partition_meta.min_ts) / resolution) as u8),
      s => panic!(format!("Invalid column size {}", s))
    };
  }

  pub fn put_currency(&mut self, val: f32) { self.put_f32(val) }

  pub fn put_symbol(&mut self, val: String) {
    let column_symbols = &mut self.column_symbols[self.column_index];
    let symbol_nums = &mut column_symbols.symbol_nums;
    let index = match symbol_nums.get(&val) {
      Some(i) => *i,
      None => {
        let symbols = &mut column_symbols.symbols;
        symbol_nums.insert(val.clone(), symbols.len());
        symbols.push(val);
        symbols.len() - 1
      }
    };
    let column = &self.columns[self.column_index];
    match column.r#type {
      ColumnType::Symbol8 => self.put_u8(index as u8),
      ColumnType::Symbol16 => self.put_u16(index as u16),
      ColumnType::Symbol32 => self.put_u32(index as u32),
      bad_type => panic!(format!("Unsupported column type {:?}", bad_type))
    }
  }

  pub fn put_i8(&mut self, val: i8) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_u8(&mut self, val: u8) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_i16(&mut self, val: i16) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_u16(&mut self, val: u16) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_i32(&mut self, val: i32) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_u32(&mut self, val: u32) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_f32(&mut self, val: f32) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_i64(&mut self, val: i64) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_u64(&mut self, val: u64) { self.put_bytes(&val.to_le_bytes()) }

  pub fn put_f64(&mut self, val: f64) { self.put_bytes(&val.to_le_bytes()) }

  fn write_symbols(&self) {
    for table_col_symbols in &self.column_symbols {
      if table_col_symbols.symbols.len() == 0 {
        continue;
      }
      let symbols_text = table_col_symbols.symbols.join("\n");
      let path = &table_col_symbols.path;

      let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .unwrap_or_else(|_| panic!("Could not open symbols file {:?}", path));
      f.write_all(symbols_text.as_bytes())
        .unwrap_or_else(|_| panic!("Could not write to symbols file {:?}", path));
      f.flush()
        .unwrap_or_else(|_| panic!("Could not flush to symbols file {:?}", path));
    }
  }

  pub fn write(&mut self) {
    self.column_index = 0;
    self.cur_partition_meta.row_count += 1;
    // Check if next write will be larger than file
    for c in &mut self.columns {
      let size = c.data.len();
      let row_size = c.size;
      if size <= row_size * (self.cur_partition_meta.row_count + 1) {
        let size = c.data.len() as u64;
        // println!("Grow {} from {} to {}", c.name, size, size * 2);
        // Unmap by dropping c.data
        drop(&c.data);
        // Grow file
        c.file
          .set_len(size * 2)
          .unwrap_or_else(|_| panic!("Could not truncate {:?} to {}", c.file, size * 2));
        // Map file again
        unsafe {
          c.data = memmap::MmapOptions::new()
            .map_mut(&c.file)
            .unwrap_or_else(|_| panic!("Could not mmapp {:?}", c.file));
        }
        // TODO: remove memmap dep and use mremap on *nix
        // https://man7.org/linux/man-pages/man2/mremap.2.html
      }
    }
  }

  pub fn flush(&mut self) {
    for column in &mut self.columns {
      column
        .data
        .flush()
        .unwrap_or_else(|_| panic!("Could not flush {:?}", column.path));
      // Leave a spot for the next insert
      let size = column.size * (self.cur_partition_meta.row_count + 1);
      column.file.set_len(size as u64).unwrap_or_else(|_| {
        panic!(
          "Could not truncate {:?} to {} to save {} bytes on disk",
          column.file,
          size,
          column.data.len() - size
        )
      });
    }
    self.write_symbols();
    self.save_cur_partition_meta();
    self
      .write_meta()
      .expect("Could not write meta file with row_count");
  }
}
