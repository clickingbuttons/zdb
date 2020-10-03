use crate::{schema::ColumnType, table::Table};
use memmap;
use std::{
  fs::{create_dir_all, OpenOptions},
  io::Write
};
use time::{date, NumericalDuration, PrimitiveDateTime};

use super::PartitionMeta;

impl PartitionMeta {
  pub fn increment_row_count(&mut self) -> usize {
    self.row_count += 1;
    self.row_count
  }
}

impl Table {
  // TODO: Use const generics once stable.
  // https://github.com/rust-lang/rust/issues/44580
  fn put_bytes(&mut self, bytes: &[u8]) {
    let size = bytes.len();
    let row_count = self.get_row_count();
    let offset = row_count * size;
    self.columns[self.column_index].data[offset..offset + size].copy_from_slice(bytes);
    self.column_index += 1;
  }

  fn get_partition_folder(&self, val: i64) -> String {
    let time: PrimitiveDateTime = date!(1970 - 01 - 01).midnight() + val.nanoseconds();

    time.format(&self.schema.partition_by)
  }

  pub fn get_row_count(&self) -> usize {
    self
      .partition_meta
      .get(&self.data_folder)
      .expect(&format!("No row count for {}", &self.data_folder))
      .row_count
  }

  pub fn put_timestamp(&mut self, val: i64) {
    if self.column_index == 0 {
      let partition_folder = self.get_partition_folder(val);
      if partition_folder != self.data_folder {
        self.data_folder = partition_folder;
        let mut data_path = self.data_path.clone();
        data_path.push(&self.data_folder);
        create_dir_all(&data_path).expect(&format!("Cannot create dir {:?}", &data_path));
        self.columns = self.open_columns(&data_path, 0);
      }

      match self.partition_meta.get_mut(&self.data_folder) {
        Some(meta) => {
          if val < meta.to_ts {
            panic!(format!(
              "Timestamp {} is out of order (previous ts is {})",
              val, meta.to_ts
            ));
          }
          meta.to_ts = val;
        }
        None => {
          let new_meta = PartitionMeta {
            from_ts:   val,
            to_ts:     val,
            row_count: 0
          };
          self
            .partition_meta
            .insert(self.data_folder.clone(), new_meta);
        }
      };
    }
    self.put_i64(val);
  }

  pub fn put_currency(&mut self, val: f32) { self.put_f32(val) }

  pub fn put_symbol(&mut self, val: &str) {
    let symbols = &mut self.column_symbols[self.column_index].symbols;
    let index = symbols.iter().position(|s| s == val);
    let index = match index {
      Some(i) => i + 1,
      None => {
        symbols.push(String::from(val));
        symbols.len()
      }
    };
    let column = &self.columns[self.column_index];
    match column.r#type {
      ColumnType::SYMBOL8 => {
        self.put_bytes(&(index as u8).to_le_bytes());
      }
      ColumnType::SYMBOL16 => {
        self.put_bytes(&(index as u16).to_le_bytes());
      }
      ColumnType::SYMBOL32 => {
        self.put_bytes(&(index as u32).to_le_bytes());
      }
      _ => {
        panic!(format!("Unsupported column type {:?}", column.r#type));
      }
    }
  }

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
        .expect(&format!("Could not open symbols file {:?}", path));
      f.write_all(symbols_text.as_bytes())
        .expect(&format!("Could not write to symbols file {:?}", path));
      f.flush()
        .expect(&format!("Could not flush to symbols file {:?}", path));
    }
  }

  pub fn write(&mut self) {
    self.column_index = 0;
    let partition_meta = self.partition_meta.get_mut(&self.data_folder).unwrap();
    let row_count = partition_meta.increment_row_count();
    // Check if next write contains ts
    // Check if next write will be larger than file
    for c in &mut self.columns {
      let size = c.data.len();
      let row_size = Table::get_row_size(c.r#type);
      if size <= row_size * row_count {
        let size = c.data.len() as u64;
        println!("{} -> {}", size, size * 2);
        // Unmap by dropping c.data
        drop(&c.data);
        // Grow file
        c.file.set_len(size * 2).expect(&format!(
          "Could not truncate {:?} to {}",
          c.file,
          size * 2
        ));
        // Map file again
        unsafe {
          c.data = memmap::MmapOptions::new()
            .map_mut(&c.file)
            .expect(&format!("Could not mmapp {:?}", c.file));
        }
        // TODO: remove memmap dep and use mremap on *nix
        // https://man7.org/linux/man-pages/man2/mremap.2.html
      }
    }
  }

  pub fn flush(&mut self) {
    let row_count = self.get_row_count();
    for column in &mut self.columns {
      column
        .data
        .flush()
        .expect(&format!("Could not flush {:?}", column.path));
      let row_size = Table::get_row_size(column.r#type);
      // Leave a spot for the next insert
      let size = row_size * (row_count + 1);
      column.file.set_len(size as u64).expect(&format!(
        "Could not truncate {:?} to {} to save {} bytes on disk",
        column.file,
        size,
        column.data.len() - size
      ));
    }
    self.write_symbols();
    self
      .write_meta()
      .expect("Could not write meta file with row_count");
  }
}
