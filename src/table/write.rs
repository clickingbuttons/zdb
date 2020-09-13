use crate::table::meta::write_meta;
use crate::table::util::TableColumn;
use crate::{schema::ColumnType, table::Table};
use std::{io::Write, fs::OpenOptions};
use memmap;

pub fn get_row_size(r#type: ColumnType) -> usize {
  match r#type {
    ColumnType::TIMESTAMP => 8,
    ColumnType::CURRENCY => 4,
    ColumnType::SYMBOL8 => 1,
    ColumnType::SYMBOL16 => 2,
    ColumnType::SYMBOL32 => 4,
    ColumnType::I32 => 4,
    ColumnType::U32 => 4,
    ColumnType::F32 => 4,
    ColumnType::I64 => 8,
    ColumnType::U64 => 8,
    ColumnType::F64 => 8,
  }
}

fn write_symbols(column: &TableColumn) {
  let symbols_text = column.symbols.join("\n");
  let path = &column.symbols_path;

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

impl Table {
  // TODO: Use const generics once stable.
  // https://github.com/rust-lang/rust/issues/44580
  fn put_bytes(&mut self, bytes: &[u8]) {
    let size = bytes.len();
    let offset = self.row_index * size;
    self.columns[self.column_index].data[offset..offset + size].copy_from_slice(bytes);
    self.column_index += 1;
  }

  pub fn puttimestamp(&mut self, val: i64) {
    self.puti64(val);
  }
  pub fn putcurrency(&mut self, val: f32) {
    self.putf32(val);
  }
  pub fn putsymbol(&mut self, val: &str) {
    let column = &mut self.columns[self.column_index];
    let symbols = &mut column.symbols;
    let index = symbols.iter().position(|s| s == val); 
    let index = match index {
      Some(i) => i + 1,
      None => {
        symbols.push(String::from(val));
        symbols.len()
      }
    };
    match column.r#type {
      ColumnType::SYMBOL8 => {
        self.put_bytes(&(index as u8).to_le_bytes());
      },
      ColumnType::SYMBOL16 => {
        self.put_bytes(&(index as u16).to_le_bytes());
      },
      ColumnType::SYMBOL32 => {
        self.put_bytes(&(index as u32).to_le_bytes());
      },
      _ => {
        panic!(format!("Unsupported column type {:?}", column.r#type));
      }
    }
  }
  pub fn puti32(&mut self, val: i32) {
    self.put_bytes(&val.to_le_bytes());
  }
  pub fn putu32(&mut self, val: u32) {
    self.put_bytes(&val.to_le_bytes());
  }
  pub fn putf32(&mut self, val: f32) {
    self.put_bytes(&val.to_le_bytes());
  }
  pub fn puti64(&mut self, val: i64) {
    self.put_bytes(&val.to_le_bytes());
  }
  pub fn putu64(&mut self, val: u64) {
    self.put_bytes(&val.to_le_bytes());
  }
  pub fn putf64(&mut self, val: f64) {
    self.put_bytes(&val.to_le_bytes());
  }

  pub fn write(&mut self) {
    self.column_index = 0;
    self.row_index += 1;
    // Check if next write will be larger than file
    for c in &mut self.columns {
      let size = c.data.len();
      let row_size = get_row_size(c.r#type);
      if size <= row_size * self.row_index {
        let size = c.data.len() as u64;
        println!("{} -> {}", size, size * 2);
        // Unmap by dropping c.data
        drop(&c.data);
        // Grow file
        c.file.set_len(size * 2)
          .expect(&format!("Could not truncate {:?} to {}", c.file, size * 2));
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
    for column in &mut self.columns {
      column.data.flush().expect(
        &format!("Could not flush {:?}", column.path)
      );
      let row_size = get_row_size(column.r#type);
      // Leave a spot for the next insert
      let size = row_size * (self.row_index + 1);
      column.file.set_len(size as u64)
        .expect(&format!("Could not truncate {:?} to {} to save {} bytes on disk", column.file, size, column.data.len() - size));
      if column.symbols.len() > 0 {
        write_symbols(&column);
      }
    }
    write_meta(&self)
      .expect("Could not write meta file with row_index");
  }
}