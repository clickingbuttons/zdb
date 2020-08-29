use crate::table::meta::write_meta;
use crate::{schema::ColumnType, table::Table};
use std::{io::Write, fs::OpenOptions};

impl Table {
  fn putbyte(&mut self, byte: u8) {
    let offset = self.row_index;
    self.columns[self.column_index].file[offset] = byte;
    self.column_index += 1;
  }
  fn put2bytes(&mut self, bytes: &[u8; 2]) {
    let offset = self.row_index * 2;
    self.columns[self.column_index].file[offset..offset + 2].copy_from_slice(bytes);
    self.column_index += 1;
  }
  fn put4bytes(&mut self, bytes: &[u8; 4]) {
    let offset = self.row_index * 4;
    self.columns[self.column_index].file[offset..offset + 4].copy_from_slice(bytes);
    self.column_index += 1;
  }
  fn put8bytes(&mut self, bytes: &[u8; 8]) {
    let offset = self.row_index * 8;
    self.columns[self.column_index].file[offset..offset + 8].copy_from_slice(bytes);
    self.column_index += 1;
  }

  pub fn puttimestamp(&mut self, val: u64) {
    self.putu64(val);
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
      ColumnType::SYMBOL8 => self.putbyte(index as u8),
      ColumnType::SYMBOL16 => self.put2bytes(&(index as u16).to_le_bytes()),
      ColumnType::SYMBOL32 => self.put4bytes(&(index as u32).to_le_bytes()),
      _ => {}
    }
  }
  pub fn puti32(&mut self, val: i32) {
    self.put4bytes(&val.to_le_bytes());
  }
  pub fn putu32(&mut self, val: u32) {
    self.put4bytes(&val.to_le_bytes());
  }
  pub fn putf32(&mut self, val: f32) {
    self.put4bytes(&val.to_le_bytes());
  }
  pub fn puti64(&mut self, val: i64) {
    self.put8bytes(&val.to_le_bytes());
  }
  pub fn putu64(&mut self, val: u64) {
    self.put8bytes(&val.to_le_bytes());
  }
  pub fn putf64(&mut self, val: f64) {
    self.put8bytes(&val.to_le_bytes());
  }

  pub fn write(&mut self) {
    self.column_index = 0;
    self.row_index += 1;
  }
  pub fn flush(&mut self) {
    for column in &mut self.columns {
      column.file.flush().expect(
        &format!("Could not flush {:?}", column.path)
      );
      if column.symbols.len() > 0 {
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
    }
    write_meta(&self)
      .expect("Could not write meta file with row_index");
  }
}