use crate::{schema::ColumnType, table::Table};
use std::convert::TryInto;

impl Table {
  pub fn read(&self, _from_ts: u64, _to_ts: u64) {
    for i in 0..self.row_index {
      for c in &self.columns {
        match c.r#type {
            ColumnType::TIMESTAMP => {
              let my = u64::from_le_bytes(c.file[i * 8..i * 8 + 8].try_into().unwrap());
              print!("{}", my);
            }
            ColumnType::CURRENCY => {
              let my = f32::from_le_bytes(c.file[i * 4..i * 4 + 4].try_into().unwrap());
              print!("{:.2}", my);
            }
            ColumnType::SYMBOL8 => {
              let my = u8::from_le_bytes(c.file[i..i + 1].try_into().unwrap());
              print!("{}", c.symbols[my as usize - 1]);
            }
            ColumnType::SYMBOL16 => {
              let my = u16::from_le_bytes(c.file[i * 2..i * 2 + 2].try_into().unwrap());
              print!("{}", c.symbols[my as usize - 1]);
            }
            ColumnType::SYMBOL32 => {
              let my = u32::from_le_bytes(c.file[i * 4..i * 4 + 4].try_into().unwrap());
              print!("{}", c.symbols[my as usize - 1]);
            }
            ColumnType::I32 => {
              let my = i32::from_le_bytes(c.file[i * 4..i * 4 + 4].try_into().unwrap());
              print!("{}", my);
            }
            ColumnType::U32 => {
              let my = u32::from_le_bytes(c.file[i * 4..i * 4 + 4].try_into().unwrap());
              print!("{}", my);
            }
            ColumnType::F32 => {
              let my = f32::from_le_bytes(c.file[i * 4..i * 4 + 4].try_into().unwrap());
              print!("{:.2}", my);
            }
            ColumnType::I64 => {
              let my = i64::from_le_bytes(c.file[i * 8..i * 8 + 8].try_into().unwrap());
              print!("{}", my);
            }
            ColumnType::U64 => {
              let my = u64::from_le_bytes(c.file[i * 8..i * 8 + 8].try_into().unwrap());
              print!("{}", my);
            }
            ColumnType::F64 => {
              let my = f64::from_le_bytes(c.file[i * 8..i * 8 + 8].try_into().unwrap());
              print!("{}", my);
            }
        }
        print!(" ")
      }
      println!("")
    }
  }
}