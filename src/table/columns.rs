use crate::{
  schema::{Column, ColumnType},
  table::{write::get_row_size, Table}
};
use memmap::MmapMut;
use std::{
  fs::{File, OpenOptions},
  path::PathBuf
};

#[derive(Debug)]
pub struct TableColumnSymbols {
  pub path: PathBuf,
  pub symbols: Vec<String>
}

#[derive(Debug)]
pub struct TableColumn {
  pub name: String,
  pub file: File,
  pub data: memmap::MmapMut,
  pub path: PathBuf,
  pub r#type: ColumnType
}

fn get_col_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension(String::from(format!("{:?}", column.r#type).to_lowercase()));
  path
}

fn get_column_data(path: &PathBuf, row_count: usize, column_type: ColumnType) -> (File, MmapMut) {
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(&path)
    .expect(&format!("Unable to open file {:?}", path));
  // Allocate extra 1GB per column (expect some writes)
  let init_size = row_count * get_row_size(column_type) + 1024 * 1024 * 1024;
  file
    .set_len(init_size as u64)
    .expect(&format!("Could not truncate {:?} to {}", path, init_size));
  unsafe {
    let data = memmap::MmapOptions::new()
      .map_mut(&file)
      .expect(&format!("Could not mmapp {:?}", path));

    (file, data)
  }
}

impl Table {
  pub fn open_columns(&self, data_path: &PathBuf, row_count: usize) -> Vec<TableColumn> {
    self
      .schema
      .columns
      .iter()
      .map(|column| {
        let path = get_col_path(&data_path, &column);
        let (file, data) = get_column_data(&path, row_count, column.r#type);
        TableColumn {
          name: column.name.clone(),
          file,
          data,
          path,
          r#type: column.r#type.clone()
        }
      })
      .collect::<Vec<_>>()
  }
}
