use crate::{
  schema::{Column, ColumnType, Schema},
  table::{Table, TableColumn, TableColumnSymbols}
};
use memmap::MmapMut;
use std::{
  fs::{File, OpenOptions},
  io::{BufRead, BufReader, ErrorKind},
  path::PathBuf
};

pub fn get_symbols_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension("symbols");
  path
}

pub fn get_capacity(column: &Column) -> usize {
  match column.r#type {
    ColumnType::SYMBOL8 => 2 << 7,
    ColumnType::SYMBOL16 => 2 << 15,
    ColumnType::SYMBOL32 => 2 << 31,
    _ => 0
  }
}

fn get_column_symbols(symbols_path: &PathBuf, column: &Column) -> Vec<String> {
  let capacity = get_capacity(&column);
  if capacity == 0 {
    return Vec::new();
  }
  let mut symbols = Vec::<String>::with_capacity(capacity);
  let file = OpenOptions::new().read(true).open(&symbols_path);
  match file {
    Ok(file) => {
      let f = BufReader::new(&file);
      for line in f.lines() {
        let my_line = line.expect(&format!(
          "Could not read line from symbol file {:?}",
          symbols_path
        ));
        symbols.push(my_line);
      }
    }
    Err(error) => {
      if error.kind() != ErrorKind::NotFound {
        panic!(
          "Problem opening symbol file {:?}: {:?}",
          symbols_path, error
        )
      }
    }
  };

  symbols
}

pub fn read_column_symbols(data_path: &PathBuf, schema: &Schema) -> Vec<TableColumnSymbols> {
  let mut res = Vec::new();

  for column in &schema.columns {
    let path = get_symbols_path(&data_path, &column);
    let col_syms = TableColumnSymbols {
      symbols: get_column_symbols(&path, &column),
      path
    };
    res.push(col_syms);
  }

  res
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
  let init_size = row_count * Table::get_row_size(column_type) + 1024 * 1024 * 1024;
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
  pub fn open_columns(&self, partition_path: &PathBuf, row_count: usize) -> Vec<TableColumn> {
    self
      .schema
      .columns
      .iter()
      .map(|column| {
        let path = get_col_path(&partition_path, &column);
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
