use crate::{
  schema::{Column, ColumnType, Schema},
  table::{Table, TableColumn, TableColumnSymbols}
};
use fnv::FnvHashMap;
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
        let my_line = line.unwrap_or_else(|_| panic!(
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
    let symbols = get_column_symbols(&path, &column);
    let mut symbol_nums =
      FnvHashMap::with_capacity_and_hasher(get_capacity(&column), Default::default());
    for (i, symbol) in symbols.iter().enumerate() {
      symbol_nums.insert(symbol.clone(), i + 1);
    }
    let col_syms = TableColumnSymbols {
      symbols,
      symbol_nums,
      path
    };
    res.push(col_syms);
  }

  res
}

fn get_col_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension(String::from(format!("{}", column.r#type).to_lowercase()));
  path
}

fn get_column_data(path: &PathBuf, row_count: usize, column_type: ColumnType) -> (File, MmapMut) {
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(&path)
    .unwrap_or_else(|_| panic!("Unable to open file {:?}", path));
  
  let init_size = (row_count + 1) * Table::get_row_size(column_type);
  file
    .set_len(init_size as u64)
    .unwrap_or_else(|_| panic!("Could not truncate {:?} to {}", path, init_size));
  unsafe {
    let data = memmap::MmapOptions::new()
      .map_mut(&file)
      .unwrap_or_else(|_| panic!("Could not mmapp {:?}", path));

    (file, data)
  }
}

impl Table {
  pub fn open_column(
    &self,
    partition_path: &PathBuf,
    row_count: usize,
    column: &Column
  ) -> TableColumn {
    let path = get_col_path(&partition_path, &column);
    let (file, data) = get_column_data(&path, row_count, column.r#type);
    TableColumn {
      name: column.name.clone(),
      file,
      data,
      path,
      r#type: column.r#type.clone()
    }
  }

  pub fn open_columns(&self, partition_path: &PathBuf, extra_row_count: usize) -> Vec<TableColumn> {
    let row_count = match self.partition_meta.get(&self.data_folder) {
      Some(meta) => meta.row_count,
      None => 0
    } + extra_row_count;

    self
      .schema
      .columns
      .iter()
      .map(|column| self.open_column(partition_path, row_count, column))
      .collect::<Vec<_>>()
  }
}
