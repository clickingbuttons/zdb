use crate::schema::{Column,ColumnType};
use memmap::MmapMut;
use std::{
  path::PathBuf,
  fs::OpenOptions,
  io::{BufReader,BufRead,ErrorKind},
fs::File};

pub fn get_data_path(name: &str) -> PathBuf {
  let mut path = PathBuf::from("data");
  path.push(name);
  path
}

pub fn get_meta_path(data_path: &PathBuf) -> PathBuf {
  let mut path = data_path.clone();
  path.push("_meta");
  path
}

fn get_col_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension(String::from(format!("{:?}", column.r#type).to_lowercase()));
  path
}

fn get_symbol_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension("symbols");
  path
}

fn get_column_data(path: &PathBuf, init: bool) -> (File, MmapMut) {
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(&path)
    .expect(&format!("Unable to open file {:?}", path));
  if init {
    // Allocate 1MB per-column to start
    let init_size = 1024;
    file.set_len(init_size)
      .expect(&format!("Could not truncate {:?} to {}", path, init_size));
  }
  unsafe {
    let data = memmap::MmapOptions::new()
      .map_mut(&file)
      .expect(&format!("Could not mmapp {:?}", path));

    (file, data)
  }
}

fn get_column_symbols(symbols_path: &PathBuf, column: &Column) -> Vec<String> {
  let capacity = match column.r#type {
    ColumnType::SYMBOL8 => 2 << 7,
    ColumnType::SYMBOL16 => 2 << 15,
    ColumnType::SYMBOL32 => 2 << 31,
    _ => 0
  };
  let mut symbols = Vec::<String>::with_capacity(capacity);
  if capacity > 0 {
    let file = OpenOptions::new()
      .read(true)
      .open(&symbols_path);
    match file {
      Ok(file) => {
        let f = BufReader::new(&file);
        for line in f.lines() {
          let my_line = line
            .expect(&format!("Could not read line from symbol file {:?}", symbols_path));
          symbols.push(my_line);
        }
      },
      Err(error) => {
        if error.kind() != ErrorKind::NotFound {
          panic!("Problem opening symbol file {:?}: {:?}", symbols_path, error)
        }
      }
    };
  }

  symbols
}

#[derive(Debug)]
pub struct TableColumn {
  pub file: File,
  pub data: memmap::MmapMut,
  pub path: PathBuf,
  pub symbols_path: PathBuf,
  pub symbols: Vec<String>,
  pub r#type: ColumnType
}

pub fn get_columns(data_path: &PathBuf, columns: &Vec<Column>, init: bool) -> Vec<TableColumn> {
  columns.iter()
    .map(|column| {
      let path = get_col_path(&data_path, &column);
      let symbols_path = get_symbol_path(&data_path, &column);
      let (file, data) = get_column_data(&path, init);
      TableColumn {
        file,
        data,
        symbols: get_column_symbols(&symbols_path, &column),
        symbols_path,
        path,
        r#type: column.r#type.clone()
      }
    })
    .collect::<Vec::<_>>()
}
