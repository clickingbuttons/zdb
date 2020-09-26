mod columns;
mod meta;
mod read;
mod write;

use crate::schema::*;
// "meta" crate is reserved
// https://internals.rust-lang.org/t/is-the-module-name-meta-forbidden/9587/3
use crate::table::meta::*;
use columns::*;
use read::*;
use std::{
  collections::HashMap,
  fs::create_dir_all,
  io::{Error, ErrorKind},
  path::PathBuf
};

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

#[derive(Debug)]
pub struct Table {
  schema: Schema,
  partition_folder: String,
  columns: Vec<TableColumn>,
  column_symbols: Vec<TableColumnSymbols>,
  row_counts: HashMap<String, usize>,
  column_index: usize,
  data_path: PathBuf,
  meta_path: PathBuf
}

impl Table {
  pub fn create(schema: Schema) -> std::io::Result<Table> {
    let data_path = get_data_path(&schema.name);
    create_dir_all(&data_path).expect(&format!("Cannot create dir {:?}", data_path));
    let meta_path = get_meta_path(&data_path);

    if meta_path.exists() {
      return Err(Error::new(
        ErrorKind::Other,
        format!(
          "Table {name:?} already exists. Try Table::open({name:?}) instead",
          name = schema.name
        )
      ));
    }
    let column_symbols = read_column_symbols(&data_path, &schema);

    let table = Table {
      columns: Vec::new(),
      column_symbols,
      partition_folder: String::new(),
      schema,
      column_index: 0,
      row_counts: HashMap::new(),
      data_path,
      meta_path
    };
    write_table_meta(&table)?;

    Ok(table)
  }

  pub fn open(name: &str) -> std::io::Result<Table> {
    let data_path = get_data_path(name);
    let meta_path = get_meta_path(&data_path);
    let (schema, row_counts) = read_meta(&meta_path, name);
    let column_symbols = read_column_symbols(&data_path, &schema);

    Ok(Table {
      columns: Vec::new(),
      column_symbols,
      partition_folder: String::new(),
      schema,
      column_index: 0,
      row_counts,
      data_path,
      meta_path
    })
  }

  pub fn create_or_open(schema: Schema) -> std::io::Result<Table> {
    let name = schema.name.clone();
    match Self::create(schema) {
      Ok(table) => Ok(table),
      Err(_) => Self::open(&name)
    }
  }
}
