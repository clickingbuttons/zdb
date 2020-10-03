mod meta;
mod read;
mod write;

use crate::schema::*;
// "meta" crate is reserved
// https://internals.rust-lang.org/t/is-the-module-name-meta-forbidden/9587/3
use crate::table::meta::*;
use read::*;
use std::{
  collections::HashMap,
  fs::{create_dir_all, File},
  io::{Error, ErrorKind},
  path::PathBuf
};

#[derive(Debug)]
pub struct TableColumnSymbols {
  pub path:    PathBuf,
  pub symbols: Vec<String>
}

#[derive(Debug)]
pub struct TableColumn {
  pub name:   String,
  pub file:   File,
  pub data:   memmap::MmapMut,
  pub path:   PathBuf,
  pub r#type: ColumnType
}

#[derive(Debug)]
pub struct PartitionMeta {
  from_ts:   i64,
  to_ts:     i64,
  row_count: usize
}

#[derive(Debug)]
pub struct Table {
  schema:         Schema,
  // Date-formatted string for partition
  data_folder:    String,
  // Working directory for column files
  data_path:      PathBuf,
  // This file's existance means the Table exists
  meta_path:      PathBuf,
  // Current column for read/writes
  column_index:   usize,
  columns:        Vec<TableColumn>,
  // Table-wide symbols for columns of type Symbol
  column_symbols: Vec<TableColumnSymbols>,
  // Partition metadata
  partition_meta: HashMap<String, PartitionMeta>
}

fn get_data_path(name: &str) -> PathBuf {
  let mut path = PathBuf::from("data");
  path.push(name);
  path
}

fn get_meta_path(data_path: &PathBuf) -> PathBuf {
  let mut path = data_path.clone();
  path.push("_meta");
  path
}

impl Table {
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
      ColumnType::F64 => 8
    }
  }

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
    let column_symbols = schema
      .columns
      .iter()
      .map(|c| TableColumnSymbols {
        symbols: Vec::<String>::with_capacity(get_capacity(&c)),
        path:    get_symbols_path(&data_path, &c)
      })
      .collect::<Vec<_>>();

    let table = Table {
      columns: Vec::new(),
      column_symbols,
      data_folder: String::new(),
      schema,
      column_index: 0,
      partition_meta: HashMap::new(),
      data_path,
      meta_path
    };
    table.write_meta()?;

    Ok(table)
  }

  pub fn open(name: &str) -> std::io::Result<Table> {
    let data_path = get_data_path(name);
    let meta_path = get_meta_path(&data_path);
    let (schema, partition_meta) = read_meta(&meta_path, name);
    let column_symbols = read_column_symbols(&data_path, &schema);

    Ok(Table {
      columns: Vec::new(),
      column_symbols,
      data_folder: String::new(),
      schema,
      column_index: 0,
      partition_meta,
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
