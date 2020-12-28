mod meta;
mod read;
pub mod scan;
mod write;
use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

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
  pub path:        PathBuf,
  // Good for writing
  pub symbol_nums: FnvHashMap<String, usize>,
  // Good for reading
  pub symbols:     Vec<String>
}

#[derive(Debug)]
pub struct TableColumn {
  pub name:   String,
  pub file:   File,
  pub data:   memmap::MmapMut,
  pub path:   PathBuf,
  pub r#type: ColumnType,
  pub size:   usize,
  pub resolution: i64
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionMeta {
  pub dir:       PathBuf,
  pub from_ts:   i64,
  pub to_ts:     i64,
  pub min_ts:    i64,
  pub max_ts:    i64,
  pub row_count: usize
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
  schema: Schema,
  // This file's existance means the Table exists
  #[serde(skip)]
  meta_path: PathBuf,
  // Current column for read/writes
  #[serde(skip)]
  columns: Vec<TableColumn>,
  #[serde(skip)]
  column_index: usize,
  // Table-wide symbols for columns of type Symbol
  #[serde(skip)]
  pub column_symbols: Vec<TableColumnSymbols>,
  // Partition metadata
  pub partition_meta: HashMap<String, PartitionMeta>,
  // Helps better choose which next partition to write to
  dir_index: usize,
  #[serde(skip)]
  cur_partition: String,
  #[serde(skip)]
  pub cur_partition_meta: PartitionMeta
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
  pub fn create(schema: Schema) -> std::io::Result<Table> {
    let data_path = get_data_path(&schema.name);
    create_dir_all(&data_path).unwrap_or_else(|_| panic!("Cannot create dir {:?}", data_path));
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
        symbol_nums: FnvHashMap::with_capacity_and_hasher(get_capacity(&c), Default::default()),
        symbols:     Vec::<String>::with_capacity(get_capacity(&c)),
        path:        get_symbols_path(&data_path, &c)
      })
      .collect::<Vec<_>>();

    let table = Table {
      columns: Vec::new(),
      column_symbols,
      schema,
      dir_index: 0,
      column_index: 0,
      partition_meta: HashMap::new(),
      cur_partition: String::new(),
      cur_partition_meta: PartitionMeta::default(),
      meta_path
    };
    table.write_meta()?;

    Ok(table)
  }

  pub fn open<'b>(name: &'b str) -> std::io::Result<Table> {
    let data_path = get_data_path(&name);
    let meta_path = get_meta_path(&data_path);
    let mut res = read_meta(&meta_path)?;
    res.column_symbols = read_column_symbols(&data_path, &res.schema);
    res.meta_path = meta_path;
    res.schema.name = String::from(name);

    Ok(res)
  }

  pub fn create_or_open(schema: Schema) -> std::io::Result<Table> {
    let name = schema.name.clone();
    match Self::create(schema) {
      Ok(table) => Ok(table),
      Err(_) => Self::open(&name)
    }
  }
}
