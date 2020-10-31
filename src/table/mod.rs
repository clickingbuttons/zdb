mod meta;
mod read;
pub mod scan;
mod write;
use fnv::FnvHashMap;

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
  pub r#type: ColumnType
}

#[derive(Debug, Clone, Copy)]
pub struct PartitionMeta {
  from_ts:   i64,
  to_ts:     i64,
  min_ts:    i64,
  max_ts:    i64,
  row_count: usize
}

#[derive(Debug)]
pub struct Table {
  schema: Schema,
  // Date-formatted string for partition
  data_folder: String,
  // Working directory for column files
  data_path: PathBuf,
  // This file's existance means the Table exists
  meta_path: PathBuf,
  // Current column for read/writes
  columns: Vec<TableColumn>,
  column_index: usize,
  // Table-wide symbols for columns of type Symbol
  pub column_symbols: Vec<TableColumnSymbols>,
  // Partition metadata
  partition_meta: HashMap<String, PartitionMeta>,
  cur_partition_meta: PartitionMeta
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
        symbol_nums: FnvHashMap::with_capacity_and_hasher(get_capacity(&c), Default::default()),
        symbols:     Vec::<String>::with_capacity(get_capacity(&c)),
        path:        get_symbols_path(&data_path, &c)
      })
      .collect::<Vec<_>>();

    let table = Table {
      columns: Vec::new(),
      column_symbols,
      data_folder: String::new(),
      schema,
      column_index: 0,
      partition_meta: HashMap::new(),
      cur_partition_meta: PartitionMeta {
        from_ts:   0,
        to_ts:     0,
        min_ts:    0,
        max_ts:    0,
        row_count: 0
      },
      data_path,
      meta_path
    };
    table.write_meta()?;

    Ok(table)
  }

  pub fn open<'b>(name: &'b str) -> std::io::Result<Table> {
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
      cur_partition_meta: PartitionMeta {
        from_ts:   0,
        to_ts:     0,
        min_ts:    0,
        max_ts:    0,
        row_count: 0
      },
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
