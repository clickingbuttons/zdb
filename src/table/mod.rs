mod read;
mod write;

use crate::schema::*;
// "meta" crate is reserved
// https://internals.rust-lang.org/t/is-the-module-name-meta-forbidden/9587/3
use read::*;
use std::{collections::HashMap, fs::File, fs::create_dir_all, io::{BufRead, BufReader, Error, ErrorKind}, path::PathBuf, str::FromStr};

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
pub struct Table {
  schema:         Schema,
  data_folder:    String,
  columns:        Vec<TableColumn>,
  column_symbols: Vec<TableColumnSymbols>,
  row_counts:     HashMap<String, usize>,
  column_index:   usize,
  data_path:      PathBuf,
  meta_path:      PathBuf
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

fn read_meta(meta_path: &PathBuf, name: &str) -> (Schema, HashMap<String, usize>) {
  let mut schema = Schema::new(name);
  let mut row_counts = HashMap::new();
  let f = File::open(meta_path).expect(&format!("Could not open meta file {:?}", meta_path));
  let f = BufReader::new(f);
  let mut section = String::new();
  for line in f.lines() {
    let my_line = line.expect(&format!(
      "Could not read line from meta file {:?}",
      meta_path
    ));
    if my_line.starts_with("[") {
      section = my_line[1..my_line.len() - 1].to_string();
    } else if !my_line.starts_with("#") && my_line != "" {
      if section == "columns" {
        let mut split = my_line.split(", ");
        let name = String::from(split.next().unwrap());
        schema.columns.push(Column {
          name,
          r#type: ColumnType::from_str(split.next().unwrap()).unwrap()
        });
      } else if section == "partition_by" {
        schema.partition_by = String::from(my_line);
      } else if section == "row_counts" {
        let mut split = my_line.split("/");
        let name = String::from(split.next().unwrap());
        let partition_row_count_str = split.next().unwrap();
        let partition_row_count = partition_row_count_str
          .parse::<usize>()
          .expect(&format!("Invalid row_count {}", partition_row_count_str));
        row_counts.insert(name, partition_row_count);
      }
    }
  }

  (schema, row_counts)
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
      row_counts: HashMap::new(),
      data_path,
      meta_path
    };
    table.write_meta()?;

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
      data_folder: String::new(),
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
