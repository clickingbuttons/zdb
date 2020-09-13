mod util;
mod meta;
mod write;
mod read;

use crate::schema::*;
// "meta" crate is reserved
// https://internals.rust-lang.org/t/is-the-module-name-meta-forbidden/9587/3
use crate::table::meta::*;
use crate::table::util::*;
use std::{
  fs::create_dir_all,
  io::{Error,ErrorKind}, path::PathBuf,
};

#[derive(Debug)]
pub struct Table {
  schema: Schema,
  columns: Vec<TableColumn>,
  row_index: usize,
  column_index: usize,
  meta_path: PathBuf
}

impl Table {
  pub fn create(schema: Schema) -> std::io::Result<Table> {
    let data_path = get_data_path(&schema.name);
    create_dir_all(&data_path)
      .expect(&format!("Cannot create dir {:?}", data_path));
    let meta_path = get_meta_path(&data_path);

    if meta_path.exists() {
      return Err(Error::new(ErrorKind::Other, format!(
        "Table {name:?} already exists. Try Table::open({name:?}) instead", name=schema.name
      )));
    }

    let row_index: usize = 0;

    let table = Table {
      columns: get_columns(&data_path, &schema.columns, row_index),
      schema,
      column_index: 0,
      row_index,
      meta_path
    };
    write_meta(&table)?;

    Ok(table)
  }

  pub fn open(name: &str) -> std::io::Result<Table> {
    let data_path = get_data_path(name);
    let meta_path = get_meta_path(&data_path);
    let (schema, row_index) = read_meta(&meta_path, name);

    Ok(Table {
      columns: get_columns(&data_path, &schema.columns, row_index),
      schema,
      column_index: 0,
      row_index,
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
