mod util;
mod meta;
mod write;

use crate::schema::*;
// "meta" crate is reserved
// https://internals.rust-lang.org/t/is-the-module-name-meta-forbidden/9587/3
use crate::table::meta::*;
use crate::table::util::*;
use std::{
    fs::create_dir_all,
    io::{Error,ErrorKind},
    path::PathBuf
};

#[derive(Debug)]
pub struct Table {
  schema: Schema,
  data_path: PathBuf,
  columns_files: Vec<memmap::MmapMut>
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
    write_meta(meta_path, &schema)?;

    Ok(Table {
      columns_files: get_column_files(&data_path, &schema.columns, true),
      data_path,
      schema,
    })
  }

  pub fn open(name: &str) -> std::io::Result<Table> {
    let data_path = get_data_path(name);
    let meta_path = get_meta_path(&data_path);
    let schema = read_meta(meta_path, name);

    Ok(Table {
      columns_files: get_column_files(&data_path, &schema.columns, false),
      data_path,
      schema,
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
