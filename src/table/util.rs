use crate::schema::Column;
use std::{
  path::PathBuf,
  fs::OpenOptions
};
use memmap::MmapMut;

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

pub fn get_col_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension(format!("{:?}", column.r#type));
  path
}

pub fn get_column_files(data_path: &PathBuf, columns: &Vec<Column>, init_file: bool) -> Vec<MmapMut> {
  columns.iter()
    .map(|column| get_col_path(data_path, &column))
    .map(|path| {
      let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .expect(&format!("Unable to open file {:?}", path));
      if init_file {
        // Allocate 1MB per-column to start
        file.set_len(1024)
          .expect(&format!("Could not truncate {:?}", path));
      }
      (path, file)
    })
    .map(|(path, file)| unsafe {
      memmap::MmapOptions::new()
        .map_mut(&file)
        .expect(&format!("Could not access data from mmapped {:?}", path))
    })
    .collect::<Vec<_>>()
}
