use crate::table::Table;
use std::{
  fs::{File, OpenOptions},
  io::{BufReader, Write},
  path::PathBuf
};

pub fn read_meta(meta_path: &PathBuf) -> std::io::Result<Table> {
  let f =
    File::open(meta_path).unwrap_or_else(|_| panic!("Could not open meta file {:?}", meta_path));
  let reader = BufReader::new(f);

  let res = serde_json::from_reader(reader)?;
  Ok(res)
}

impl Table {
  pub fn save_cur_partition_meta(&mut self) {
    if self.cur_partition_meta.row_count > 0 {
      self
        .partition_meta
        .insert(self.cur_partition.clone(), self.cur_partition_meta.clone());
    }
  }

  pub fn write_meta(&self) -> std::io::Result<()> {
    let mut f = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&self.meta_path)
      .unwrap_or_else(|_| panic!("Could not open meta file {:?}", &self.meta_path));

    serde_json::to_writer_pretty(&f, &self)
      .unwrap_or_else(|_| panic!("Could not write to meta file {:?}", &self.meta_path));
    f.flush()
      .unwrap_or_else(|_| panic!("Could not flush to meta file {:?}", &self.meta_path));
    Ok(())
  }

  pub fn get_first_ts(&self) -> Option<i64> {
    let mut min_ts = if self.cur_partition_meta.row_count == 0 {
      None
    } else {
      Some(self.cur_partition_meta.from_ts)
    };
    for partition_meta in self.partition_meta.values() {
      if min_ts.is_none() || partition_meta.to_ts < min_ts.unwrap() {
        min_ts = Some(partition_meta.from_ts);
      }
    }

    min_ts
  }

  pub fn get_last_ts(&self) -> Option<i64> {
    let mut max_ts = if self.cur_partition_meta.row_count == 0 {
      None
    } else {
      Some(self.cur_partition_meta.to_ts)
    };
    for partition_meta in self.partition_meta.values() {
      if max_ts.is_none() || partition_meta.to_ts > max_ts.unwrap() {
        max_ts = Some(partition_meta.to_ts);
      }
    }

    max_ts
  }
}
