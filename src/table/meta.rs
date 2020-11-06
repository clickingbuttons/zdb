use crate::{
  schema::{Column, ColumnType, PartitionBy, Schema},
  table::{PartitionMeta, Table}
};
use std::{
  collections::HashMap,
  fs::{File, OpenOptions},
  io::{BufRead, BufReader, Write},
  iter::FromIterator,
  path::PathBuf,
  str::FromStr
};

pub fn read_meta(meta_path: &PathBuf, name: &str) -> (Schema, HashMap<String, PartitionMeta>) {
  let mut schema = Schema::new(name);
  let mut partition_meta = HashMap::new();
  let f = File::open(meta_path).unwrap_or_else(|_| panic!("Could not open meta file {:?}", meta_path));
  let f = BufReader::new(f);
  let mut section = String::new();
  for line in f.lines() {
    let my_line = line.unwrap_or_else(|_| panic!(
      "Could not read line from meta file {:?}",
      meta_path
    ));
    if my_line.starts_with("[") {
      section = my_line[1..my_line.len() - 1].to_string();
    } else if !my_line.starts_with("#") && my_line != "" {
      if section == "columns" {
        let mut split = my_line.split("/");
        let name = String::from(split.next().unwrap());
        schema.columns.push(Column {
          name,
          r#type: ColumnType::from_str(split.next().unwrap()).unwrap()
        });
      } else if section == "partition_by" {
        schema.partition_by = PartitionBy::from_str(&my_line).unwrap();
      } else if section.starts_with("partitions.") {
        let partition = section[11..section.len()].to_string();

        let mut split = my_line.split("/");
        let from_ts_str = split.next().unwrap();
        let from_ts = from_ts_str
          .parse::<i64>()
          .unwrap_or_else(|_| panic!("Invalid from_ts {}", from_ts_str));
        let to_ts_str = split.next().unwrap();
        let to_ts = to_ts_str
          .parse::<i64>()
          .unwrap_or_else(|_| panic!("Invalid to_ts {}", to_ts_str));
        let min_ts_str = split.next().unwrap();
        let min_ts = min_ts_str
          .parse::<i64>()
          .unwrap_or_else(|_| panic!("Invalid min_ts {}", min_ts_str));
        let max_ts_str = split.next().unwrap();
        let max_ts = max_ts_str
          .parse::<i64>()
          .unwrap_or_else(|_| panic!("Invalid max_ts {}", max_ts_str));
        let row_count_str = split.next().unwrap();
        let row_count = row_count_str
          .parse::<usize>()
          .unwrap_or_else(|_| panic!("Invalid row_count {}", row_count_str));

        partition_meta.insert(partition, PartitionMeta {
          from_ts,
          to_ts,
          min_ts,
          max_ts,
          row_count
        });
      }
    }
  }

  (schema, partition_meta)
}

impl Table {
  pub fn save_cur_partition_meta(&mut self) {
    if self.cur_partition_meta.row_count > 0 {
      self
        .partition_meta
        .insert(self.data_folder.clone(), self.cur_partition_meta);
    }
  }

  pub fn write_meta(&self) -> std::io::Result<()> {
    let mut f = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&self.meta_path)
      .unwrap_or_else(|_| panic!("Could not open meta file {:?}", &self.meta_path));

    let mut meta_text = String::from("[columns]\n");
    meta_text += &self
      .schema
      .columns
      .iter()
      .skip(1)
      .map(|c| format!("{}/{}", c.name, c.r#type))
      .collect::<Vec<_>>()
      .join("\n");
    meta_text += "\n\n[partition_by]\n";
    meta_text += &format!("{}", self.schema.partition_by);
    meta_text += "\n\n";
    let mut partitions = Vec::from_iter(self.partition_meta.keys().cloned());
    partitions.sort();
    for partition in partitions {
      let partition_meta = self.partition_meta.get(&partition).unwrap();
      meta_text += &format!(
        "[partitions.{}]\n{}/{}/{}/{}/{}\n",
        &partition,
        partition_meta.from_ts,
        partition_meta.to_ts,
        partition_meta.min_ts,
        partition_meta.max_ts,
        partition_meta.row_count,
      );
    }

    f.write_all(meta_text.as_bytes()).unwrap_or_else(|_| panic!(
      "Could not write to meta file {:?}",
      &self.meta_path
    ));
    f.flush().unwrap_or_else(|_| panic!(
      "Could not flush to meta file {:?}",
      &self.meta_path
    ));
    Ok(())
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
