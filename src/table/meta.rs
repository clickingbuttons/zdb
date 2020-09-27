use crate::{schema::*, table::Table};
use std::{
  collections::HashMap,
  fs::{File, OpenOptions},
  io::{BufRead, BufReader, Write},
  iter::FromIterator,
  path::PathBuf,
  str::FromStr
};

pub fn get_meta_path(data_path: &PathBuf) -> PathBuf {
  let mut path = data_path.clone();
  path.push("_meta");
  path
}

pub fn read_meta(meta_path: &PathBuf, name: &str) -> (Schema, HashMap<String, usize>) {
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

pub fn write_meta(table: &Table) -> std::io::Result<()> {
  let meta_path = &table.meta_path;
  let mut f = OpenOptions::new()
    .write(true)
    .create(true)
    .open(&table.meta_path)
    .expect(&format!("Could not open meta file {:?}", meta_path));

  let mut meta_text = String::from("[columns]\n");
  meta_text += &table
    .schema
    .columns
    .iter()
    .skip(1)
    .map(|c| format!("{}, {:?}", c.name, c.r#type))
    .collect::<Vec<_>>()
    .join("\n");
  meta_text += "\n\n[partition_by]\n";
  meta_text += &table.schema.partition_by;
  meta_text += "\n\n[row_counts]\n";
  let mut partitions = Vec::from_iter(table.row_counts.keys().cloned());
  partitions.sort();
  for partition in partitions {
    meta_text += &format!(
      "{}/{}\n",
      &partition,
      &table.row_counts.get(&partition).unwrap()
    );
  }

  f.write_all(meta_text.as_bytes())
    .expect(&format!("Could not write to meta file {:?}", meta_path));
  f.flush()
    .expect(&format!("Could not flush to meta file {:?}", meta_path));
  Ok(())
}
