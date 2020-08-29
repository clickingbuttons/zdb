use crate::schema::*;
use crate::table::{Table};
use std::path::PathBuf;
use std::fs::{File,OpenOptions};
use std::io::{BufReader,BufRead,Write};
use std::str::FromStr;

pub fn read_meta(meta_path: &PathBuf, name: &str) -> (Schema, usize) {
  let mut schema = Schema::new(name);
  let mut row_index = 0;
  let f = File::open(meta_path)
    .expect(&format!("Could not open meta file {:?}", meta_path));
  let f = BufReader::new(f);
  let mut section = String::new();
  for line in f.lines() {
    let my_line = line
      .expect(&format!("Could not read line from meta file {:?}", meta_path));
    if my_line.starts_with("[") {
      section = my_line[1..my_line.len() -1].to_string();
    }
    else if !my_line.starts_with("#") && my_line != "" {
      if section == "columns" {
        let mut split = my_line.split(", ");
        let name = String::from(split.next().unwrap());
        schema.columns.push(Column {
          name,
          r#type: ColumnType::from_str(split.next().unwrap()).unwrap()
        });
      }
      else if section == "partition_by" {
        schema.partition_by = PartitionBy::from_str(&my_line).unwrap();
      }
      else if section == "row_index" {
        row_index = my_line.parse::<usize>().expect(
          &format!("Invalid row_index {}", my_line)
        );
      }
    }
  }

  (schema, row_index)
}

pub fn write_meta(table: &Table) -> std::io::Result<()> {
  let meta_path = &table.meta_path;
  let mut f = OpenOptions::new()
    .write(true)
    .create(true)
    .open(&table.meta_path)
    .expect(&format!("Could not open meta file {:?}", meta_path));
  
  let mut meta_text = String::from("[columns]\n");
  meta_text += &table.schema.columns.iter()
    .skip(1)
    .map(|c| format!("{}, {:?}", c.name, c.r#type))
    .collect::<Vec<_>>()
    .join("\n");
  meta_text += "\n\n[partition_by]\n";
  meta_text += &format!("{:?}", &table.schema.partition_by);
  meta_text += "\n\n[row_index]\n";
  meta_text += &format!("{:?}", &table.row_index);

  f.write_all(meta_text.as_bytes())
    .expect(&format!("Could not write to meta file {:?}", meta_path));
  f.flush()
    .expect(&format!("Could not flush to meta file {:?}", meta_path));
  Ok(())
}
