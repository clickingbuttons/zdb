use crate::schema::*;
use std::fs::File;
use std::io::Write;
use std::io::{BufReader,BufRead};
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::str::FromStr;

#[derive(Debug)]
pub struct Table {
  schema: Schema,
  path: PathBuf
}

fn get_data_path(name: &str) -> std::io::Result<PathBuf> {
  let mut path = PathBuf::from("data");
  path.push(name);
  std::fs::create_dir_all(&path)?;
  Ok(path)
}

fn get_meta_path(name: &str) -> std::io::Result<PathBuf> {
  let mut path = PathBuf::from(get_data_path(name)?);
  path.push("_meta");
  Ok(path)
}

impl Table {
  pub fn open(name: &str) -> std::io::Result<Table> {
    let mut schema = Schema::new(name);
    let f = File::open(get_meta_path(name)?)?;
    let f = BufReader::new(f);

    let mut section = String::new();
    for line in f.lines() {
      let my_line = line?;
      if my_line.starts_with("[") {
        section = my_line[1..my_line.len() -1].to_string();
      }
      else {
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
        println!("{}", my_line);
      }
    }

    Ok(Table {
      schema,
      path: get_data_path(name)?
    })
  }

  pub fn create<'a>(schema: Schema) -> std::io::Result<Table> {
    let columns = schema.columns.iter()
      .map(|c| format!("{}, {:?}", c.name, c.r#type))
      .collect::<Vec<_>>()
      .join("\n");
      
    let path = get_meta_path(&schema.name)?;
    let mut f = OpenOptions::new()
      .write(true)
      .create(true)
      .open(path)?;
    
    f.write_all("[columns]\n".as_bytes())?;
    f.write_all(columns.as_bytes())?;
    f.write_all("\n[partition_by]\n".as_bytes())?;
    f.write_all(format!("{:?}", schema.partition_by).as_bytes())?;
    f.flush()?;

    Ok(Table {
      path: get_data_path(&schema.name)?,
      schema
    })
  }
}
