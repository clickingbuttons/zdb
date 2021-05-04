use serde::{Deserialize, Serialize};
use std::{cmp::PartialEq, fmt, path::PathBuf};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
  Timestamp,
  Currency,
  Symbol8,  // 256 symbols
  Symbol16, // 65536 symbols
  Symbol32, // 4294967296 symbols
  I8,
  U8,
  I16,
  U16,
  I32,
  U32, // Good for up to 4.29B volume
  F32,
  I64,
  U64,
  F64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
  pub name:       String,
  pub r#type:     ColumnType,
  // How many bytes each row of the column takes
  pub size:       usize,
  // If timestamp column what nanoseconds to divide by. Can be used to shrink column size
  pub resolution: i64
}

impl Column {
  pub fn new(name: &str, r#type: ColumnType) -> Column {
    Column {
      name: name.to_owned(),
      r#type,
      resolution: 1,
      size: match r#type {
        ColumnType::Timestamp => 8,
        ColumnType::Currency => 4,
        ColumnType::Symbol8 => 1,
        ColumnType::Symbol16 => 2,
        ColumnType::Symbol32 => 4,
        ColumnType::I8 => 1,
        ColumnType::U8 => 1,
        ColumnType::I16 => 2,
        ColumnType::U16 => 2,
        ColumnType::I32 => 4,
        ColumnType::U32 => 4,
        ColumnType::F32 => 4,
        ColumnType::I64 => 8,
        ColumnType::U64 => 8,
        ColumnType::F64 => 8
      }
    }
  }

  pub fn with_resolution(mut self, resolution_nanos: i64) -> Column {
    self.resolution = resolution_nanos;
    self
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum PartitionBy {
  None,
  Year,
  Month,
  Day
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
  #[serde(skip, default)]
  pub name: String,
  pub columns: Vec<Column>,
  pub partition_by: PartitionBy,
  pub partition_dirs: Vec<PathBuf>
}

impl fmt::Debug for Schema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "Schema {} {}:\n  {}",
      self.name,
      format!("{:?}", self.partition_by),
      self
        .columns
        .iter()
        .map(|c| format!("({}, {:?})", c.name, c.r#type))
        .collect::<Vec<_>>()
        .join("\n  ")
    )
  }
}

impl<'a> Schema {
  pub fn new(name: &'a str) -> Self {
    Self {
      name: name.to_owned(),
      columns: vec![],
      partition_by: PartitionBy::None,
      partition_dirs: vec![PathBuf::from("data")]
    }
  }

  pub fn add_col(mut self, column: Column) -> Self {
    self.columns.push(column);
    self.set_timestamp_size();
    self
  }

  pub fn add_cols(mut self, columns: Vec<Column>) -> Self {
    self.columns.extend(columns);
    self.set_timestamp_size();
    self
  }

  pub fn partition_by(mut self, partition_by: PartitionBy) -> Self {
    self.partition_by = partition_by;
    self.set_timestamp_size();
    self
  }

  pub fn partition_dirs(mut self, partition_dirs: Vec<&str>) -> Self {
    self.partition_dirs = partition_dirs
      .iter()
      .map(|partition_dir| PathBuf::from(partition_dir))
      .collect::<Vec<_>>();
    self
  }

  fn set_timestamp_size(&mut self) {
    // Determine lengths of timestamp columns based on partition_by and their resolution
    let partition_by = self.partition_by;
    self
      .columns
      .iter_mut()
      .filter(|col| col.r#type == ColumnType::Timestamp)
      .for_each(|mut col| {
        let nanoseconds_in_partition = match partition_by {
          PartitionBy::Day => 24 * 60 * 60 * 1_000_000_000,
          PartitionBy::Month => 24 * 60 * 60 * 1_000_000_000 * 31,
          PartitionBy::Year => 24 * 60 * 60 * 1_000_000_000 * 365,
          PartitionBy::None => i64::MAX
        };
        col.size = match nanoseconds_in_partition / col.resolution {
          0..=256 => 1,
          257..=65536 => 2,
          65537..=4294967296 => 4,
          _ => {
            // Don't bother rounding to be compatible with writing i64s
            col.resolution = 1;
            8
          }
        };
      });
  }
}
