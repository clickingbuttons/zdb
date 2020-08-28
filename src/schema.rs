use std::fmt;
use std::str::FromStr;

#[derive(Debug)]
pub enum ColumnType {
  TIMESTAMP,
  CURRENCY,
  SYMBOL,
  INT32,
  UINT32, // Good for up to 4.29B volume
  INT64,
  UINT64,
  FLOAT32,
  FLOAT64
}

impl FromStr for ColumnType {
  type Err = ();

  fn from_str(input: &str) -> Result<ColumnType, Self::Err> {
      match input {
          "TIMESTAMP" => Ok(ColumnType::TIMESTAMP),
          "CURRENCY" => Ok(ColumnType::CURRENCY),
          "SYMBOL" => Ok(ColumnType::SYMBOL),
          "INT32" => Ok(ColumnType::INT32),
          "UINT32" => Ok(ColumnType::UINT32),
          "INT64" => Ok(ColumnType::INT64),
          "UINT64" => Ok(ColumnType::UINT64),
          "FLOAT32" => Ok(ColumnType::FLOAT32),
          "FLOAT64" => Ok(ColumnType::FLOAT64),
          _      => Err(()),
      }
  }
}

pub union Cell {
  pub float32: f32,
  pub float64: f64,
  pub uint32: i32,
  pub uint64: i64,
  pub symbol: *const char
}

#[derive(Debug)]
pub struct Column {
  pub name: String,
  pub r#type: ColumnType
}

impl Column {
  pub fn new(name: &str, r#type: ColumnType) -> Column {
    Column {
      name: name.to_owned(),
      r#type
    }
  }
}

#[derive(Debug)]
pub enum PartitionBy {
  NONE,
  DAY,
  WEEK,
  MONTH,
  YEAR
}

impl FromStr for PartitionBy {
  type Err = ();

  fn from_str(input: &str) -> Result<PartitionBy, Self::Err> {
    match input {
      "NONE" => Ok(PartitionBy::NONE),
      "DAY" => Ok(PartitionBy::DAY),
      "WEEK" => Ok(PartitionBy::WEEK),
      "MONTH" => Ok(PartitionBy::MONTH),
      "YEAR" => Ok(PartitionBy::YEAR),
      _      => Err(()),
    }
  }
}

pub struct Schema {
  pub name: String,
  pub columns: Vec<Column>,
  pub partition_by: PartitionBy
}

impl fmt::Debug for Schema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Schema {} {}:\n  {}",
      self.name,
      format!("{:?}", self.partition_by),
      self.columns.iter()
        .map(|c| format!("({}, {:?})", c.name, c.r#type))
        .collect::<Vec<_>>()
        .join("\n  ")
    )
  }
}

impl<'a> Schema {
  pub fn new(name: &'a str) -> Schema {
    Schema {
      name: name.to_owned(),
      columns: vec!(Column::new("ts", ColumnType::TIMESTAMP)),
      partition_by: PartitionBy::NONE
    }
  }

  pub fn add_col(mut self, column: Column) -> Self {
    self.columns.push(column);
    self
  }

  pub fn add_cols(mut self, columns: Vec<Column>) -> Self {
    self.columns.extend(columns);
    self
  }

  pub fn partition_by(mut self, partition_by: PartitionBy) -> Self {
    self.partition_by = partition_by;
    self
  }
}
