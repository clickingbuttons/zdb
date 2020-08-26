use std::fmt;

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

pub union Cell {
  pub float32: f32,
  pub float64: f64,
  pub uint32: i32,
  pub uint64: i64,
  pub symbol: *const char
}

#[derive(Debug)]
pub struct Column<'a> {
  pub name: &'a str,
  pub r#type: ColumnType
}

impl Column<'_> {
  pub fn new(name: &str, r#type: ColumnType) -> Column {
    Column {
      name,
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

pub struct Schema<'a> {
  pub name: &'a str,
  pub columns: Vec<Column<'a>>,
  pub partition_by: PartitionBy
}

impl fmt::Debug for Schema<'_> {
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

impl<'a> Schema<'a> {
  pub fn new(name: &'a str) -> Schema<'a> {
    Schema {
      name,
      columns: vec!(Column::new("ts", ColumnType::TIMESTAMP)),
      partition_by: PartitionBy::NONE
    }
  }

  pub fn add_col(mut self, column: Column<'a>) -> Self {
    self.columns.push(column);
    self
  }

  pub fn add_cols(mut self, columns: Vec<Column<'a>>) -> Self {
    self.columns.extend(columns);
    self
  }

  pub fn partition_by(mut self, partition_by: PartitionBy) -> Self {
    self.partition_by = partition_by;
    self
  }
}
