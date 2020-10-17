use std::{fmt, str::FromStr};

#[derive(Debug, Copy, Clone)]
pub enum ColumnType {
  TIMESTAMP,
  CURRENCY,
  SYMBOL8,  // 256 symbols
  SYMBOL16, // 65536 symbols
  SYMBOL32, // 4294967296 symbols
  I32,
  U32, // Good for up to 4.29B volume
  F32,
  I64,
  U64,
  F64
}

impl FromStr for ColumnType {
  type Err = ();

  fn from_str(input: &str) -> Result<ColumnType, Self::Err> {
    match input {
      "TIMESTAMP" => Ok(ColumnType::TIMESTAMP),
      "CURRENCY" => Ok(ColumnType::CURRENCY),
      "SYMBOL8" => Ok(ColumnType::SYMBOL8),
      "SYMBOL16" => Ok(ColumnType::SYMBOL16),
      "SYMBOL32" => Ok(ColumnType::SYMBOL32),
      "I32" => Ok(ColumnType::I32),
      "U32" => Ok(ColumnType::U32),
      "I64" => Ok(ColumnType::I64),
      "U64" => Ok(ColumnType::U64),
      "F32" => Ok(ColumnType::F32),
      "F64" => Ok(ColumnType::F64),
      _ => Err(())
    }
  }
}

#[derive(Debug, Clone)]
pub struct Column {
  pub name:   String,
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

pub struct Schema {
  pub name:         String,
  pub columns:      Vec<Column>,
  pub partition_by: String
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
  pub fn new(name: &'a str) -> Schema {
    Schema {
      name:         name.to_owned(),
      columns:      vec![Column::new("ts", ColumnType::TIMESTAMP)],
      partition_by: String::new()
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

  pub fn partition_by(mut self, partition_by: &'a str) -> Self {
    self.partition_by = partition_by.to_owned();
    self
  }
}
