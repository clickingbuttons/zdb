use crate::schema::*;

#[derive(Debug)]
pub struct Table<'a> {
  schema: Schema<'a>
}

impl Table<'_> {
  pub fn open(name: &str) -> Table {
    Table {
      schema: Schema::new("empty")
    }
  }

  pub fn create(schema: Schema) -> Table {
    Table {
      schema
    }
  }
}
