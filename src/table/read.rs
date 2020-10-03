use crate::{
  schema::{Column, ColumnType, Schema},
  table::{PartitionMeta, Table, TableColumn, TableColumnSymbols}
};
use memmap::MmapMut;
use std::{
  cmp::max,
  collections::HashMap,
  convert::TryInto,
  fs::{File, OpenOptions},
  io::{BufRead, BufReader, ErrorKind},
  iter::FromIterator,
  mem::size_of,
  path::PathBuf,
  str::FromStr
};
use time::{date, NumericalDuration, PrimitiveDateTime};

macro_rules! read_bytes {
  ($_type:ty, $bytes:expr, $i:expr) => {{
    let size = size_of::<$_type>();
    <$_type>::from_le_bytes($bytes[$i * size..$i * size + size].try_into().unwrap())
  }};
}

fn format_currency(dollars: f32, sig_figs: usize) -> String {
  let mut res = String::with_capacity(sig_figs + 4);

  if dollars as i32 >= i32::pow(10, sig_figs as u32) {
    res += &format!("{:.width$e}", dollars, width = sig_figs - 4);
  } else {
    let mut num_digits = 0;
    let mut tmp_dollars = dollars;
    while tmp_dollars > 1. {
      tmp_dollars /= 10.;
      num_digits += 1;
    }
    res += &format!(
      "{:<width1$.width2$}",
      dollars,
      width1 = num_digits,
      width2 = max(sig_figs - num_digits, 1)
    );
  }

  String::from(res.trim_end_matches('0').trim_end_matches('.'))
}

impl Table {
  pub fn read_columns(&self, partition_path: &PathBuf, row_count: usize) -> Vec<TableColumn> {
    self
      .schema
      .columns
      .iter()
      .map(|column| {
        let path = get_col_path(&partition_path, &column);
        let (file, data) = get_column_data(&path, row_count, column.r#type);
        TableColumn {
          name: column.name.clone(),
          file,
          data,
          path,
          r#type: column.r#type.clone()
        }
      })
      .collect::<Vec<_>>()
  }

  pub fn scan(&mut self, _from_ts: i64, _to_ts: i64) {
    let mut partitions = Vec::from_iter(self.partition_meta.keys().cloned());
    partitions.sort();
    for partition in partitions {
      self.data_folder = partition;
      let mut partition_path = PathBuf::from(&self.data_path);
      partition_path.push(&self.data_folder);
      let columns = self.read_columns(&partition_path, 0);
      let row_count = self.get_row_count();
      for i in 0..row_count {
        for (j, c) in columns.iter().enumerate() {
          match c.r#type {
            ColumnType::TIMESTAMP => {
              let nanoseconds = read_bytes!(i64, c.data, i);
              let time: PrimitiveDateTime =
                date!(1970 - 01 - 01).midnight() + nanoseconds.nanoseconds();

              print!("{}", time.format("%Y-%m-%d %H:%M:%S.%N"));
            }
            ColumnType::CURRENCY => {
              print!("{:>9}", format_currency(read_bytes!(f32, c.data, i), 7));
            }
            ColumnType::SYMBOL8 => {
              let symbol_index = read_bytes!(u8, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::SYMBOL16 => {
              let symbol_index = read_bytes!(u16, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::SYMBOL32 => {
              let symbol_index = read_bytes!(u32, c.data, i);
              let symbols = &self.column_symbols[j].symbols;

              print!("{:7}", symbols[symbol_index as usize - 1]);
            }
            ColumnType::I32 => {
              print!("{}", read_bytes!(i32, c.data, i));
            }
            ColumnType::U32 => {
              print!("{}", read_bytes!(u32, c.data, i));
            }
            ColumnType::F32 => {
              print!("{:.2}", read_bytes!(f32, c.data, i));
            }
            ColumnType::I64 => {
              print!("{}", read_bytes!(i64, c.data, i));
            }
            ColumnType::U64 => {
              print!("{:>10}", read_bytes!(u64, c.data, i));
            }
            ColumnType::F64 => {
              print!("{}", read_bytes!(f64, c.data, i));
            }
          }
          print!(" ")
        }
        println!("")
      }
    }
  }
}

pub fn read_meta(meta_path: &PathBuf, name: &str) -> (Schema, HashMap<String, PartitionMeta>) {
  let mut schema = Schema::new(name);
  let mut partition_meta = HashMap::new();
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
        let mut split = my_line.split("/");
        let name = String::from(split.next().unwrap());
        schema.columns.push(Column {
          name,
          r#type: ColumnType::from_str(split.next().unwrap()).unwrap()
        });
      } else if section == "partition_by" {
        schema.partition_by = String::from(my_line);
      } else if section.starts_with("partitions.") {
        let partition = section[11..section.len()].to_string();

        let mut split = my_line.split("/");
        let from_ts_str = split.next().unwrap();
        let from_ts = from_ts_str
          .parse::<i64>()
          .expect(&format!("Invalid from_ts {}", from_ts_str));
        let to_ts_str = split.next().unwrap();
        let to_ts = to_ts_str
          .parse::<i64>()
          .expect(&format!("Invalid to_ts {}", to_ts_str));
        let row_count_str = split.next().unwrap();
        let row_count = row_count_str
          .parse::<usize>()
          .expect(&format!("Invalid row_count {}", row_count_str));

        partition_meta.insert(partition, PartitionMeta {
          from_ts,
          to_ts,
          row_count
        });
      }
    }
  }

  (schema, partition_meta)
}

pub fn get_symbols_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension("symbols");
  path
}

pub fn get_capacity(column: &Column) -> usize {
  match column.r#type {
    ColumnType::SYMBOL8 => 2 << 7,
    ColumnType::SYMBOL16 => 2 << 15,
    ColumnType::SYMBOL32 => 2 << 31,
    _ => 0
  }
}

fn get_column_symbols(symbols_path: &PathBuf, column: &Column) -> Vec<String> {
  let capacity = get_capacity(&column);
  if capacity == 0 {
    return Vec::new();
  }
  let mut symbols = Vec::<String>::with_capacity(capacity);
  let file = OpenOptions::new().read(true).open(&symbols_path);
  match file {
    Ok(file) => {
      let f = BufReader::new(&file);
      for line in f.lines() {
        let my_line = line.expect(&format!(
          "Could not read line from symbol file {:?}",
          symbols_path
        ));
        symbols.push(my_line);
      }
    }
    Err(error) => {
      if error.kind() != ErrorKind::NotFound {
        panic!(
          "Problem opening symbol file {:?}: {:?}",
          symbols_path, error
        )
      }
    }
  };

  symbols
}

pub fn read_column_symbols(data_path: &PathBuf, schema: &Schema) -> Vec<TableColumnSymbols> {
  let mut res = Vec::new();

  for column in &schema.columns {
    let path = get_symbols_path(&data_path, &column);
    let col_syms = TableColumnSymbols {
      symbols: get_column_symbols(&path, &column),
      path
    };
    res.push(col_syms);
  }

  res
}

fn get_col_path(data_path: &PathBuf, column: &Column) -> PathBuf {
  let mut path = data_path.clone();
  path.push(&column.name);
  path.set_extension(String::from(format!("{:?}", column.r#type).to_lowercase()));
  path
}

fn get_column_data(path: &PathBuf, row_count: usize, column_type: ColumnType) -> (File, MmapMut) {
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(&path)
    .expect(&format!("Unable to open file {:?}", path));
  // Allocate extra 1GB per column (expect some writes)
  let init_size = row_count * Table::get_row_size(column_type) + 1024 * 1024 * 1024;
  file
    .set_len(init_size as u64)
    .expect(&format!("Could not truncate {:?} to {}", path, init_size));
  unsafe {
    let data = memmap::MmapOptions::new()
      .map_mut(&file)
      .expect(&format!("Could not mmapp {:?}", path));

    (file, data)
  }
}
