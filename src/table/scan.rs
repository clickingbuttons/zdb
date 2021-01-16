use crate::{
  calendar::ToNaiveDateTime,
  schema::{Column, ColumnType},
  table::{PartitionMeta, Table, TableColumn}
};
use chrono::{offset, NaiveDateTime};
use jlrs::{prelude::*, value::simple_vector::SimpleVector};
use std::{
  cmp::max, convert::TryInto, env::temp_dir, fmt::Debug, fs, mem::size_of, process, thread
};

// Important that this fits in single register.
#[derive(Copy, Clone)]
pub union RowValue<'a> {
  pub sym: &'a String,
  pub i8:  i8,
  pub u8:  u8,
  pub i16: i16,
  pub u16: u16,
  pub i32: i32,
  pub u32: u32,
  pub f32: f32,
  pub i64: i64,
  pub u64: u64,
  pub f64: f64
}

impl Debug for RowValue<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("{:x}", self.get_i64()))
  }
}

impl AsMut<i64> for RowValue<'_> {
  fn as_mut(&mut self) -> &mut i64 { unsafe { &mut self.i64 } }
}

impl RowValue<'_> {
  pub fn get_timestamp(&self) -> NaiveDateTime {
    let nanoseconds = unsafe { self.i64 };
    nanoseconds.to_naive_date_time()
  }

  pub fn get_currency(&self) -> f32 { unsafe { self.f32 } }

  pub fn get_symbol(&self) -> &String { unsafe { self.sym } }

  pub fn get_i32(&self) -> i32 { unsafe { self.i32 } }

  pub fn get_u32(&self) -> u32 { unsafe { self.u32 } }

  pub fn get_f32(&self) -> f32 { unsafe { self.f32 } }

  pub fn get_i64(&self) -> i64 { unsafe { self.i64 } }

  pub fn get_u64(&self) -> u64 { unsafe { self.u64 } }

  pub fn get_f64(&self) -> f64 { unsafe { self.f64 } }
}

pub trait FormatCurrency {
  fn format_currency(self, sig_figs: usize) -> String;
}

impl FormatCurrency for f32 {
  fn format_currency(self, sig_figs: usize) -> String {
    let mut res = String::with_capacity(sig_figs + 4);

    if self as i32 >= i32::pow(10, sig_figs as u32) {
      res += &format!("{:.width$e}", self, width = sig_figs - 4);
    } else {
      let mut num_digits = 0;
      let mut tmp_dollars = self;
      while tmp_dollars > 1. {
        tmp_dollars /= 10.;
        num_digits += 1;
      }
      res += &format!(
        "{:<width1$.width2$}",
        self,
        width1 = num_digits,
        width2 = max(sig_figs - num_digits, 1)
      );
    }

    String::from(res.trim_end_matches('0').trim_end_matches('.'))
  }
}

macro_rules! read_bytes {
  ($_type:ty, $bytes:expr, $i:expr) => {{
    let size = size_of::<$_type>();
    <$_type>::from_le_bytes($bytes[$i * size..$i * size + size].try_into().unwrap())
  }};
}

#[derive(Debug)]
struct TableColumnMeta<'a> {
  column:  Column,
  symbols: &'a Vec<String>
}

impl Table {
  fn get_union<'a>(&'a self, columns: &Vec<&str>) -> Vec<TableColumnMeta<'a>> {
    columns
      .iter()
      .map(|col_name| {
        let index = self
          .schema
          .columns
          .iter()
          .position(|col| &col.name == col_name)
          .unwrap_or_else(|| panic!("Column {} does not exist", col_name));
        TableColumnMeta {
          column:  self.schema.columns[index].clone(),
          symbols: &self.column_symbols[index].symbols
        }
      })
      .collect::<Vec<_>>()
  }

  fn validate_args(&self, columns: &Vec<&str>, julia: &mut Julia, julia_prog: &str) -> String {
    // Run user program
    // TODO: sandbox julia
    let mut prog_file = temp_dir();
    prog_file.push(format!(
      "zdb_query_{}_process_{}_thread_{:?}_time_{:?}.jl",
      self.schema.name,
      process::id(),
      thread::current().id(),
      offset::Local::now()
    ));
    fs::write(&prog_file, julia_prog).expect("Unable to write user program file");
    julia.include(prog_file).unwrap();
    julia
      .dynamic_frame(|global, frame| {
        let columns = self.get_union(&columns);
        let expected_args = SimpleVector::with_capacity(frame, columns.len()).unwrap();
        for (i, c) in columns.iter().enumerate() {
          let column_type = match c.column.r#type {
            ColumnType::I8 => "Int8",
            ColumnType::I16 => "Int16",
            ColumnType::I32 => "Int32",
            ColumnType::I64 | ColumnType::Timestamp => "Int64",
            ColumnType::U8 => "UInt8",
            ColumnType::U16 => "UInt16",
            ColumnType::U32 => "UInt32",
            ColumnType::U64 => "UInt64",
            ColumnType::Symbol8 | ColumnType::Symbol16 | ColumnType::Symbol32 => "String",
            ColumnType::F32 | ColumnType::Currency => "Float32",
            ColumnType::F64 => "Float64"
          };

          unsafe {
            expected_args
              .set(i, Value::new(frame, column_type).unwrap())
              .unwrap();
          }
        }
        let scan_fn = Module::main(global).function("scan").unwrap();

        Module::main(global)
          .submodule("ScanValidate")?
          .function("validate_args")?
          .call2(frame, scan_fn, expected_args.as_value())
          .expect("Errorz")
          .expect("ScanZDB goofed")
          .cast::<String>()
      })
      .unwrap()
  }

  // juliaFunc = "
  //   acc = Float32(0);
  //   function scan(x::Int16, y::Float32)
  //     global acc += x;
  //     global acc += y;
  //     return acc;
  //   end
  // "
  //
  // 1. Verify args match column types:
  //   Meta.parse(juliaFunc.split(";").last())).args[1].args[1].args[2].args[2] === :Int16
  //   Meta.parse(juliaFunc.split(";").last())).args[1].args[1].args[3].args[2] === :Float32
  //
  // 2. Return serialized val casted to Vec<u8>
  //   using Serialization
  //   io = IOBuffer()
  //   serialize(io, juliaFuncLastCall(row))
  //   io.data
  //
  // Then client can call
  //   deserialize(IOBuffer(take!(io)))
  //   deserialize(IOBuffer(UInt8[bytes]))
  pub unsafe fn scan_julia(
    &self,
    from_ts: i64,
    to_ts: i64,
    columns: Vec<&str>,
    mut julia: Julia,
    julia_prog: &str
  ) -> Option<Vec<u8>> {
    let scan_errors = self.validate_args(&columns, &mut julia, julia_prog);
    if scan_errors.is_empty() {
      let mut rows = self.row_iter(from_ts, to_ts, columns).peekable();
      julia
        .dynamic_frame(|_global, frame| {
          Value::eval_string(frame, julia_prog).unwrap().unwrap();
          Ok(())
        })
        .unwrap();
      while let Some(row) = rows.next() {
        julia
          .dynamic_frame(|global, frame| {
            let arg1 = Value::new(frame, row[0].i64).unwrap();
            let arg2 = Value::new(frame, row[1].f32).unwrap();
            let accumulator = Module::main(global)
            .function("scan")
            .expect("Function `scan` doesn't exist")
            .call2(frame, arg1, arg2)?
            //.call(frame, &mut row)?
            .expect("Ur scan goofed");
            if rows.peek().is_none() {
              Module::main(global).set_global("scan_result", accumulator);
            }
            Ok(())
          })
          .unwrap();
      }
      let res: Vec<u8> = julia
        .dynamic_frame(|global, frame| {
          let io = Value::eval_string(frame, "IOBuffer()").unwrap().unwrap();
          let res = Module::main(global).global("scan_result").unwrap();
          Module::main(global)
            .function("serialize")
            .expect("Function `serialize` doesn't exist")
            .call2(frame, io, res)?
            .expect("Can't serialize scan's value");
          let bytes = io
            .get_field(frame, "data")
            .unwrap()
            .cast::<Array>()
            .unwrap();
          let bytes = bytes.inline_data(frame).unwrap();
          let bytes: Vec<u8> = bytes.as_slice().to_vec();
          Ok(bytes)
        })
        .unwrap();
      return Some(res);
    }
    None
  }

  pub fn row_iter(&self, from_ts: i64, to_ts: i64, columns: Vec<&str>) -> RowIterator {
    let mut partitions = self
      .partition_meta
      .iter()
      .filter(|(_data_dir, partition_meta)| {
        from_ts >= partition_meta.from_ts || to_ts > partition_meta.from_ts
      })
      .map(|(_data_dir, partition_meta)| partition_meta)
      .collect::<Vec<&PartitionMeta>>();
    partitions.sort_by_key(|partition_meta| partition_meta.from_ts);

    let columns = self.get_union(&columns);

    RowIterator {
      from_ts,
      to_ts,
      columns,
      partitions,
      partition_index: 0,
      data_columns: vec![],
      row_index: 0
    }
  }
}

pub struct RowIterator<'a> {
  from_ts: i64,
  to_ts: i64,
  columns: Vec<TableColumnMeta<'a>>,
  partitions: Vec<&'a PartitionMeta>,
  partition_index: usize,
  data_columns: Vec<TableColumn>,
  row_index: usize
}

impl<'a> Iterator for RowIterator<'a> {
  type Item = Vec<RowValue<'a>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.row_index > self.partitions[self.partition_index].row_count {
      self.partition_index += 1;
      if self.partition_index > self.partitions.len() {
        return None;
      }
      self.row_index = 0;
    }
    let partition_meta = self.partitions.get(self.partition_index)?;
    if self.row_index == 0 {
      self.data_columns = self
        .columns
        .iter()
        .map(|column| {
          Table::open_column(
            &partition_meta.dir,
            partition_meta.row_count,
            &column.column
          )
        })
        .collect::<Vec<TableColumn>>();
    }
    let mut row = Vec::<RowValue>::with_capacity(self.data_columns.len());
    for (col_index, table_column) in self.data_columns.iter().enumerate() {
      let data = &table_column.data;
      match table_column.r#type {
        ColumnType::Timestamp => {
          let nanoseconds = match table_column.size {
            8 => read_bytes!(i64, data, self.row_index),
            4 => read_bytes!(u32, data, self.row_index) as i64,
            2 => read_bytes!(u16, data, self.row_index) as i64,
            1 => read_bytes!(u8, data, self.row_index) as i64,
            s => panic!(format!("Invalid column size {}", s))
          } * table_column.resolution;
          if col_index == 0 {
            if nanoseconds > self.to_ts {
              return None;
            } else if nanoseconds < self.from_ts {
              // TODO: binary search + rollback for first ts
              break;
            }
          }
          row.push(RowValue { i64: nanoseconds });
        }
        ColumnType::Currency => {
          let f32 = read_bytes!(f32, data, self.row_index);
          row.push(RowValue { f32 });
        }
        ColumnType::Symbol8 => {
          let symbol_index = read_bytes!(u8, data, self.row_index) as usize;
          let sym = &self.columns[col_index].symbols[symbol_index - 1];
          row.push(RowValue { sym });
        }
        ColumnType::Symbol16 => {
          let symbol_index = read_bytes!(u16, data, self.row_index) as usize;
          let sym = &self.columns[col_index].symbols[symbol_index - 1];
          row.push(RowValue { sym });
        }
        ColumnType::Symbol32 => {
          let symbol_index = read_bytes!(u32, data, self.row_index) as usize;
          let sym = &self.columns[col_index].symbols[symbol_index - 1];
          row.push(RowValue { sym });
        }
        ColumnType::I8 => {
          let i8 = read_bytes!(i8, data, self.row_index);
          row.push(RowValue { i8 });
        }
        ColumnType::U8 => {
          let u8 = read_bytes!(u8, data, self.row_index);
          row.push(RowValue { u8 });
        }
        ColumnType::I16 => {
          let i16 = read_bytes!(i16, data, self.row_index);
          row.push(RowValue { i16 });
        }
        ColumnType::U16 => {
          let u16 = read_bytes!(u16, data, self.row_index);
          row.push(RowValue { u16 });
        }
        ColumnType::I32 => {
          let i32 = read_bytes!(i32, data, self.row_index);
          row.push(RowValue { i32 });
        }
        ColumnType::U32 => {
          let u32 = read_bytes!(u32, data, self.row_index);
          row.push(RowValue { u32 });
        }
        ColumnType::F32 => {
          let f32 = read_bytes!(f32, data, self.row_index);
          row.push(RowValue { f32 });
        }
        ColumnType::I64 => {
          let i64 = read_bytes!(i64, data, self.row_index);
          row.push(RowValue { i64 });
        }
        ColumnType::U64 => {
          let u64 = read_bytes!(u64, data, self.row_index);
          row.push(RowValue { u64 });
        }
        ColumnType::F64 => {
          let f64 = read_bytes!(f64, data, self.row_index);
          row.push(RowValue { f64 });
        }
      }
    }

    self.row_index += 1;
    return Some(row);
  }
}
