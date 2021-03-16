use crate::{
  schema::{Column, ColumnType},
  table::{PartitionMeta, Table, TableColumn}
};
use chrono::offset;
use jlrs::{prelude::*, value::simple_vector::SimpleVector};
use std::{
  cmp::max, env::temp_dir, fmt::Debug, fs, process, slice::from_raw_parts_mut, thread, time::Instant
};

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

#[derive(Debug)]
struct TableColumnMeta<'a> {
  column:  Column,
  symbols: &'a Vec<String>
}

#[derive(Debug)]
pub enum JuliaQueryError {
  JuliaError(JlrsError),
  JuliaHeapError(Box<JlrsError>),
  ArgMismatch(String)
}

macro_rules! partition_to_value {
  ($frame: expr, $partition: expr, $_type: ty) => {{
    let len = $partition.slice.len() / $partition.column.size;
    let slice = from_raw_parts_mut($partition.slice.as_mut_ptr().cast::<$_type>(), len);
    Value::borrow_array($frame, slice, len).unwrap()
  }}
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

  pub fn partition_iter(&self, from_ts: i64, to_ts: i64, columns: Vec<&str>) -> PartitionIterator {
    let mut partitions = self
      .partition_meta
      .iter()
      .map(|(_partition_dir, partition_meta)| partition_meta)
      .filter(|partition_meta| {
        // Start
        (from_ts >= partition_meta.from_ts && from_ts <= partition_meta.to_ts) || 
        // Middle
        (from_ts < partition_meta.from_ts && to_ts > partition_meta.to_ts) || 
        // End
        (to_ts >= partition_meta.from_ts && to_ts <= partition_meta.to_ts) 
      })
      .collect::<Vec<&PartitionMeta>>();
    partitions.sort_by_key(|partition_meta| partition_meta.from_ts);
    let ts_column = self.schema.columns[0].clone();

    PartitionIterator {
      from_ts,
      to_ts,
      ts_column,
      columns: self.get_union(&columns),
      partitions,
      partition_index: 0
    }
  }

  fn eval_julia_prog(
    &self,
    columns: &Vec<TableColumnMeta>,
    julia: &mut Julia,
    julia_prog: &str
  ) -> Option<JuliaQueryError> {
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
    let err_string = julia
      .dynamic_frame(|global, frame| {
        let expected_args = SimpleVector::with_capacity(frame, columns.len())?;
        for (i, c) in columns.iter().enumerate() {
          let column_type = match c.column.r#type {
            ColumnType::I8 => "Array{Int8,1}",
            ColumnType::I16 => "Array{Int16,1}",
            ColumnType::I32 => "Array{Int32,1}",
            ColumnType::I64=> "Array{Int64,1}",
            ColumnType::U8 | ColumnType::Symbol8 => "Array{UInt8,1}",
            ColumnType::U16 | ColumnType::Symbol16 => "Array{UInt16,1}",
            ColumnType::U32 | ColumnType::Symbol32 => "Array{UInt32,1}",
            ColumnType::U64 => "Array{UInt64,1}",
            ColumnType::F32 | ColumnType::Currency => "Array{Float32,1}",
            ColumnType::F64 => "Array{Float64,1}",
            ColumnType::Timestamp => match c.column.size {
              8 => "Array{UInt64}",
              4 => "Array{UInt32}",
              2 => "Array{UInt16}",
              1 => "Array{UInt8}",
              _ => panic!("Invalid timestamp column size")
            }
          };

          unsafe {
            expected_args
              .set(i, Value::new(frame, column_type)?)
              .unwrap();
          }
        }
        let scan_fn = Module::main(global).function("scan")?;

        Module::main(global)
          .submodule("ScanValidate")?
          .function("validate_args")?
          .call2(frame, scan_fn, expected_args.as_value())?
          .expect("ScanZDB goofed")
          .cast::<String>()
      })
      .unwrap();
    match err_string.is_empty() {
      true => None,
      false => Some(JuliaQueryError::ArgMismatch(err_string))
    }
  }

  // Client can call deserialize(IOBuffer(UInt8[bytes]))
  pub unsafe fn scan_julia(
    &self,
    from_ts: i64,
    to_ts: i64,
    columns: Vec<&str>,
    julia: &mut Julia,
    julia_prog: &str
  ) -> Result<Vec<u8>, JuliaQueryError> {
    let now = Instant::now();
    let partition_iter = self.partition_iter(from_ts, to_ts, columns);
    let partition_iter_len = partition_iter.partitions.len();
    if let Some(prog_errors) = self.eval_julia_prog(&partition_iter.columns, julia, julia_prog)
    {
      return Err(prog_errors);
    }
    println!("{}ms to eval", now.elapsed().as_millis());
    let now = Instant::now();
    for (i, mut partitions) in partition_iter.enumerate() {
      if let Err(scan_error) = julia.dynamic_frame(|global, frame| {
        let mut args = partitions.iter_mut()
          .map(|partition| {
            match partition.column.r#type {
              ColumnType::U8  | ColumnType::Symbol8 =>  partition_to_value!(frame, partition, u8),
              ColumnType::U16 | ColumnType::Symbol16 => partition_to_value!(frame, partition, u16),
              ColumnType::U32 | ColumnType::Symbol32 => partition_to_value!(frame, partition, u32),
              ColumnType::U64 => partition_to_value!(frame, partition, u64),
              ColumnType::I8 =>  partition_to_value!(frame, partition, i8),
              ColumnType::I16 => partition_to_value!(frame, partition, i16),
              ColumnType::I32 => partition_to_value!(frame, partition, i32),
              ColumnType::I64 => partition_to_value!(frame, partition, i64),
              ColumnType::F32 | ColumnType::Currency => partition_to_value!(frame, partition, f32),
              ColumnType::F64 => partition_to_value!(frame, partition, f64),
              ColumnType::Timestamp => match partition.column.size {
                8 => partition_to_value!(frame, partition, i64),
                4 => partition_to_value!(frame, partition, i32),
                2 => partition_to_value!(frame, partition, i16),
                1 => partition_to_value!(frame, partition, i8),
                _ => panic!("Invalid timestamp column size")
              }
            }
          })
          .collect::<Vec<_>>();
        let scan_result = Module::main(global)
          .function("scan")
          .expect("Function `scan` doesn't exist")
          .call(frame, &mut args)?
          .expect("Function `scan` threw an exception")
          .clone();
        if i == partition_iter_len - 1 {
          Module::main(global).set_global("scan_result", scan_result);
        }
        Ok(())
      }) {
        return Err(JuliaQueryError::JuliaHeapError(scan_error));
      }
    }
    println!("{}ms to scan", now.elapsed().as_millis());
    let now = Instant::now();
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
    println!("{}ms to serialize", now.elapsed().as_millis());
    Ok(res)
  }
}

#[derive(Debug)]
pub struct PartitionColumn<'a> {
  pub column:    TableColumn,
  pub slice:     &'a mut [u8],
  pub symbols:   &'a Vec<String>,
  pub meta:      &'a PartitionMeta,
  pub row_count: usize
}

macro_rules! get_partition_slice {
  ($slice: expr, $_type: ty) => {
    unsafe {
      from_raw_parts_mut(
        $slice.as_ptr() as *mut $_type,
        $slice.len() / std::mem::size_of::<$_type>()
      )
    }
  }
}

impl<'a> PartitionColumn<'_> {
  pub fn get_currency(&self) -> &[f32] { self.get_f32() }

  pub fn get_i8(&self) -> &mut [i8] { get_partition_slice!(self.slice, i8) }
  pub fn get_u8(&self) -> &mut [u8] { get_partition_slice!(self.slice, u8) }
  pub fn get_i16(&self) -> &mut [i16] { get_partition_slice!(self.slice, i16) }
  pub fn get_u16(&self) -> &mut [u16] { get_partition_slice!(self.slice, u16) }
  pub fn get_i32(&self) -> &mut [i32] { get_partition_slice!(self.slice, i32) }
  pub fn get_u32(&self) -> &mut [u32] { get_partition_slice!(self.slice, u32) }
  pub fn get_i64(&self) -> &mut [i64] { get_partition_slice!(self.slice, i64) }
  pub fn get_u64(&self) -> &mut [u64] { get_partition_slice!(self.slice, u64) }
  pub fn get_f32(&self) -> &mut [f32] { get_partition_slice!(self.slice, f32) }
  pub fn get_f64(&self) -> &mut [f64] { get_partition_slice!(self.slice, f64) }

  pub fn get_symbol(&self, row_index: usize) -> &String {
    match self.column.r#type {
      ColumnType::Symbol8 => &self.symbols[self.get_u8()[row_index] as usize],
      ColumnType::Symbol16 => &self.symbols[self.get_u16()[row_index] as usize],
      ColumnType::Symbol32 => &self.symbols[self.get_u32()[row_index] as usize],
      ctype => panic!("ColumnType {:?} is not a Symbol", ctype)
    }
  }

  pub fn get_timestamp(&self, row_index: usize) -> i64 {
    if self.column.r#type != ColumnType::Timestamp {
      panic!("ColumnType {:?} is not a Timestamp", self.column.r#type);
    }

    match self.column.size {
      8 => self.get_i64()[row_index],
      4 => self.get_u32()[row_index] as i64 * self.column.resolution + self.meta.min_ts,
      2 => self.get_u16()[row_index] as i64 * self.column.resolution + self.meta.min_ts,
      csize => panic!("Size {:?} is not a supported Timestamp size", csize)
    }
  }
}

#[derive(Debug)]
pub struct PartitionIterator<'a> {
  from_ts: i64,
  to_ts: i64,
  ts_column: Column,
  columns: Vec<TableColumnMeta<'a>>,
  partitions: Vec<&'a PartitionMeta>,
  partition_index: usize
}

macro_rules! binary_search_seek {
  ($ts_column: expr, $len: expr, $needle: expr, $seek_start: expr, $_type: ty) => {{
    let needle = $needle as $_type;
    let data = from_raw_parts_mut($ts_column.data.as_ptr() as *mut $_type, $len);
    let mut index = data.binary_search(&needle);
    if let Ok(ref mut i) = index {
      // Seek to beginning/end
      if $seek_start {
        while *i > 1 && data[*i - 1] == needle {
          *i -= 1;
        }
      } else {
        while *i < data.len() - 2 && data[*i + 1] == needle {
          *i += 1;
        }
      }
    }
    index
  }};
}

unsafe fn find_ts(ts_column: &TableColumn, from_ts: i64, seek_start: bool) -> usize {
  let needle = from_ts / ts_column.resolution;
  let len = ts_column.data.len() / ts_column.size;
  let search_results = match ts_column.size {
    8 => binary_search_seek!(ts_column, len, needle, seek_start, i64),
    4 => binary_search_seek!(ts_column, len, needle, seek_start, u32),
    2 => binary_search_seek!(ts_column, len, needle, seek_start, u16),
    1 => binary_search_seek!(ts_column, len, needle, seek_start, u8),
    s => panic!(format!("Invalid column size {}", s))
  };
  match search_results {
    Ok(n) => n,
    Err(n) => n
  }
}

impl<'a> Iterator for PartitionIterator<'a> {
  type Item = Vec<PartitionColumn<'a>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.partition_index > self.partitions.len() {
      return None;
    }
    let partition_meta = self.partitions.get(self.partition_index)?;
    let start_row = if self.partition_index == 0 {
      let ts_column = Table::open_column(
        &partition_meta.dir,
        partition_meta.row_count,
        &self.ts_column
      );
      unsafe { find_ts(&ts_column, self.from_ts - partition_meta.min_ts, true) }
    } else {
      0
    };
    let end_row = if self.partition_index == self.partitions.len() - 1 {
      let ts_column = Table::open_column(
        &partition_meta.dir,
        partition_meta.row_count,
        &self.ts_column
      );
      unsafe { find_ts(&ts_column, self.to_ts - partition_meta.min_ts, false) }
    } else {
      partition_meta.row_count
    };
    let data_columns = self
      .columns
      .iter()
      .map(|column| {
        let table_column = Table::open_column(
          &partition_meta.dir,
          partition_meta.row_count,
          &column.column
        );
        let slice = unsafe {
          from_raw_parts_mut(
            table_column.data.as_ptr().add(start_row * table_column.size) as *mut u8,
            (end_row - start_row) * table_column.size
          )
        };

        PartitionColumn {
          slice,
          column: table_column,
          symbols: column.symbols,
          meta: partition_meta,
          row_count: end_row - start_row
        }
      })
      .collect::<Vec<_>>();

    self.partition_index += 1;
    return Some(data_columns);
  }
}
