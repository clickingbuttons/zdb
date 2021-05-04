use chrono::{DateTime, NaiveDate};
use nix::{
  sys::{signal, wait::waitpid},
  unistd::{fork, ForkResult, Pid}
};
use serde::{de, Deserialize};
use std::{
  ffi::{c_void, CStr, CString},
  io::prelude::*,
  net::{TcpListener, TcpStream},
  process::exit,
  slice::from_raw_parts,
  time::Instant
};
use zdb::{
  c_str,
  schema::{Column, ColumnType},
  server::julia::*,
  table::Table
};

extern "C" fn handle_sigint(_: i32) {
  unsafe { jl_atexit_hook(0) };
  exit(0);
}

fn main() {
  let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

  for i in 0..1 {
    match unsafe { fork() } {
      Ok(ForkResult::Child) => {
        println!("fork {}", i);
        let sig_action = signal::SigAction::new(
          signal::SigHandler::Handler(handle_sigint),
          signal::SaFlags::SA_NODEFER,
          signal::SigSet::empty()
        );
        unsafe {
          jl_init__threading();
          signal::sigaction(signal::SIGINT, &sig_action).unwrap();
          jl_eval_string(c_str!("using Serialization"));
          let ans = jl_eval_string(c_str!("IOBuffer()"));
          // Specialize serializing common types. This saves ~20x on serialization (50ms to 20µs)
          let func = jl_get_function(jl_main_module, "serialize");
          jl_call2(func, ans, jl_box_bool(0));
          jl_call2(func, ans, jl_box_char(0));
          jl_call2(func, ans, jl_eval_string(c_str!("\"0\"")));
          jl_call2(func, ans, jl_box_int8(0));
          jl_call2(func, ans, jl_box_int16(0));
          jl_call2(func, ans, jl_box_int32(0));
          jl_call2(func, ans, jl_box_int64(0));
          jl_call2(func, ans, jl_box_uint8(0));
          jl_call2(func, ans, jl_box_uint16(0));
          jl_call2(func, ans, jl_box_uint32(0));
          jl_call2(func, ans, jl_box_uint64(0));
          jl_call2(func, ans, jl_box_float32(0.0));
          jl_call2(func, ans, jl_box_float64(0.0));
        }
        for stream in listener.incoming() {
          let stream = stream.unwrap();
          handle_connection(stream, i);
        }
      }
      Ok(ForkResult::Parent { child: _ }) => {}
      Err(_) => println!("Fork failed")
    }
  }
  // Close socket in parent
  drop(listener);
  waitpid(Some(Pid::from_raw(-1)), None).unwrap();
}

fn write_contents(
  mut stream: TcpStream,
  code: i64,
  contents: &[u8],
  headers: Option<Vec<(&str, &str)>>
) {
  let mut headers = headers.unwrap_or_default();
  let content_len = contents.len().to_string();
  headers.push(("content-length", &content_len));
  let header = format!(
    "HTTP/1.1 {} OK\n{}\n\n",
    code,
    headers
      .iter()
      .map(|(key, val)| format!("{}: {}", key, val))
      .collect::<Vec<String>>()
      .join("\r\n")
  );
  stream.write(header.as_bytes()).unwrap();
  stream.write(contents).unwrap();
  stream.flush().unwrap();
}

macro_rules! check_julia_error {
  ($stream:expr) => {
    // https://github.com/JuliaLang/julia/blob/f6b51abb294998571ff88a95b50a15ce062a2994/test/embedding/embedding.c
    if !jl_exception_occurred().is_null() {
      // https://discourse.julialang.org/t/julia-exceptions-in-c/18387
      let err = jl_unbox_voidpointer(jl_eval_string(c_str!("pointer(sprint(showerror, ccall(:jl_exception_occurred, Any, ())))")));
      let err = CStr::from_ptr(err as *const i8);
      return write_contents($stream, 400, err.to_bytes(), None);
    }
  }
}

fn get_expected_type(column: &Column) -> *mut jl_datatype_t {
  unsafe {
    match column.r#type {
      ColumnType::I8 => jl_int8_type,
      ColumnType::I16 => jl_int16_type,
      ColumnType::I32 => jl_int32_type,
      ColumnType::I64 => jl_int64_type,
      ColumnType::U8 | ColumnType::Symbol8 => jl_uint8_type,
      ColumnType::U16 | ColumnType::Symbol16 => jl_uint16_type,
      ColumnType::U32 | ColumnType::Symbol32 => jl_uint32_type,
      ColumnType::U64 => jl_uint64_type,
      ColumnType::F32 | ColumnType::Currency => jl_float32_type,
      ColumnType::F64 => jl_float64_type,
      ColumnType::Timestamp => match column.size {
        8 => jl_uint64_type,
        4 => jl_uint32_type,
        2 => jl_uint16_type,
        1 => jl_uint8_type,
        _ => panic!("Invalid timestamp column size")
      }
    }
  }
}

pub fn string_to_datetime<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
  D: de::Deserializer<'de>
{
  struct StringVisitor;

  impl<'de> de::Visitor<'de> for StringVisitor {
    type Value = i64;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
      formatter.write_str("a rfc 3339 string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      let convenience_format = "%Y-%m-%d";
      match DateTime::parse_from_rfc3339(&value) {
        Ok(date) => Ok(date.timestamp_nanos()),
        Err(_e) => match NaiveDate::parse_from_str(&value, &convenience_format) {
          Ok(date) => Ok(date.and_hms(0, 0, 0).timestamp_nanos()),
          Err(_e) => {
            let msg = format!("Could not parse {} in RFC3339 or {} format", &value, &convenience_format);
            Err(E::custom(msg))
          }
        }
      }
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      Ok(value as i64)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      Ok(value)
    }
  }
  deserializer.deserialize_any(StringVisitor)
}

#[derive(Deserialize)]
struct Query {
  table: String,
  query: String,
  #[serde(deserialize_with = "string_to_datetime")]
  from:  i64,
  #[serde(deserialize_with = "string_to_datetime")]
  to:    i64
}

fn handle_connection(mut stream: TcpStream, process_num: i64) {
  let mut buffer = [0; 4096];
  let len = stream.read(&mut buffer).unwrap();
  let req = String::from_utf8_lossy(&buffer[..len]);
  let end = req.find('\n').unwrap();
  let url = &req[..end];
  let mut parts = url.split(' ');
  let method = parts.next().unwrap();
  let resource = parts.next().unwrap();

  if method == "GET" {
    println!("{}: {} {}", process_num, method, resource);
    if resource == "/favicon.ico" {
      let headers = vec![
        ("cache-control", "public, max-age=191200"),
        ("content-type", "image/x-icon"),
      ];
      write_contents(stream, 200, include_bytes!("../zdb.ico"), Some(headers));
    } else if resource == "/" {
      write_contents(stream, 200, include_bytes!("../hello.html"), None);
    } else {
      write_contents(stream, 404, "Not found".as_bytes(), None);
    }
  } else if method == "POST" {
    let body_start = req.find("\n\r\n").unwrap() + 3;
    let body = &req[body_start..];
    let query = serde_json::from_str::<Query>(body);
    match query {
      Err(err) => {
        let err = format!("error parsing request: {}", err.to_string());
        return write_contents(stream, 400, err.as_bytes(), None);
      }
      Ok(query) => {
        let table = Table::open(&query.table);
        if let Err(err) = table {
          let err = format!("error opening table: {}", err.to_string());
          return write_contents(stream, 400, err.as_bytes(), None);
        }
        let table = table.unwrap();

        // Clear previously set module
        let jl_string = CString::new(format!("module Scan {}\nend", query.query)).unwrap();
        unsafe {
          jl_eval_string(jl_string.as_ptr());
          check_julia_error!(stream);
          let scan_fn = jl_eval_string(c_str!("Scan.scan"));
          if !jl_exception_occurred().is_null() || !jl_typeis(scan_fn, jl_function_type) {
            return write_contents(
              stream,
              400,
              "must define function \"scan\" in query".as_bytes(),
              None
            );
          }
          let arg_names = jl_eval_string(c_str!("typeof(Scan.scan).name.mt.defs.func.slot_syms"));
          let arg_names = from_raw_parts(jl_string_data(arg_names), jl_string_len(arg_names) - 1);
          let arg_names = String::from_utf8(arg_names.to_vec()).unwrap();
          let arg_names = arg_names.split('\0').skip(1).collect::<Vec<&str>>();
          let arg_types =
            jl_eval_string(c_str!("typeof(Scan.scan).name.mt.defs.sig.types")) as *mut jl_svec_t;
          let arg_types = from_raw_parts(
            jl_svec_data(arg_types).add(1) as *mut *mut jl_datatype_t,
            (*arg_types).length - 1
          );

          for (arg_name, arg_type) in arg_names.iter().zip(arg_types.iter()).skip(1) {
            let column = table.schema.columns.iter().find(|c| &c.name == arg_name);
            if column.is_none() {
              let err = format!(
                "column {} does not exist on table {}",
                arg_name, table.schema.name
              );
              return write_contents(stream, 400, err.as_bytes(), None);
            }
            let column = column.unwrap();
            let expected_type = get_expected_type(&column);
            let arg_params = (*(*arg_type)).parameters as *mut jl_svec_t;
            let arg_params = from_raw_parts(
              jl_svec_data(arg_params) as *mut *mut jl_value_t,
              (*arg_params).length
            );
            if arg_params.len() != 2
              || arg_params[0] != expected_type as *mut jl_value_t
              || *arg_params[1] != 1
            {
              let expected_type = jl_symbol_name((*(*expected_type).name).name);
              let expected_type = CStr::from_ptr(expected_type as *const i8);
              let mut err = format!(
                "expected parameter {} to be of type Vector{{{:?}}}",
                arg_name, expected_type
              );
              err.retain(|c| c != '"');
              return write_contents(stream, 400, err.as_bytes(), None);
            }
          }
          let partitions = table.partition_iter(query.from, query.to, arg_names);
          let mut res = jl_nothing;
          let now = Instant::now();
          for partition in partitions {
            let mut args: Vec<*mut jl_value_t> = Vec::new();
            for (partition, arg_type) in partition.iter().zip(arg_types.iter()) {
              args.push(jl_ptr_to_array_1d(
                *arg_type as *mut jl_value_t,
                partition.get_u8().as_mut_ptr() as *mut c_void,
                partition.row_count,
                0
              ) as *mut jl_value_t);
            }
            res = jl_call(scan_fn, args.as_mut_ptr(), args.len() as i32);
            check_julia_error!(stream);
          }
          println!("scan {:?}", now.elapsed());
          jl_call1(jl_eval_string(c_str!("println")), res);
          let now = Instant::now();

          let func = jl_get_function(jl_main_module, "serialize");
          let ans = jl_eval_string(c_str!("IOBuffer()"));
          jl_call2(func, ans, res);
          let data = *(jl_get_field(ans, c_str!("data")) as *const jl_array_t);
          let data = from_raw_parts(data.data as *const u8, data.length);
          let res = format!("{:#04x?}", data);
          println!("serialize {:?}", now.elapsed());
          write_contents(stream, 200, res.as_bytes(), None);
        }
      }
    }
  }
}
