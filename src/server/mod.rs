pub mod julia;
pub mod ohlcv;
pub mod query;

use crate::{
  server::query::{run_query, serialize_jl_value, Query},
  table::Table
};
use ohlcv::ohlcv;
use std::{io::prelude::*, net::TcpStream};

// Since we have to embed Julia we use a process per-connection
static mut BUFFER: [u8; 1024] = [0; 1024];
static mut BODY_BUFFER: [u8; 1024] = [0; 1024];

pub fn handle_connection(mut stream: TcpStream, process_num: i64) {
  let len = unsafe { stream.read(&mut BUFFER).unwrap() };
  let mut headers = [httparse::EMPTY_HEADER; 16];
  let mut req = httparse::Request::new(&mut headers);
  let headers_len = unsafe { req.parse(&BUFFER).unwrap().unwrap() };
  let method = match req.method {
    Some(m) => m,
    None => return write_contents(stream, 400, "No method specified".as_bytes(), None)
  };
  let path = match req.path {
    Some(p) => p,
    None => return write_contents(stream, 400, "No resource specified".as_bytes(), None)
  };
  let body = match method {
    "POST" => {
      unsafe {
        if len > headers_len {
          // Body already written
          Some(&BUFFER[headers_len..len])
        } else {
          // Body coming next
          let len = stream.read(&mut BODY_BUFFER).unwrap();
          Some(&BODY_BUFFER[..len])
        }
      }
    }
    _ => None
  };

  println!("{}: {} {}", process_num, method, path);
  if method == "GET" {
    if path == "/favicon.ico" {
      let headers = vec![
        ("cache-control", "public, max-age=191200"),
        ("content-type", "image/x-icon"),
      ];
      write_contents(
        stream,
        200,
        include_bytes!("./static/zdb.ico"),
        Some(headers)
      );
    } else if path == "/" {
      write_contents(stream, 200, include_bytes!("./static/hello.html"), None);
    } else if path.starts_with("/symbols") {
      let mut parts = path.split('/');
      parts.next();
      parts.next();
      let table_name = parts.next();
      let column = parts.next();
      if table_name.is_none() || column.is_none() {
        return write_contents(
          stream,
          400,
          "url must be in format /symbols/{table}/{column}".as_bytes(),
          None
        );
      }
      let table = Table::open(&table_name.unwrap());
      if let Err(_e) = table {
        let err = format!("table \"{}\" does not exist", table_name.unwrap());
        return write_contents(stream, 400, err.as_bytes(), None);
      }
      let table = table.unwrap();
      match table
        .schema
        .columns
        .iter()
        .position(|c| c.name == column.unwrap())
      {
        Some(index) => {
          let serialized = serde_json::to_vec(&table.column_symbols[index].symbols).unwrap();
          write_contents(stream, 200, &serialized, None);
        }
        None => {
          let err = format!(
            "Column {} does not exist on table {}",
            column.unwrap(),
            table.schema.name
          );
          write_contents(stream, 400, err.as_bytes(), None);
        }
      }
    } else if path.starts_with("/ohlcv") {
      match ohlcv(&path) {
        Err(err) => {
          let err = format!("error parsing ohlcv: {}", err.to_string());
          return write_contents(stream, 400, err.as_bytes(), None);
        }
        Ok(res) => write_contents(stream, 200, &res, None)
      }
    } else {
      write_contents(stream, 404, "Not found".as_bytes(), None);
    }
  } else if method == "POST" && path == "/q" {
    let body = match body {
      Some(b) => b,
      None => return write_contents(stream, 400, "Never receieved body".as_bytes(), None)
    };
    match serde_json::from_slice::<Query>(body) {
      Err(err) => {
        let err = format!("error parsing body: {}", err.to_string());
        return write_contents(stream, 400, err.as_bytes(), None);
      }
      Ok(query) => match run_query(&query) {
        Ok(value) => {
          let serialized = serialize_jl_value(value);
          // let res = format!("{:#04x?}", serialized);
          write_contents(stream, 200, serialized, None);
        }
        Err(err) => write_contents(stream, 400, err.to_string().as_bytes(), None)
      }
    }
  }
}

pub fn write_contents(
  mut stream: TcpStream,
  code: i64,
  contents: &[u8],
  headers: Option<Vec<(&str, &str)>>
) {
  let mut headers = headers.unwrap_or_default();
  let content_len = contents.len().to_string();
  headers.push(("content-length", &content_len));
  headers.push(("access-control-allow-origin", "*"));
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
