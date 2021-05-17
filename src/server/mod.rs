pub mod julia;
pub mod query;

use crate::server::query::{
  serialize_jl_value,
  Query,
  run_query
};
use std::{
  io::prelude::*,
  net::TcpStream
};

pub fn handle_connection(mut stream: TcpStream, process_num: i64) {
  let mut buffer = [0; 4096];
  let len = stream.read(&mut buffer).unwrap();
  let req = String::from_utf8_lossy(&buffer[..len]);
  let end = req.find('\n').unwrap();
  let url = &req[..end];
  let mut parts = url.split(' ');
  let method = parts.next().unwrap();
  let resource = parts.next().unwrap();
  let mut parts = resource.split('?');
  let resource = parts.next().unwrap();

  if method == "GET" {
    println!("{}: {} {}", process_num, method, resource);
    if resource == "/favicon.ico" {
      let headers = vec![
        ("cache-control", "public, max-age=191200"),
        ("content-type", "image/x-icon"),
      ];
      write_contents(stream, 200, include_bytes!("./static/zdb.ico"), Some(headers));
    } else if resource == "/" {
      write_contents(stream, 200, include_bytes!("./static/hello.html"), None);
    } else if resource == "/symbols" {
      let query = parts.next();
      println!("symbol {:?}", query);
    } else {
      write_contents(stream, 404, "Not found".as_bytes(), None);
    }
  } else if method == "POST" && resource == "/q" {
    let body_start = req.find("\n\r\n").unwrap() + 3;
    let body = &req[body_start..];
    let query = serde_json::from_str::<Query>(body);
    match query {
      Err(err) => {
        let err = format!("error parsing request: {}", err.to_string());
        return write_contents(stream, 400, err.as_bytes(), None);
      }
      Ok(query) => match run_query(&query) {
        Ok(value) => {
          let serialized = serialize_jl_value(value);
          let res = format!("{:#04x?}", serialized);
          write_contents(stream, 200, res.as_bytes(), None);
        },
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

