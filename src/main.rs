extern crate libc;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use nix::unistd::{fork, ForkResult};
use std::process::exit;
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use nix::sys::wait::waitpid;
use nix::unistd::Pid;
use nix::sys::signal;

#[allow(non_camel_case_types)]
type jl_value_t = u8;
#[link(name = "julia")]
extern {
  fn jl_init__threading();
  fn jl_eval_string(str: *const c_char) -> *mut jl_value_t;
  fn jl_unbox_float64(v: *mut jl_value_t) -> f64;
  fn jl_atexit_hook(status: c_int);
}

extern "C" fn handle_sigint(_: i32) {
  unsafe { jl_atexit_hook(0) };
  exit(0);
}

fn main() {
  let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

  for i in 0..3 {
    match unsafe {fork()} {
      Ok(ForkResult::Child) => {
        println!("spawn {}", i);
        let sig_action = signal::SigAction::new(
          signal::SigHandler::Handler(handle_sigint),
          signal::SaFlags::SA_NODEFER,
          signal::SigSet::empty(),
        );
        unsafe {
          jl_init__threading();
          signal::sigaction(signal::SIGINT, &sig_action).unwrap();
        }
        for stream in listener.incoming() {
          let stream = stream.unwrap();
          handle_connection(stream, i);
        }
      }
      Ok(ForkResult::Parent { child: _ }) => {}
      Err(_) => println!("Fork failed"),
    }
  }
  // Close socket in parent
  drop(listener);
  waitpid(Some(Pid::from_raw(-1)), None).unwrap();
}

fn write_contents(mut stream: TcpStream, code: i64, contents: &[u8], headers: Option<Vec<(&str, &str)>>) {
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

fn handle_connection(mut stream: TcpStream, process_num: i64) {
  let mut buffer = [0; 4096];
  stream.read(&mut buffer).unwrap();
  let req = String::from_utf8_lossy(&buffer[..]);
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
        ("content-type", "image/x-icon")
      ];
      write_contents(stream, 200, include_bytes!("../zdb.ico"), Some(headers));
    }
    else if resource == "/" {
      write_contents(stream, 200, include_bytes!("../hello.html"), None);
    }
    else if resource.starts_with("/query") {
      let start = resource.find('?').unwrap();
      let mut query = "";
      &resource[start + 1..]
        .split('&')
        .into_iter()
        .map(|pair| {
          let mut split = pair.split('=');
          (split.next(), split.next())
        })
        .filter(|(key, _val)| key.is_some())
        .for_each(|(key, val)| {
          if key == Some("query") {
            query = val.unwrap();
          }
        });
      println!("{:?}", query);
      let jl_string = CString::new(query).unwrap();
      unsafe {
        let res = jl_eval_string(jl_string.as_ptr());
        let val = jl_unbox_float64(res).to_string();
        write_contents(stream, 200, val.as_bytes(), None);
      }
    }
    else {
      write_contents(stream, 404, "Not found".as_bytes(), None);
    }
  }
}

