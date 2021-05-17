use nix::{
  sys::{signal, wait::waitpid},
  unistd::{fork, ForkResult, Pid}
};
use std::{
  net::TcpListener,
  process::exit,
  env::var
};
use zdb::server::handle_connection;
use zdb::server::julia::{
  init_julia,
  jl_atexit_hook
};

extern "C" fn handle_sigint(_: i32) {
  unsafe { jl_atexit_hook(0) };
  exit(0);
}

fn main() {
  let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

  let num_threads = var("ZDB_NUM_THREADS")
    .unwrap_or("12".to_string())
    .parse::<i64>().unwrap();

  for i in 0..num_threads {
    match unsafe { fork() } {
      Ok(ForkResult::Child) => {
        println!("fork {}", i);
        let sig_action = signal::SigAction::new(
          signal::SigHandler::Handler(handle_sigint),
          signal::SaFlags::SA_NODEFER,
          signal::SigSet::empty()
        );
        unsafe {
          signal::sigaction(signal::SIGINT, &sig_action).unwrap();
        }
        init_julia();
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

