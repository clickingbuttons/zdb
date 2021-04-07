#![allow(non_camel_case_types)]
use std::os::raw::{c_void,c_char,c_int,c_ulong};

type jl_value_t = u8;
type jl_function_t = jl_value_t;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_sym_t {
  left: *mut jl_sym_t,
  right: *mut jl_sym_t,
  hash: usize,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct htable_t {
  size: usize,
  table: *mut *mut c_void,
  _space: [*mut c_void; 32usize],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct arraylist_t {
  len: usize,
  max: usize,
  items: *mut *mut c_void,
  _space: [*mut c_void; 29usize],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_uuid_t {
  hi: u64,
  lo: u64,
}
type pthread_t = c_ulong;
type jl_thread_t = pthread_t;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_mutex_t {
  owner: jl_thread_t,
  count: u32,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_module_t {
  name: *mut jl_sym_t,
  parent: *mut jl_module_t,
  bindings: htable_t,
  usings: arraylist_t,
  build_id: u64,
  uuid: jl_uuid_t,
  primary_world: usize,
  counter: u32,
  nospecialize: i32,
  optlevel: i8,
  compile: i8,
  infer: i8,
  istopmod: u8,
  lock: jl_mutex_t,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_svec_t {
  length: usize,
}
type jl_array_flags_t = u16;
#[repr(C)]
#[derive(Copy, Clone)]
union jl_array_union {
  maxsize: usize,
  ncols: usize,
}
#[repr(C)]
#[derive(Copy, Clone)]
struct jl_array_t {
  data: *mut c_void,
  length: usize,
  flags: jl_array_flags_t,
  elsize: u16,
  offset: u32,
  nrows: usize,
  maxsize_ncols: jl_array_union,
}
#[repr(C)]
#[derive(Copy, Clone)]
union jl_method_def {
  value: *mut jl_value_t,
  module: *mut jl_module_t,
  method: *mut jl_method_t
}
type jl_call_t = Option<
  unsafe extern "C" fn(
    arg1: *mut jl_value_t,
    arg2: *mut *mut jl_value_t,
    arg3: u32,
    arg4: *mut jl_code_instance_t,
  ) -> *mut jl_value_t,
>;
type jl_callptr_t = jl_call_t;
type jl_fptr_args_t = Option<
  unsafe extern "C" fn(
    arg1: *mut jl_value_t,
    arg2: *mut *mut jl_value_t,
    arg3: u32,
  ) -> *mut jl_value_t,
>;
type jl_fptr_sparam_t = Option<
  unsafe extern "C" fn(
    arg1: *mut jl_value_t,
    arg2: *mut *mut jl_value_t,
    arg3: u32,
    arg4: *mut jl_svec_t,
  ) -> *mut jl_value_t,
>;
#[repr(C)]
#[derive(Copy, Clone)]
union jl_generic_specptr_t {
  fptr: *mut c_void,
  fptr1: jl_fptr_args_t,
  fptr3: jl_fptr_sparam_t,
  _bindgen_union_align: u64,
}
#[repr(C)]
#[derive(Copy, Clone)]
struct jl_code_instance_t {
  def: *mut jl_method_instance_t,
  next: *mut jl_code_instance_t,
  min_world: usize,
  max_world: usize,
  rettype: *mut jl_value_t,
  rettype_const: *mut jl_value_t,
  inferred: *mut jl_value_t,
  isspecsig: u8,
  precompile: u8,
  invoke: jl_callptr_t,
  specptr: jl_generic_specptr_t,
}
#[repr(C)]
#[derive(Copy, Clone)]
struct jl_method_instance_t {
  def: jl_method_def,
  spec_types: *mut jl_value_t,
  sparam_vals: *mut jl_svec_t,
  uninferred: *mut jl_value_t,
  backedges: *mut jl_array_t,
  callbacks: *mut jl_array_t,
  cache: *mut jl_code_instance_t,
  in_inference: u8,
}
type jl_typemap_t = jl_value_t;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_method_t {
  name: *mut jl_sym_t,
  module: *mut jl_module_t,
  file: *mut jl_sym_t,
  line: i32,
  primary_world: usize,
  deleted_world: usize,
  sig: *mut jl_value_t,
  specializations: *mut jl_svec_t,
  speckeyset: *mut jl_array_t,
  slot_syms: *mut jl_value_t,
  source: *mut jl_value_t,
  unspecialized: *mut jl_method_instance_t,
  generator: *mut jl_value_t,
  roots: *mut jl_array_t,
  ccallable: *mut jl_svec_t,
  invokes: *mut jl_typemap_t,
  nargs: i32,
  called: i32,
  nospecialize: i32,
  nkw: i32,
  isva: u8,
  pure_: u8,
  writelock: jl_mutex_t,
}

#[link(name = "julia")]
extern "C" {
  pub fn jl_init__threading();
  pub fn jl_eval_string(str: *const c_char) -> *mut jl_value_t;
  pub fn jl_unbox_float64(v: *mut jl_value_t) -> f64;
  pub fn jl_exception_occurred() -> *mut jl_value_t;
  pub static mut jl_base_module: *mut jl_module_t;
  pub fn jl_call2(f: *mut jl_function_t, a: *mut jl_value_t, b: *mut jl_value_t) -> *mut jl_value_t;
  pub fn jl_atexit_hook(status: c_int);
}

