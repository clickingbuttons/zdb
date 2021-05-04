#![allow(non_camel_case_types)]
use std::{
  ffi::CString,
  mem::size_of,
  os::raw::{c_char, c_int, c_ulong, c_void}
};

pub type jl_value_t = u8;
type jl_function_t = jl_value_t;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_sym_t {
  pub left:  *mut jl_sym_t,
  pub right: *mut jl_sym_t,
  pub hash:  usize
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct htable_t {
  size:   usize,
  table:  *mut *mut c_void,
  _space: [*mut c_void; 32usize]
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct arraylist_t {
  len:    usize,
  max:    usize,
  items:  *mut *mut c_void,
  _space: [*mut c_void; 29usize]
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_uuid_t {
  hi: u64,
  lo: u64
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct jl_mutex_t {
  owner: c_ulong,
  count: u32
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_module_t {
  pub name: *mut jl_sym_t,
  pub parent: *mut jl_module_t,
  bindings: htable_t,
  usings: arraylist_t,
  pub build_id: u64,
  uuid: jl_uuid_t,
  pub primary_world: usize,
  pub counter: u32,
  pub nospecialize: i32,
  pub optlevel: i8,
  pub compile: i8,
  pub infer: i8,
  pub istopmod: u8,
  lock: jl_mutex_t
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_svec_t {
  pub length: usize /* pointer size aligned
                     * pub data: *mut *mut jl_value_t, */
}
#[repr(C)]
#[derive(Copy, Clone)]
union jl_array_union {
  maxsize: usize,
  ncols:   usize
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct jl_array_t {
  pub data:      *mut c_void,
  pub length:    usize,
  flags:         u16,
  elsize:        u16,
  offset:        u32,
  nrows:         usize,
  maxsize_ncols: jl_array_union
}

type jl_typemap_t = jl_value_t;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_methtable_t {
  name:      *mut jl_sym_t,
  defs:      *mut jl_typemap_t,
  leafcache: *mut jl_array_t,
  cache:     *mut jl_typemap_t,
  max_args:  isize,
  kwsorter:  *mut jl_value_t,
  module:    *mut jl_module_t,
  backedges: *mut jl_array_t,
  writelock: jl_mutex_t,
  offs:      u8,
  frozen:    u8
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_typename_t {
  pub name: *mut jl_sym_t,
  pub module: *mut jl_module_t,
  pub names: *mut jl_svec_t,
  pub wrapper: *mut jl_value_t,
  pub cache: *mut jl_svec_t,
  pub linearcache: *mut jl_svec_t,
  pub hash: isize,
  pub mt: *mut jl_methtable_t,
  pub partial: *mut jl_array_t
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_datatype_layout_t {
  nfields:   u32,
  npointers: u32,
  first_ptr: i32,
  alignment: u16,
  flags:     u16
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct jl_datatype_t {
  pub name: *mut jl_typename_t,
  pub super_: *mut jl_datatype_t,
  pub parameters: *mut jl_svec_t,
  pub types: *mut jl_svec_t,
  pub names: *mut jl_svec_t,
  pub instance: *mut jl_value_t,
  pub layout: *const jl_datatype_layout_t,
  pub size: i32,
  pub ninitialized: i32,
  pub hash: u32,
  pub abstract_: u8,
  pub mutabl: u8,
  pub hasfreetypevars: u8,
  pub isconcretetype: u8,
  pub isdispatchtuple: u8,
  pub isbitstype: u8,
  pub zeroinit: u8,
  pub isinlinealloc: u8,
  pub has_concrete_subtype: u8,
  pub cached_by_hash: u8
}

#[link(name = "julia")]
extern "C" {
  pub fn jl_init__threading();
  pub fn jl_eval_string(str: *const c_char) -> *mut jl_value_t;
  pub fn jl_box_bool(x: i8) -> *mut jl_value_t;
  pub fn jl_box_char(x: c_char) -> *mut jl_value_t;
  pub fn jl_box_int8(x: i8) -> *mut jl_value_t;
  pub fn jl_box_int16(x: i16) -> *mut jl_value_t;
  pub fn jl_box_int32(x: i32) -> *mut jl_value_t;
  pub fn jl_box_int64(x: i64) -> *mut jl_value_t;
  pub fn jl_box_uint8(x: u8) -> *mut jl_value_t;
  pub fn jl_box_uint16(x: u16) -> *mut jl_value_t;
  pub fn jl_box_uint32(x: u32) -> *mut jl_value_t;
  pub fn jl_box_uint64(x: u64) -> *mut jl_value_t;
  pub fn jl_box_float32(x: f32) -> *mut jl_value_t;
  pub fn jl_box_float64(x: f64) -> *mut jl_value_t;
  pub fn jl_unbox_voidpointer(v: *mut jl_value_t) -> *mut c_void;
  pub fn jl_exception_occurred() -> *mut jl_value_t;
  pub static mut jl_main_module: *mut jl_module_t;
  pub static mut jl_core_module: *mut jl_module_t;
  pub static mut jl_base_module: *mut jl_module_t;
  pub static mut jl_method_type: *mut jl_datatype_t;
  pub static mut jl_method_instance_type: *mut jl_datatype_t;
  pub static mut jl_function_type: *mut jl_datatype_t;
  pub static mut jl_datatype_type: *mut jl_datatype_t;
  pub static mut jl_float32_type: *mut jl_datatype_t;
  pub static mut jl_float64_type: *mut jl_datatype_t;
  pub static mut jl_int8_type: *mut jl_datatype_t;
  pub static mut jl_int16_type: *mut jl_datatype_t;
  pub static mut jl_int32_type: *mut jl_datatype_t;
  pub static mut jl_int64_type: *mut jl_datatype_t;
  pub static mut jl_uint8_type: *mut jl_datatype_t;
  pub static mut jl_uint16_type: *mut jl_datatype_t;
  pub static mut jl_uint32_type: *mut jl_datatype_t;
  pub static mut jl_uint64_type: *mut jl_datatype_t;
  pub static mut jl_nothing: *mut jl_value_t;
  pub fn jl_isa(a: *mut jl_value_t, t: *mut jl_value_t) -> c_int;
  pub fn jl_call(f: *mut jl_function_t, args: *mut *mut jl_value_t, nargs: i32) -> *mut jl_value_t;
  pub fn jl_call1(f: *mut jl_function_t, a: *mut jl_value_t) -> *mut jl_value_t;
  pub fn jl_call2(f: *mut jl_function_t, a: *mut jl_value_t, b: *mut jl_value_t)
    -> *mut jl_value_t;
  pub fn jl_call3(
    f: *mut jl_function_t,
    a: *mut jl_value_t,
    b: *mut jl_value_t,
    c: *mut jl_value_t
  ) -> *mut jl_value_t;
  pub fn jl_set_global(m: *mut jl_module_t, var: *mut jl_sym_t, val: *mut jl_value_t);
  pub fn jl_get_global(m: *mut jl_module_t, var: *mut jl_sym_t) -> *mut jl_value_t;
  pub fn jl_set_const(m: *mut jl_module_t, var: *mut jl_sym_t, val: *mut jl_value_t);
  pub fn jl_new_module(name: *mut jl_sym_t) -> *mut jl_module_t;
  pub fn jl_symbol(str: *const c_char) -> *mut jl_sym_t;
  pub fn jl_get_field(o: *mut jl_value_t, fld: *const c_char) -> *mut jl_value_t;
  pub fn jl_stderr_obj() -> *mut jl_value_t;
  pub fn jl_typeof_str(v: *mut jl_value_t) -> *const c_char;
  pub fn jl_ptr_to_array_1d(
    atype: *mut jl_value_t,
    data: *mut c_void,
    nel: usize,
    own_buffer: c_int
  ) -> *mut jl_array_t;

  // pub fn jl_get_ptls_states() -> jl_ptls_t;
  pub fn jl_atexit_hook(status: c_int);
}

#[macro_export]
macro_rules! c_str {
  ($lit:expr) => {
    std::ffi::CStr::from_ptr(concat!($lit, "\0").as_ptr() as *const std::os::raw::c_char).as_ptr()
  };
}

pub unsafe fn jl_get_function(module: *mut jl_module_t, name: &str) -> *mut jl_value_t {
  let name = CString::new(name).unwrap();
  jl_get_global(module, jl_symbol(name.as_ptr()))
}

pub unsafe fn jl_typeis(v: *mut jl_value_t, t: *mut jl_datatype_t) -> bool {
  jl_isa(v, t as *mut jl_value_t) != 0
}

#[inline(always)]
pub unsafe fn jl_svec_data(t: *mut jl_svec_t) -> *mut *mut jl_value_t {
  t.cast::<u8>().add(size_of::<jl_svec_t>()).cast()
}

#[inline(always)]
pub unsafe fn jl_string_len(s: *mut jl_value_t) -> usize { *(s.cast()) }

#[inline(always)]
pub unsafe fn jl_string_data(s: *mut jl_value_t) -> *const u8 {
  (s as *const u8).add(size_of::<usize>())
}

macro_rules! llt_align {
  ($x:expr, $sz:expr) => {
    (($x) + ($sz) - 1) & !(($sz) - 1)
  };
}

#[inline]
pub unsafe fn jl_symbol_name(s: *mut jl_sym_t) -> *mut u8 {
  s.cast::<u8>()
    .add(llt_align!(size_of::<jl_sym_t>(), size_of::<*mut c_void>()))
}
