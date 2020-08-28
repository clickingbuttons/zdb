use std::path::PathBuf;

pub fn get_data_path(name: &str) -> PathBuf {
  let mut path = PathBuf::from("data");
  path.push(name);
  path
}

pub fn get_meta_path(data_path: &PathBuf) -> PathBuf {
  let mut path = data_path.clone();
  path.push("_meta");
  path
}
