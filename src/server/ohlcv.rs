use crate::{server::query::string_to_nanoseconds, table::Table, schema::ColumnType};
use serde::Serialize;
use std::{
  collections::HashMap,
  io::{Error, ErrorKind}
};

fn querify<'a>(string: &'a str) -> Vec<(&'a str, &'a str)> {
  let mut v = Vec::new();
  for pair in string.split('&') {
    let mut it = pair.split('=').take(2);
    let kv = match (it.next(), it.next()) {
      (Some(k), Some(v)) => (k, v),
      _ => continue
    };
    v.push(kv);
  }
  v
}

fn get_symbols<'a>(query_params: Vec<(&'a str, &'a str)>) -> Option<Vec<&'a str>> {
  for (k, v) in query_params {
    if k == "symbols" {
      return Some(v.split(',').map(|s| s.trim()).collect::<Vec<_>>());
    }
  }
  None
}

#[derive(Serialize)]
struct OHLCVs {
  t: Vec<i64>,
  o: Vec<f32>,
  h: Vec<f32>,
  l: Vec<f32>,
  c: Vec<f32>,
  v: Vec<u64>
}

#[derive(Serialize)]
struct OHLCVsResponse {
  results:  HashMap<String, OHLCVs>,
  min_date: i64,
  max_date: i64
}

pub fn ohlcv(path: &str) -> std::io::Result<Vec<u8>> {
  let mut query_parts = path.split('?');
  let mut parts = query_parts.next().unwrap().split('/');
  let symbol_query = match query_parts.next() {
    Some(query_params) => get_symbols(querify(query_params)),
    None => None
  };
  parts.next();
  parts.next();
  let table_name = parts.next(); // TODO: duration like 5Minutes
  let from = parts.next();
  let to = parts.next();

  if table_name.is_none() || from.is_none() || to.is_none() {
    return Err(Error::new(
      ErrorKind::Other,
      "url must be in format /ohlcv/{table}/{from}/{to}"
    ));
  }
  let mut from = string_to_nanoseconds(from.unwrap())?;
  let mut to = string_to_nanoseconds(to.unwrap())?;
  if from > to {
    let tmp = from;
    from = to;
    to = tmp;
  }
  let table = Table::open(&table_name.unwrap())?;

  let partitions = table.partition_iter(from, to, vec![
    "ts", "sym", "open", "high", "low", "close", "volume",
  ]);
  let total_rows = partitions
    .partitions
    .iter()
    .map(|(_, p)| p.row_count)
    .sum::<usize>();
  let mut res = OHLCVsResponse {
    results:  HashMap::new(),
    min_date: i64::MAX,
    max_date: i64::MIN
  };
  for partition in partitions {
    for i in 0..partition[0].row_count {
      let symbol = partition[1].get_symbol(i);
      match symbol_query {
        Some(ref symbols) => {
          if symbols[0] != "" && !symbols.contains(&symbol) {
            continue;
          }
        }
        None => {}
      };
      let ohlcvs = match res.results.get_mut(symbol) {
        Some(v) => v,
        None => {
          let ohlcvs = OHLCVs {
            t: Vec::with_capacity(total_rows),
            o: Vec::with_capacity(total_rows),
            h: Vec::with_capacity(total_rows),
            l: Vec::with_capacity(total_rows),
            c: Vec::with_capacity(total_rows),
            v: Vec::with_capacity(total_rows)
          };
          res.results.insert(symbol.to_string(), ohlcvs);
          res.results.get_mut(symbol).unwrap()
        }
      };
      let ts = partition[0].get_timestamp(i);
      if ts > res.max_date {
        res.max_date = ts;
      }
      if ts < res.min_date {
        res.min_date = ts;
      }
      ohlcvs.t.push(ts);
      ohlcvs.o.push(partition[2].get_currency()[i]);
      ohlcvs.h.push(partition[3].get_currency()[i]);
      ohlcvs.l.push(partition[4].get_currency()[i]);
      ohlcvs.c.push(partition[5].get_currency()[i]);
      ohlcvs.v.push(match partition[6].column.r#type {
        ColumnType::U64 => partition[6].get_u64()[i],
        ColumnType::U32 => partition[6].get_u32()[i] as u64,
        ColumnType::U16 => partition[6].get_u16()[i] as u64,
        ColumnType::U8 => partition[6].get_u8()[i] as u64,
        _ => panic!("Unsupported volume column type {:?}", partition[6].column.r#type)
      });
    }
  }

  Ok(serde_json::to_vec(&res).unwrap())
}
