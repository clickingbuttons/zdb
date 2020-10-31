  // let mut rows = Vec::<Vec<RowValue>>::new();
  // let mut sum = 0.0;

  // agg1d.scan(
  //   0,
  //   date!(1972 - 02 - 01).nanoseconds(),
  //   vec!["ts", "ticker", "close", "volume"],
  //   |row: Vec<RowValue>| {
  //     if row[1].get_symbol() == "TX" {
  //       sum += row[2].get_currency() as f64;
  //       rows.push(row);
  //     }
  //   }
  // );

  // for r in rows {
  //   println!(
  //     "{} {:7} {:<9} {:>10}",
  //     r[0].get_timestamp().format("%Y-%m-%d %H:%M:%S.%N"),
  //     r[1].get_symbol(),
  //     r[2].get_f32().format_currency(7),
  //     r[3].get_u64()
  //   );
  // }
  // println!("Sum {}", sum);