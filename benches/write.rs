use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::{prelude::ThreadRng, Rng};
use zdb::{schema::*, table::*};

static ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn generate_symbol(num_chars: usize, rng: &mut ThreadRng) -> String {
  let mut res = String::with_capacity(num_chars);
  for _ in 0..num_chars {
    let rand_index = rng.gen_range(0, ALPHABET.len());
    res += &ALPHABET[rand_index..rand_index + 1];
  }

  res
}

fn generate_row(nanosecond_offset: i64, rng: &mut ThreadRng) -> (i64, String, f32, f32, f32, f32, f32, u64) {
  (
    nanosecond_offset,
    generate_symbol(rng.gen_range(1, 5), rng),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen(),
    rng.gen()
  )
}

fn generate_rows(
  row_count: usize,
  rng: &mut ThreadRng
) -> Vec<(i64, String, f32, f32, f32, f32, f32, u64)> {
  let mut res = Vec::with_capacity(row_count);

  for i in 0..row_count {
    let row = generate_row(i as i64, rng);
    res.push(row);
  }

  res
}

fn write_rows(rows: &Vec<(i64, String, f32, f32, f32, f32, f32, u64)>, index: i64) {
  let schema = Schema::new(&format!("agg1d{}", index))
    .add_cols(vec![
      Column::new("ticker", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("close_un", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
    ])
    // Specifiers: https://docs.rs/time/0.2.22/time/index.html#formatting
    .partition_by("%Y");

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");
  // Maybe one day we can do this dynamically...
  for r in rows {
    let ts = match agg1d.get_last_ts() {
      Some(ts) => ts,
      None => 0
    };
    agg1d.put_timestamp(ts + r.0);
    agg1d.put_symbol(&r.1);
    agg1d.put_currency(r.2);
    agg1d.put_currency(r.3);
    agg1d.put_currency(r.4);
    agg1d.put_currency(r.5);
    agg1d.put_currency(r.6);
    agg1d.put_u64(r.7);
    agg1d.write();
  }
  agg1d.flush();
}

fn write_bench(c: &mut Criterion) {
  let rows = generate_rows(1_000_000, &mut rand::thread_rng());

  let mut i: i64 = 0;
  c.bench_function("write_rows", move |b| {
    // This will avoid timing the to_vec call.
    b.iter_batched(|| rows.clone(), |data| { write_rows(&data, i); i +=1; }, BatchSize::SmallInput)
  });
}

criterion_group!{
  name = benches;
  config = Criterion::default().sample_size(10);
  targets = write_bench
}
criterion_main!(benches);
