use chrono::NaiveDateTime;

pub mod us_equity;

pub trait ToNaiveDateTime {
  fn to_naive_date_time(self) -> NaiveDateTime;
}

impl ToNaiveDateTime for i64 {
  fn to_naive_date_time(self) -> NaiveDateTime {
    let seconds = self / 1_000_000_000;
    let nanoseconds = self % 1_000_000_000;
    NaiveDateTime::from_timestamp(seconds, nanoseconds as u32)
  }
}
