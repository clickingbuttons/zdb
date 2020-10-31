use chrono::{
  prelude::*,
  Duration, NaiveDate,
  Weekday::{Mon, Sat, Sun, Thu}
};

fn get_easter(year: i32) -> NaiveDate {
  let aa = year % 19;
  let bb = year / 100;
  let cc = year % 100;
  let dd = bb / 4;
  let ee = bb % 4;
  let ff = (bb + 8) / 25;
  let gg = (bb - ff + 1) / 3;
  let hh = (19 * aa + bb - dd - gg + 15) % 30;
  let ii = cc / 4;
  let kk = cc % 4;
  let ll = (32 + 2 * ee + 2 * ii - hh - kk) % 7;
  let mm = (aa + 11 * hh + 22 * ll) / 451;
  let month = (hh + ll - 7 * mm + 114) / 31;
  let day = (hh + ll - 7 * mm + 114) % 31 + 1;

  NaiveDate::from_ymd(year, month as u32, day as u32)
}

fn is_weekend(date: &NaiveDate) -> bool { date.weekday() == Sat || date.weekday() == Sun }

pub fn is_market_open(date: &NaiveDate) -> bool {
  let year = date.year();

  // Weekend
  if is_weekend(&date) {
    return false;
  }

  // New year's
  let mut new_year = NaiveDate::from_ymd(year, 1, 1);
  while is_weekend(&new_year) {
    new_year += Duration::days(1);
  }
  if date == &new_year {
    return false;
  }

  // MLK day on 3rd Mon of January
  if date == &NaiveDate::from_weekday_of_month(year, 1, Mon, 3) {
    return false;
  }

  // Washington's Birthday on 3rd Mon of February
  if date == &NaiveDate::from_weekday_of_month(year, 2, Mon, 3) {
    return false;
  }

  // Good Friday
  let mut easter = get_easter(year);
  easter -= Duration::days(2);
  if date == &easter {
    return false;
  }

  // Memorial Day
  let mut memorial_day = NaiveDate::from_ymd(year, 5, 31);
  while memorial_day.weekday() != Mon {
    memorial_day -= Duration::days(1);
  }
  if date == &memorial_day {
    return false;
  }

  // Independence Day
  let mut independence_day = NaiveDate::from_ymd(year, 7, 4);
  if independence_day.weekday() == Sat {
    independence_day -= Duration::days(1);
  } else if independence_day.weekday() == Sun {
    independence_day += Duration::days(1);
  }
  if date == &independence_day {
    return false;
  }

  // Labor Day on first Mon of September
  if date == &NaiveDate::from_weekday_of_month(year, 9, Mon, 1) {
    return false;
  }

  // Thanksgiving on fourth Thu of November
  if date == &NaiveDate::from_weekday_of_month(year, 11, Thu, 4) {
    return false;
  }

  // Christmas
  let mut christmas = NaiveDate::from_ymd(year, 12, 25);
  if christmas.weekday() == Sat {
    christmas -= Duration::days(1);
  } else if christmas.weekday() == Sun {
    christmas += Duration::days(1);
  }
  if date == &christmas {
    return false;
  }

  let disasters = &[
    // Ronald Reagan dead at 93
    // https://money.cnn.com/2004/06/11/markets/reagan_closings/index.htm
    NaiveDate::from_ymd(2004, 6, 11),
    // National Day of Mourning for Gerald R. Ford
    // https://georgewbush-whitehouse.archives.gov/news/releases/2006/12/20061228-2.html
    NaiveDate::from_ymd(2007, 1, 2),
    // Hurricane Sandy
    NaiveDate::from_ymd(2012, 10, 29),
    NaiveDate::from_ymd(2012, 10, 30),
    // George H.W. Bush dead at 94
    NaiveDate::from_ymd(2018, 12, 5)
  ];

  if disasters.contains(date) {
    return false;
  }

  return true;
}

#[cfg(test)]
mod tests {
  use crate::calendar::us_equity::{get_easter, is_market_open};
  use chrono::{
    NaiveDate,
    Weekday::{Mon, Thu}
  };

  #[test]
  fn mlk() {
    assert_eq!(
      NaiveDate::from_weekday_of_month(2004, 1, Mon, 3),
      NaiveDate::from_ymd(2004, 01, 19)
    );
  }

  #[test]
  fn washington() {
    assert_eq!(
      NaiveDate::from_weekday_of_month(2004, 2, Mon, 3),
      NaiveDate::from_ymd(2004, 02, 16)
    );
  }

  #[test]
  fn easter() {
    assert_eq!(get_easter(2004), NaiveDate::from_ymd(2004, 04, 11));
  }

  #[test]
  fn good_friday() {
    assert_eq!(is_market_open(&NaiveDate::from_ymd(2004, 04, 09)), false);
  }

  #[test]
  fn labor() {
    assert_eq!(
      NaiveDate::from_weekday_of_month(2004, 9, Mon, 1),
      NaiveDate::from_ymd(2004, 9, 6)
    );
  }

  #[test]
  fn thanksgiving() {
    assert_eq!(
      NaiveDate::from_weekday_of_month(2004, 11, Thu, 4),
      NaiveDate::from_ymd(2004, 11, 25)
    );
  }

  #[test]
  fn christmas() {
    assert_eq!(is_market_open(&NaiveDate::from_ymd(2004, 12, 25)), false);
  }
}
