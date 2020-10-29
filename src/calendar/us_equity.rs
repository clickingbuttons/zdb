use time::{date,Date,Duration,Weekday::{Monday,Saturday,Sunday,Thursday}};

fn get_easter(year: i32) -> Date {
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

  Date::try_from_ymd(year, month as u8, day as u8).unwrap()
}

fn is_weekend(date: &Date) -> bool {
  date.weekday() == Saturday || date.weekday() == Sunday
}

static DISASTERS: &'static [Date] = &[
  // Ronald Reagan dead at 93
  // https://money.cnn.com/2004/06/11/markets/reagan_closings/index.htm
  date!(2004-6-11),
  // National Day of Mourning for Gerald R. Ford
  // https://georgewbush-whitehouse.archives.gov/news/releases/2006/12/20061228-2.html
  date!(2007-01-02),
  // Hurricane Sandy
  date!(2012-10-29),
  date!(2012-10-30),
  // George H.W. Bush dead at 94
  date!(2018-12-05),
];

fn nth_weekday(year: i32, month: u8, week_num: u8, week_day: u8) -> Date {
  let mut res = Date::try_from_ymd(year, month, 1).unwrap();
  let dif = week_day as i8 - res.weekday() as i8;
  let dif = if dif < 0 { 7 + dif } else { dif };
  res += Duration::days(dif as i64);
  res += Duration::weeks(week_num as i64 - 1);
  res
}

pub fn is_market_open(date: &Date) -> bool {
  let year = date.year();

  // Weekend
  if is_weekend(&date) {
    return false;
  }

  // New year's
  let mut new_year = Date::try_from_ymd(year, 1, 1).unwrap();
  while is_weekend(&new_year) {
    new_year += Duration::days(1);
  }
  if date == &new_year {
    return false;
  }

  // MLK day on 3rd Monday of January
  if date == &nth_weekday(year, 1, 3, Monday as u8) {
    return false;
  }

  // Washington's Birthday on 3rd Monday of February
  if date == &nth_weekday(year, 2, 3, Monday as u8) {
    return false;
  }

  // Good Friday
  let mut easter = get_easter(year);
  easter -= Duration::days(2);
  if date == &easter {
    return false;
  }

  // Memorial Day
  let mut memorial_day = Date::try_from_ymd(year, 5, 31).unwrap();
  while memorial_day.weekday() != Monday {
    memorial_day -= Duration::days(1);
  }
  if date == &memorial_day {
    return false;
  }

  // Independence Day
  let mut independence_day = Date::try_from_ymd(year, 7, 4).unwrap();
  if independence_day.weekday() == Saturday {
    independence_day -= Duration::days(1);
  }
  else if independence_day.weekday() == Sunday {
    independence_day += Duration::days(1);
  }
  if date == &independence_day {
    return false;
  }

  // Labor Day on first Monday of September
  if date == &nth_weekday(year, 9, 1, Monday as u8) {
    return false;
  }

  // Thanksgiving on fourth Thursday of November
  if date == &nth_weekday(year, 11, 4, Thursday as u8) {
    return false;
  }

  // Christmas
  let mut christmas = Date::try_from_ymd(year, 12, 25).unwrap();
  if christmas.weekday() == Saturday {
    christmas -= Duration::days(1);
  }
  else if christmas.weekday() == Sunday {
    christmas += Duration::days(1);
  }
  if date == &christmas {
    return false;
  }

  if DISASTERS.contains(date) {
    return false;
  }

  return true;
}

#[cfg(test)]
mod tests {
  use time::{date,Weekday::{Monday,Thursday}};
  use crate::calendar::us_equity::{get_easter, is_market_open, nth_weekday};

  #[test]
  fn mlk() {
    assert_eq!(nth_weekday(2004, 1, 3, Monday as u8), date!(2004-01-19));
  }

  #[test]
  fn washington() {
    assert_eq!(nth_weekday(2004, 2, 3, Monday as u8), date!(2004-02-16));
  }

  #[test]
  fn easter() {
    assert_eq!(get_easter(2004), date!(2004-04-11));
  }

  #[test]
  fn good_friday() {
    assert_eq!(is_market_open(&date!(2004-04-09)), false);
  }

  #[test]
  fn labor() {
    assert_eq!(nth_weekday(2004, 9, 1, Monday as u8), date!(2004-9-6));
  }

  #[test]
  fn thanksgiving() {
    assert_eq!(nth_weekday(2004, 11, 4, Thursday as u8), date!(2004-11-25));
  }

  #[test]
  fn christmas() {
    assert_eq!(is_market_open(&date!(2004-12-25)), false);
  }
}
