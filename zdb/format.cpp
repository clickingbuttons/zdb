#include "format.h"
#include <time.h>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <fmt/core.h>
#include <fmt/chrono.h>

constexpr auto isSpace = [](int ch) { return !isspace(ch); };
constexpr auto isZero = [](int ch) { return ch != '0'; };
string full_format = "{:" + string(date_format) + "}.{:" + nano_format + "}";

void ltrim(string& s)
{
  s.erase(s.begin(), find_if(s.begin(), s.end(), isSpace));
}

void rtrim(string& s)
{
  s.erase(find_if(s.rbegin(), s.rend(), isSpace).base(), s.end());
}

void trim(string &s)
{
  ltrim(s);
  rtrim(s);
}

void rtrimZeros(string &s)
{
  auto start = find_if(s.rbegin(), s.rend(), isZero).base();
  auto end = s.end();
  // Check for decimal
  if (*(start - 1) == '.')
    start -= 1;
  s.replace(start, end, end - start, ' ');
}

string formatNanos(long long nanoseconds, string format)
{
  long long nanosecondPart = nanoseconds % nanos_to_seconds;
  time_t timeinfo = nanoseconds / nanos_to_seconds;
  gmtime(&timeinfo);

  string res = fmt::format(format, timeinfo, nanosecondPart);
  rtrimZeros(res);
  return res;
}

string formatNanos(long long nanoseconds)
{
  return formatNanos(nanoseconds, full_format);
}

long long parseNanos(string datetime, string format)
{
  tm timeinfo;
  istringstream ss(datetime);
  ss >> get_time(&timeinfo, format.c_str());
  if (ss.fail())
    throw;

  time_t seconds = mktime(&timeinfo);
  ss.ignore(1); // ignore decimal
  string nanoseconds;
  ss >> nanoseconds;

  return seconds * nanos_to_seconds + stoi(nanoseconds);
}

long long parseNanos(string datetime)
{
  return parseNanos(datetime, date_format);
}
