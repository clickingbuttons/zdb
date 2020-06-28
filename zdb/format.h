#pragma once

#include <string>

using namespace std;

constexpr static long long nanos_to_seconds = (long long) 1e9;
constexpr const char* date_format = "%Y-%m-%d %H:%M:%S";
constexpr const char* nano_format = "09";

string trim(string& s);
string rtrimZeros(string& s);

string formatNanos(long long nanoseconds, string format);
string formatNanos(long long nanoseconds);
long long parseNanos(string datetime, string format);
long long parseNanos(string datetime);
