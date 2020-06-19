#pragma once

#include <string>

using namespace std;

constexpr long long nanos_to_seconds = (long long) 1e9;
constexpr const char* date_format = "%Y-%m-%d %H:%M:%S";

string formatNanos(long long nanoseconds, const char* format);
string formatNanos(long long nanoseconds);
long long parseNanos(string datetime, const char* format);
long long parseNanos(string datetime);
