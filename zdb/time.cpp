#include "time.h"
#include <time.h>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <fmt/core.h>
#include <fmt/chrono.h>

string formatNanos(long long nanoseconds)
{
	long long seconds = nanoseconds / nanos_to_seconds;
	long long nanosecondPart = nanoseconds % nanos_to_seconds;
	tm timeinfo;
	gmtime_s(&timeinfo , &seconds);

	return fmt::format("{:%Y-%m-%d %H:%M:%S}.{:09}", timeinfo, nanosecondPart);
}

long long parseNanos(string datetime, const char* format)
{
	tm timeinfo;
	istringstream ss(datetime);
	ss >> get_time(&timeinfo, format);
	if (ss.fail()) {
		throw;
	}

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
