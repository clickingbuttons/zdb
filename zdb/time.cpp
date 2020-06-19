#include "time.h"
#include <time.h>
#include <ctime>
#include <sstream>
#include <iomanip>

string formatNanos(long long nanoseconds, const char* format)
{
	long long seconds = nanoseconds / nanos_to_seconds;
	long long nanosecondPart = nanoseconds % nanos_to_seconds;
	tm* timeinfo = gmtime(&seconds);

	char buffer[48];
	size_t strlen = strftime(buffer, sizeof(buffer), format, timeinfo);

	sprintf(buffer + strlen, ".%09lld", nanosecondPart);

	return buffer;
}

string formatNanos(long long nanoseconds)
{
	return formatNanos(nanoseconds, date_format);
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
