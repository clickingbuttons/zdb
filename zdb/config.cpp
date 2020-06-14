#include "config.h"
#include <fstream>
#include <algorithm> 
#include <cctype>
#include <locale>

// trim from start (in place)
static inline void ltrim(std::string& s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
		return !std::isspace(ch);
		}));
}

// trim from end (in place)
static inline void rtrim(std::string& s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
		return !std::isspace(ch);
		}).base(), s.end());
}

// trim from both ends (in place)
static inline std::string trim(std::string s) {
	ltrim(s);
	rtrim(s);
	return s;
}

Config::Config(filesystem::path path)
{
	this->path = path;
	read();
}

void Config::read()
{
	ifstream infile(path);

	string sectionName = "default";
	string line;
	while (getline(infile, line))
	{
		trim(line);
		if (line.empty() || line[0] == '#' || line[0] == ';') // comment
		{
			continue;
		}

		if (line[0] == '[')
		{
			// New section
			int closeBrace = line.find(']');
			line = line.substr(1, closeBrace == string::npos ? line.length() : closeBrace - 1);
			sectionName = trim(line);
		}
		else
		{
			int separatorIndex = line.find('=');
			// Invalid option: no value
			if (separatorIndex == string::npos)
			{
				continue;
			}
			string key = trim(line.substr(0, separatorIndex));
			string val = trim(line.substr(separatorIndex + 1, line.length()));
			// Adds section and key if doesn't exist
			sections[sectionName][key] = val;
		}
	}
}

void Config::write()
{
	ofstream outfile(path);

	for (auto const& [section, values] : sections)
	{
		outfile << '[' << section << ']' << endl;
		for (auto const& [key, val] : values)
		{
			outfile << trim(key) << '=' << trim(val) << std::endl;
		}
	}
}

string Config::getOption(string section, string option) const
{
	// Can throw
	return sections.at(section).at(option);
}

string Config::getOption(string section, string option, string default) const
{
	auto sect = sections.find(section);
	if (sect == sections.end())
	{
		return default;
	}
	auto val = sect->second.find(option);
	return val == sect->second.end() ? default : val->second;
}

void Config::setOption(string section, string key, string val)
{
	sections[section][key] = val;
}
