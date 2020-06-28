#include "config.h"
#include "format.h"
#include <fstream>
#include <algorithm> 
#include <cctype>
#include <fmt/core.h>
#include <fmt/ostream.h>

Config::Config()
{
}

Config::Config(filesystem::path path) : path(path)
{
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
      continue;

    if (line[0] == '[')
    {
      // New section
      size_t closeBrace = line.find(']');
      line = line.substr(1, closeBrace == string::npos ? line.length() : closeBrace - 1);
      sectionName = trim(line);
    }
    else
    {
      size_t separatorIndex = line.find('=');
      // Invalid option: no value
      if (separatorIndex == string::npos)
        continue;
      string key = trim(line.substr(0, separatorIndex));
      string val = trim(line.substr(separatorIndex + 1, line.length()));
      // Adds section and key if doesn't exist
      sections[sectionName][key] = val;
    }
  }
}

void Config::write()
{
  ofstream outfile(path, ofstream::trunc);

  for (auto const& [section, columns] : sections)
  {
    fmt::print(outfile, "[{}]\n", section);
    for (auto const& [key, val] : columns)
      fmt::print(outfile, "{}={}\n", key, val);
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
    return default;
  auto val = sect->second.find(option);
  return val == sect->second.end() ? default : val->second;
}

void Config::setOption(string section, string key, string val)
{
  sections[section][key] = val;
}
