#pragma once

#include <filesystem>
#include <map>
#include <string>

using namespace std;
using namespace filesystem;

class Config {
public:
  Config();
  Config(path path);
  void read();
  void write();
  string getOption(string section, string option) const;
  string getOption(string section, string option, string default) const;
  void setOption(string section, string key, string val);
private:
  path path;
  map<string, map<string, string>> sections;
};
