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
  static Config& getGlobal();
  void read();
  void write();
  string getOption(string section, string option) const;
  string getOption(string section, string option, string ddefault) const;
  void setOption(string section, string key, string val);
private:
  path m_path;
  map<string, map<string, string>> sections;
};
