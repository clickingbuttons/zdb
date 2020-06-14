#pragma once

#include <filesystem>
#include <map>
#include <string>

using namespace std;

class Config {
public:
	Config(filesystem::path path);
	void read();
	void write();
	string getOption(string section, string option) const;
	string getOption(string section, string option, string default) const;
	void setOption(string section, string key, string val);
private:
	filesystem::path path;
	map<string, map<string, string>> sections;
};
