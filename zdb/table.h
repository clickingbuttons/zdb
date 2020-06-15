#pragma once

#include "row.h"
#include "schema.h"
#include "config.h"
#include <filesystem>
#include <vector>
#include <unordered_map>

using namespace std;
using namespace filesystem;

class Table {
public:
	Table(const Schema &schema, const Config &globalConfig);
	vector<Row> read(int fromRow, int toRow);
	vector<Row> read();
	void write(Row row);
	void write(vector<Row> rows);
	void flush();
private:
	void readSymbolTable();
	Schema schema;
	unique_ptr<Config> meta;
	path dir;
	path getColumnFile(Column column);
	path symbolFile;
	vector<Row> rows;
	unordered_map<string, size_t> symbolSet;
	vector<string> symbols;
};
