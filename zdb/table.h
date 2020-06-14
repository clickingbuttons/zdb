#pragma once

#include "row.h"
#include "config.h"
#include <filesystem>
#include <vector>

using namespace std;
using namespace filesystem;

class Table {
public:
	Table(unique_ptr<Schema> schema, const Config &config);
	vector<Row> read(int fromRow, int toRow);
	void write(Row row);
	void flush();
private:
	unique_ptr<Schema> schema;
	path dir;
	path getColumnFile(Column column);
	vector<Row> rows;
	unique_ptr<Config> columnConfig;
};
