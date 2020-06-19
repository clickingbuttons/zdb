#pragma once

#include "row.h"
#include "schema.h"
#include "config.h"
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <fstream>

using namespace std;
using namespace filesystem;

enum class PartitionBy
{
	DAY,
	WEEK,
	MONTH,
	YEAR
};

class Table {
public:
	Table(const Schema &schema, const Config &globalConfig);
	vector<Row> read(size_t fromRow, size_t toRow);
	vector<Row> read();
	void write(Row row);
	void write(vector<Row> rows);
	void flush();
private:
	// Directory this is stored on disk
	path dir;
	Schema schema;
	// Metadata saved to _meta
	Config meta;
	// Symbol table saved to _symbols. Stored twice in RAM since there is no array-backed map
	path symbolPath;
	void readSymbolFile();
	unordered_map<string, unsigned int> symbolSet;
	vector<string> symbols;
	// Helper to get path for column based on its type
	path getColumnFile(Column column);
	// Cache column files to avoid open/close on every read/write
	vector<path> columnPaths;
	// Used to hold `write`s until `flush`
	vector<Row> rowBuffer;
	// Used to hold row count until `flush`
	size_t rowCount;
};
