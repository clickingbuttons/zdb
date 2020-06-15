#pragma once

#include <string>
#include <vector>
#include <map>

using namespace std;

enum class ColumnType
{
	TIMESTAMP,
	INT,
	LONG,
	DOUBLE,
	SYMBOL,
	STRING
};

struct Column
{
	string name;
	ColumnType type;
};


class Schema {
public:
	Schema(string name);
	void addColumn(Column c);
	void addColumn(string name, ColumnType type);
	vector<Column> getColumns();
	string getName();
	static string getColumnTypeName(ColumnType c);
	// Copy assignment operator.
	Schema& operator=(const Schema& other);
private:
	string name;
	vector<Column> columns;
	static map<ColumnType, string> columnTypeStrings;
};
