#include "schema.h"

Schema::Schema(string name)
{
	this->name = name;
	// First column is always timestamp
	addColumn("ts", ColumnType::TIMESTAMP);
}

void Schema::addColumn(Column c)
{
	this->columns.push_back(c);
}

void Schema::addColumn(string name, ColumnType type)
{
	addColumn(Column({ name, type }));
}

vector<Column> Schema::getColumns()
{
	return columns;
}

string Schema::getName()
{
	return name;
}

map<ColumnType, string> Schema::columnTypeStrings = {
	   {ColumnType::TIMESTAMP, "TIMESTAMP"},
	   {ColumnType::INT, "INT"},
	   {ColumnType::LONG, "LONG"},
	   {ColumnType::DOUBLE, "DOUBLE"},
	   {ColumnType::SYMBOL, "SYMBOL"},
	   {ColumnType::STRING, "STRING"},
};

string Schema::getColumnTypeName(ColumnType c)
{
	return columnTypeStrings.at(c);
}

Schema& Schema::operator=(const Schema& other)
{
	name = string(other.name);
	columns = other.columns;
	return *this;
}
