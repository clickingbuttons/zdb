#include "schema.h"
#include <stdexcept>
#include <regex>
#include <sstream>
#include <fmt/ostream.h>

Schema::Schema()
{
}

Schema::Schema(string name)
{
	this->name = name;
	// First column is always timestamp
	addColumn("ts", ColumnType::TIMESTAMP);
}

Schema::Schema(string name, vector<pair<string, ColumnType>> columns)
	: Schema(name)
{
	for (const pair<string, ColumnType> col : columns)
	{
		addColumn(col.first, col.second);
	}
}

void Schema::addColumn(Column c)
{
	this->columns.push_back(c);
}

void Schema::addColumn(string name, ColumnType type)
{
	addColumn(Column({ name, type }));
}

vector<pair<ColumnType, string>> Schema::columnTypes = {
	   {ColumnType::TIMESTAMP, "TIMESTAMP"},
	   {ColumnType::CURRENCY, "CURRENCY"},
	   {ColumnType::SYMBOL, "SYMBOL"},
	   {ColumnType::INT32, "INT32"},
	   {ColumnType::UINT32, "UINT32"},
	   {ColumnType::FLOAT64, "FLOAT64"},
};

string Schema::getColumnTypeName(ColumnType c)
{
	for (pair<ColumnType, string> type : columnTypes)
	{
		if (type.first == c)
		{
			return type.second;
		}
	}
	return "UNKNOWN";
}

ColumnType Schema::getColumnType(string c)
{
	for (pair<ColumnType, string> type : columnTypes)
	{
		if (type.second == c)
		{
			return type.first;
		}
	}

	throw runtime_error("Column type " + c + " is invalid");
}

Schema& Schema::operator=(const Schema& other)
{
	name = string(other.name);
	columns = other.columns;
	return *this;
}
