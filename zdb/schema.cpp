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

map<ColumnType, string> Schema::columnTypeStrings = {
	   {ColumnType::TIMESTAMP, "TIMESTAMP"},
	   {ColumnType::CURRENCY, "CURRENCY"},
	   {ColumnType::SYMBOL, "SYMBOL"},
	   {ColumnType::UINT32, "UINT32"},
};

string Schema::getColumnTypeName(ColumnType c)
{
	auto type = columnTypeStrings.find(c);
	if (type == columnTypeStrings.end())
	{
		return "UNKNOWN";
	}
	return columnTypeStrings.at(c);
}

Schema& Schema::operator=(const Schema& other)
{
	name = string(other.name);
	columns = other.columns;
	return *this;
}
