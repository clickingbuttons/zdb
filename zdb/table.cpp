#include "table.h"
#include <iostream>
#include <fstream>
#include <variant>
#include <algorithm>
#include <cctype>

Table::Table(const Schema &s, const Config &globalConfig)
	: schema(s)
{
	string dbPath = globalConfig.getOption("filesystem", "path", current_path().string());
	dir = path(dbPath).append("data").append(schema.getName());
	create_directories(dir);
	path metaFile = path(dir).append("_meta");
	meta = make_unique<Config>(metaFile);

	size_t numColumns = schema.getColumns().size();
	for (size_t i = 0; i < numColumns; i++)
	{
		Column c = schema.getColumns()[i];
		meta->setOption("columns", c.name, schema.getColumnTypeName(c.type));
	}
	symbolFile = path(dir).append("_symbols");
	readSymbolTable();
}

void Table::write(Row row)
{
	rows.push_back(row);
}

void Table::write(vector<Row> otherRows)
{
	rows.insert(rows.end(), otherRows.begin(), otherRows.end());
}

path Table::getColumnFile(Column column)
{
	string columnType = schema.getColumnTypeName(column.type);
	string columnExt = string(columnType);
	transform(columnExt.begin(), columnExt.end(), columnExt.begin(),
		[](unsigned char c) { return std::tolower(c); });

	return path(dir).append(column.name + '.' + columnExt);
}

void Table::flush()
{
	// Write config
	meta->setOption("rows", "count", to_string(rows.size()));
	meta->write();

	// Write columnar data
	size_t symNum = 0;
	vector<Column> columns = schema.getColumns();
	for (int i = 0; i < columns.size(); i++)
	{
		Column column = columns[i];
		ofstream columnFile(getColumnFile(column), ios::binary);

		for (Row row : rows)
		{
			RowValue val = row.columns[i];
			if (column.type == ColumnType::SYMBOL)
			{
				try {
					string sym = get<string>(val);
					if (symbolSet.find(sym) == symbolSet.end())
					{
						symbolSet[sym] = symNum++;
						symbols.push_back(sym);
					}
					columnFile.write(reinterpret_cast<char*>(&symbolSet[sym]), sizeof(size_t));
				}
				catch (bad_variant_access)
				{
					string columnType = schema.getColumnTypeName(column.type);

					cerr << "Error writing: value \"";
					visit([](auto&& arg) {
						cerr << arg;
					}, val);
					cerr << "\" does not match type " << columnType << endl;
				}
			}
			else
			{
				visit([&](auto&& arg) {
					columnFile.write(reinterpret_cast<char*>(&arg), sizeof(arg));
				}, val);
			}
		}

		columnFile.close();
	}

	// Write symbol map
	ofstream symStream(symbolFile);

	int i = 0;
	for (string sym : symbols)
	{
		symStream << sym << endl;
	}
}

void Table::readSymbolTable()
{
	// Clear symbols if already existing
	symbols.clear();
	// Read symbol map
	ifstream symStream(symbolFile, ios::binary);

	string line;
	int lineNum = 0;
	while (getline(symStream, line))
	{
		if (line.size() && line[line.size() - 1] == '\r') {
			line = line.substr(0, line.size() - 1);
		}
		symbolSet[line] = lineNum++;
		symbols.push_back(line);
	}
}

vector<Row> Table::read(int fromRow, int toRow)
{
	vector<Row> rows;
	vector<Column> columns = schema.getColumns();
	vector<ifstream> columnFiles;
	for (Column c : columns)
	{
		//columnStream.seekg(fromRow);
		columnFiles.push_back(ifstream(getColumnFile(c), ios::binary));
	}
	char buffer[sizeof(long long)];
	for (int i = fromRow; i < toRow; i++)
	{
		long long ts;
		columnFiles[0].read(reinterpret_cast<char*>(&ts), sizeof(long long));
		Row r = Row(ts);
		size_t symNum;
		for (int j = 1; j < columns.size(); j++)
		{
			switch (columns[j].type) {
			case ColumnType::TIMESTAMP:
			case ColumnType::LONG:
				columnFiles[j].read(buffer, sizeof(long long));
				r.put(*reinterpret_cast<long long*>(buffer));
				break;
			case ColumnType::DOUBLE:
				columnFiles[j].read(buffer, sizeof(double));
				r.put(*reinterpret_cast<double*>(buffer));
				break;
			case ColumnType::SYMBOL:
				columnFiles[j].read(buffer, sizeof(size_t));
				symNum = *reinterpret_cast<size_t*>(buffer);
				r.put(symbols[symNum]);
				break;
			default:
				break;
			}
		}
		rows.push_back(r);
	}


	return rows;
}

vector<Row> Table::read()
{
	return read(0, stoi(meta->getOption("rows", "count")));
}
