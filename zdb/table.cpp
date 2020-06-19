#include "table.h"
#include <iostream>
#include <fstream>
#include <variant>
#include <algorithm>
#include <cctype>

path getDir(const Schema& s, const Config& globalConfig)
{
	string dbPath = globalConfig.getOption("filesystem", "path", current_path().string());
	path dir = path(dbPath).append("data").append(s.name);
	create_directories(dir);

	return dir;
}

// Long member initializer to avoid using pointers in class members
Table::Table(const Schema &s, const Config &globalConfig)
	: dir(getDir(s, globalConfig)),
	schema(s),
	meta(Config(path(dir).append("_meta"))),
	symbolPath(path(dir).append("_symbols"))
{
	readSymbolFile();
	for (Column c : schema.columns)
	{
		meta.setOption("columns", c.name, schema.getColumnTypeName(c.type));
		columnPaths.emplace_back(getColumnFile(c));
	}
	rowCount = stoi(meta.getOption("rows", "count", "0"));
}

void Table::write(Row row)
{
	rowBuffer.push_back(row);
}

void Table::write(vector<Row> rows)
{
	rowBuffer.insert(rowBuffer.end(), rows.begin(), rows.end());
}

path Table::getColumnFile(Column column)
{
	string columnType = schema.getColumnTypeName(column.type);
	string columnExt = string(columnType);
	transform(columnExt.begin(), columnExt.end(), columnExt.begin(),
		[](unsigned char c) { return tolower(c); });

	path ret = path(dir).append(column.name + '.' + columnExt);
	return ret;
}

void Table::flush()
{
	// Write columnar data
	unsigned int symNum = symbols.size();
	vector<Column> columns = schema.columns;
	vector<fstream> columnStreams;
	for (int i = 0; i < columns.size(); i++)
	{
		// Create the file if it doesn't exist
		columnStreams.emplace_back(fstream(columnPaths[i], ios::out | ios::app));
		columnStreams[i].close();
		// Open the file in the mode we want
		columnStreams[i].open(columnPaths[i], ios::in | ios::out | ios::binary | ios::ate);
	}

	// Sort rowBuffer by timestamp
	sort(rowBuffer.begin(), rowBuffer.end());

	for (int i = 0; i < columns.size(); i++)
	{
		ColumnType type = columns[i].type;

		for (Row row : rowBuffer)
		{
			RowValue val = row.columns[i];
			switch (columns[i].type) {
			case ColumnType::SYMBOL:
				try
				{
					string sym = get<string>(val);
					if (symbolSet.find(sym) == symbolSet.end())
					{
						symbolSet[sym] = symNum++;
						symbols.push_back(sym);
					}
					columnStreams[i].write(reinterpret_cast<char*>(&symbolSet[sym]), sizeof(unsigned int));
				}
				catch (bad_variant_access)
				{
					string columnType = schema.getColumnTypeName(type);

					cerr << "Error writing: value \"";
					visit([](auto&& arg) {
						cerr << arg;
						}, val);
					cerr << "\" does not match type " << columnType << endl;
				}
				break;
			default:
				visit([&](auto&& arg) {
					columnStreams[i].write(reinterpret_cast<char*>(&arg), sizeof(arg));
				}, val);
				break;
			}
		}
	}

	ofstream symbolStream(symbolPath, ios::trunc);

	// Write symbol map
	for (string sym : symbols)
	{
		symbolStream << sym << '\n';
	}

	// Update row count
	rowCount += rowBuffer.size();
	meta.setOption("rows", "count", to_string(rowCount));
	meta.write();
}

void Table::readSymbolFile()
{
	ifstream symbolStream(symbolPath);

	string line;
	int lineNum = 0;
	while (getline(symbolStream, line))
	{
		if (line.size() && line[line.size() - 1] == '\r') {
			line = line.substr(0, line.size() - 1);
		}
		symbolSet[line] = lineNum++;
		symbols.push_back(line);
	}
}

vector<Row> Table::read(size_t fromRow, size_t toRow)
{
	vector<Row> rowBuffer;
	vector<ifstream> columnStreams;
	for (size_t i = 0; i < schema.columns.size(); i++)
	{
		columnStreams.emplace_back(ifstream(columnPaths[i], ios::binary));
		//columnStream.seekg(fromRow);
	}
	char buffer[sizeof(long long)];
	unsigned int symNum;
	for (size_t i = fromRow; i < toRow; i++)
	{
		long long ts;
		columnStreams[0].read(reinterpret_cast<char*>(&ts), sizeof(long long));
		Row r = Row(ts);
		for (int j = 1; j < schema.columns.size(); j++)
		{
			switch (schema.columns[j].type) {
			case ColumnType::TIMESTAMP: {
				columnStreams[j].read(buffer, sizeof(long long));
				r.put(*reinterpret_cast<long long*>(buffer));
				break;
			}
			case ColumnType::SYMBOL:
				columnStreams[j].read(buffer, sizeof(unsigned int));
				symNum = *reinterpret_cast<unsigned int*>(buffer);
				r.put(symbols[symNum]);
				break;
			case ColumnType::UINT32:
				columnStreams[j].read(buffer, sizeof(unsigned int));
				r.put(*reinterpret_cast<unsigned int*>(buffer));
				break;
			case ColumnType::CURRENCY:
				columnStreams[j].read(buffer, sizeof(float));
				r.put(*reinterpret_cast<float*>(buffer));
				break;
			default:
				break;
			}
		}
		rowBuffer.push_back(r);
	}

	return rowBuffer;
}

vector<Row> Table::read()
{
	return read(0, rowCount);
}
