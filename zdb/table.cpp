#include "table.h"
#include <iostream>
#include <fstream>
#include <variant>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <iomanip>
#include <sstream>

string serializeTimePoint(const chrono::system_clock::time_point& time, const std::string& format)
{
	time_t tt = chrono::system_clock::to_time_t(time);
	tm tm = *gmtime(&tt); //GMT (UTC)
	//std::tm tm = *std::localtime(&tt); //Locale time-zone, usually UTC by default.
	stringstream ss;
	ss << put_time(&tm, format.c_str());
	return ss.str();
}

//chrono::milliseconds millisec(1073077200000);
//chrono::time_point<chrono::system_clock> input(millisec);
//cout << serializeTimePoint(input, "UTC: %Y-%m-%d %H:%M:%S") << endl;

path getDir(const Schema& s, const Config& globalConfig)
{
	string dbPath = globalConfig.getOption("filesystem", "path", current_path().string());
	path dir = path(dbPath).append("data").append(s.getName());
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
	for (Column c : schema.getColumns())
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
	size_t symNum = 0;
	vector<Column> columns = schema.getColumns();
	vector<ofstream> columnStreams;
	for (int i = 0; i < columns.size(); i++)
	{
		columnStreams.emplace_back(ofstream(columnPaths[i], ios::binary | ios::in | ios::ate));
	}

	// Sort rowBuffer by timestamp
	sort(rowBuffer.begin(), rowBuffer.end());

	for (int i = 0; i < columns.size(); i++)
	{
		ColumnType type = columns[i].type;

		for (Row row : rowBuffer)
		{
			RowValue val = row.columns[i];
			if (type == ColumnType::SYMBOL)
			{
				try
				{
					string sym = get<string>(val);
					if (symbolSet.find(sym) == symbolSet.end())
					{
						symbolSet[sym] = symNum++;
						symbols.push_back(sym);
					}
					columnStreams[i].write(reinterpret_cast<char*>(&symbolSet[sym]), sizeof(size_t));
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
			}
			else
			{
				visit([&](auto&& arg) {
					columnStreams[i].write(reinterpret_cast<char*>(&arg), sizeof(arg));
				}, val);
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

vector<Row> Table::read(int fromRow, int toRow)
{
	vector<Row> rowBuffer;
	vector<Column> columns = schema.getColumns();
	vector<ifstream> columnStreams;
	for (int i = 0; i < columns.size(); i++)
	{
		columnStreams.emplace_back(ifstream(columnPaths[i], ios::binary));
		//columnStream.seekg(fromRow);
	}
	char buffer[sizeof(long long)];
	for (int i = fromRow; i < toRow; i++)
	{
		long long ts;
		columnStreams[0].read(reinterpret_cast<char*>(&ts), sizeof(long long));
		Row r = Row(ts);
		size_t symNum;
		for (int j = 1; j < columns.size(); j++)
		{
			switch (columns[j].type) {
			case ColumnType::TIMESTAMP:
			case ColumnType::LONG:
				columnStreams[j].read(buffer, sizeof(long long));
				r.put(*reinterpret_cast<long long*>(buffer));
				break;
			case ColumnType::DOUBLE:
				columnStreams[j].read(buffer, sizeof(double));
				r.put(*reinterpret_cast<double*>(buffer));
				break;
			case ColumnType::SYMBOL:
				columnStreams[j].read(buffer, sizeof(size_t));
				symNum = *reinterpret_cast<size_t*>(buffer);
				r.put(symbols[symNum]);
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
