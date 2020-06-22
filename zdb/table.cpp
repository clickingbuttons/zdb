#include "table.h"
#include <fstream>
#include <variant>
#include <type_traits>
#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <sstream>

path getDir(const Config& globalConfig, const string& tableName)
{
	string dbPath = globalConfig.getOption("filesystem", "path", current_path().string());
	path dir = path(dbPath).append("data").append(tableName);
	create_directories(dir);

	return dir;
}

void Table::init(const Config& globalConfig, string const& tableName)
{
	dir = getDir(globalConfig, tableName);
	meta = Config(path(dir).append("_meta"));
	symbolPath = path(dir).append("_symbols");
	readSymbolFile();
	rowCount = stoi(meta.getOption("rows", "count", "0"));
}

// Long member initializer to avoid using pointers in class members
Table::Table(const Config &globalConfig, const Schema& s)
{
	init(globalConfig, s.name);
	schema = s;

	ostringstream columnOrder;
	for (int i = 0 ; i < schema.columns.size(); i++)
	{
		Column c = schema.columns[i];
		meta.setOption("columns", c.name, schema.getColumnTypeName(c.type));
		columnPaths.emplace_back(getColumnFile(c));
		columnOrder << c.name;
		if (i != schema.columns.size() - 1)
			columnOrder << ",";
	}
	
	meta.setOption("columnOrder", "order", columnOrder.str());
}

Table::Table(const Config& globalConfig, const string& tableName)
{
	init(globalConfig, tableName);
	schema = Schema(tableName);

	// TODO: error handling if no _meta file
	stringstream order(meta.getOption("columnOrder", "order"));
	string columnName;

	while (getline(order, columnName, ','))
	{

		string columnType = meta.getOption("columns", columnName);
		Column c{
			columnName,
			schema.getColumnType(columnType)
		};
		// Skip first column which is always "ts"
		if (columnName != "ts")
		{
			schema.addColumn(c);
		}

		columnPaths.emplace_back(getColumnFile(c));
	}
}

void Table::write(Row row)
{
	rowBuffer.push_back(row);
}

void Table::write(vector<Row> rows)
{
	for (Row r : rows)
	{
		write(r);
	}
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
	uint32 symNum = (uint32)symbols.size();
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
			RowValue* val = &row.columns[i];
			switch (columns[i].type) {
			// 8 byte types
			case ColumnType::INT64:
			case ColumnType::TIMESTAMP:
			case ColumnType::UINT64:
			case ColumnType::FLOAT64:
				static_assert(
					sizeof(int64) == 8 &&
					sizeof(uint64) == 8 &&
					sizeof(Timestamp) == 8 &&
					sizeof(float64) == 8,
					"Sizes on this platform are not all 64 bit");
				columnStreams[i].write(val->sym, 8);
				break;
			// 4 byte types
			case ColumnType::INT32:
			case ColumnType::UINT32:
			case ColumnType::FLOAT32:
			case ColumnType::CURRENCY:
				static_assert(
					sizeof(int32) == 4 &&
					sizeof(uint32) == 4 &&
					sizeof(Currency) == 4 &&
					sizeof(float32) == 4,
					"Sizes on this platform are not all 32 bit");
				columnStreams[i].write(val->sym, 4);
				break;
			case ColumnType::SYMBOL:
			{
				string sym(val->sym);
				if (symbolSet.find(sym) == symbolSet.end())
				{
					symbolSet[sym] = symNum++;
					symbols.push_back(sym);
				}
				columnStreams[i].write(reinterpret_cast<char*>(&symbolSet[sym]), 4);
				break;
			}
			default:
				throw runtime_error("Writing " + schema.getColumnTypeName(columns[i].type) + " is not yet supported");
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
	size_t numColumns = schema.columns.size();
	for (size_t i = 0; i < numColumns; i++)
	{
		columnStreams.emplace_back(ifstream(columnPaths[i], ios::binary));
		//columnStream.seekg(fromRow);
	}
	shared_ptr<Schema> sharedSchema = make_shared<Schema>(schema);
	for (size_t rowNum = fromRow; rowNum < toRow; rowNum++)
	{
		// Grab timestamp from first column
		long long ts;
		columnStreams[0].read(reinterpret_cast<char*>(&ts), sizeof(Timestamp));
		Row r = Row(ts, sharedSchema);
		for (size_t i = 1; i < numColumns; i++)
		{
			RowValue val;

			switch (schema.columns[i].type) {
			// 8 byte types
			case ColumnType::INT64:
			case ColumnType::UINT64:
			case ColumnType::TIMESTAMP:
			case ColumnType::FLOAT64:
				columnStreams[i].read(val.sym, sizeof(int64));
				break;
			// 4 byte types
			case ColumnType::INT32:
			case ColumnType::UINT32:
			case ColumnType::FLOAT32:
				columnStreams[i].read(val.sym, sizeof(int32));
				break;
			case ColumnType::SYMBOL:
			{
				columnStreams[i].read(val.sym, sizeof(uint32));
				string sym = symbols[val.i32];
				strcpy_s(val.sym, sizeof(val.sym), sym.c_str());
				break;
			}
			case ColumnType::CURRENCY:
			{
				// Stored on disk as float, read as int64 for accurate fixed-precision math
				columnStreams[i].read(val.sym, sizeof(float32));
				// Use last 6 digits as cents
				// 2^63 =  9,223,372,036,854,776,000
				//		= $9,223,372,036,854.776000
				float micros = val.f32 * 1000000.f;
				long long microCents = (long long)micros;
				val.pcur = microCents;
				break;
			}
			default:
				break;
			}

			r.put(val);
		}
		rowBuffer.push_back(r);
	}

	return rowBuffer;
}

vector<Row> Table::read()
{
	return read(0, rowCount);
}
