#include "table.h"
#include <iostream>
#include <fstream>
#include <variant>
#include <algorithm>
#include <cctype>

Table::Table(unique_ptr<Schema> s, const Config &c)
{
	// This is our schema now
	schema = move(s);

	string dbPath = c.getOption("filesystem", "path", current_path().string());

	dir = path(dbPath).append("data").append(schema->getName());
	create_directories(dir);

	path metaFile = path(dir).append("_meta");
	columnConfig = make_unique<Config>(metaFile);
}

void Table::write(Row row)
{
	rows.push_back(row);
}

path Table::getColumnFile(Column column)
{
	string columnType = schema->getColumnTypeName(column.type);
	string columnExt = string(columnType);
	transform(columnExt.begin(), columnExt.end(), columnExt.begin(),
		[](unsigned char c) { return std::tolower(c); });

	return path(dir).append(column.name + '.' + columnExt);
}

void Table::flush()
{
	// Write config
	for (Column c : schema->getColumns()) {
		columnConfig->setOption("columns", c.name, schema->getColumnTypeName(c.type));
	}
	columnConfig->setOption("rows", "count", to_string(rows.size()));
	columnConfig->write();

	// Write columnar data
	vector<Column> columns = schema->getColumns();
	for (int i = 0; i < columns.size(); i++)
	{
		Column column = columns.at(i);
		ofstream columnFile(getColumnFile(column), ios::binary);

		for (Row row : rows)
		{
			pair<ColumnType, RowValue> val = row.getValues().at(i);
			if (column.type != val.first)
			{
				cerr << "Column type " << schema->getColumnTypeName(column.type)
					<< " specified in schema but received type "
					<< schema->getColumnTypeName(val.first) << " in row." << endl;
				continue;
			}
			switch (val.first) {
			case ColumnType::TIMESTAMP:
			case ColumnType::LONG:
				columnFile.write(reinterpret_cast<char*>(&get<long long>(val.second)), sizeof(long long));
				break;
			case ColumnType::DOUBLE:
				columnFile.write(reinterpret_cast<char*>(&get<double>(val.second)), sizeof(double));
				break;
			case ColumnType::SYMBOL:
				// TODO: Symbol table
				//string symbol = get<string>(val.second);
				//// Save a byte
				//char byteArray[3];

				//// convert from an unsigned long int to a 4-byte array
				//byteArray[0] = (int)((symbol & 0xFF000000) >> 24);
				//byteArray[1] = (int)((symbol & 0x00FF0000) >> 16);
				//byteArray[2] = (int)((symbol & 0x0000FF00) >> 8);
				//columnFile.write(byteArray, 3);
				break;
			default:
				break;
			}
		}

		columnFile.close();
	}
}

vector<Row> Table::read(int fromRow, int toRow)
{
	vector<Row> rows;
	vector<Column> columns = schema->getColumns();
	vector<ifstream> columnFiles;
	for (Column c : columns)
	{
		//columnStream.seekg(fromRow);
		columnFiles.push_back(ifstream(getColumnFile(c), ios::binary));
	}
	char buffer[8];
	for (int i = fromRow; i < toRow; i++)
	{
		long long ts;
		columnFiles[0].read(reinterpret_cast<char*>(&ts), sizeof(long long));
		Row r = Row(ts);
		for (int j = 1; j < columns.size(); j++)
		{
			switch (columns[j].type) {
			case ColumnType::TIMESTAMP:
			case ColumnType::LONG:
				columnFiles[j].read(buffer, sizeof(long long));
				r.putLong(*reinterpret_cast<long long*>(buffer));
				break;
			case ColumnType::DOUBLE:
				columnFiles[j].read(buffer, sizeof(double));
				r.putDouble(*reinterpret_cast<double*>(buffer));
				break;
			case ColumnType::SYMBOL:
				// TODO: Symbol table
				break;
			default:
				break;
			}
		}
		rows.push_back(r);
	}


	return rows;
}
