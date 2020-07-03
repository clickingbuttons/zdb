#include "table.h"
#include <algorithm>
#include <fmt/core.h>

void Table::write(Row row)
{
  rowBuffer.push_back(row);
}

void Table::write(vector<Row> rows)
{
  rowBuffer.insert(rowBuffer.end(), rows.begin(), rows.end());
}

void Table::write(VariantRow variantRow)
{
  write(Row(variantRow, schema));
}

void Table::write(vector<VariantRow> rows)
{
  for (VariantRow r : rows)
    write(r);
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
    symbolStream << sym << '\n';

  // Update row count
  rowCount += rowBuffer.size();
  meta.setOption("rows", "count", to_string(rowCount));
  meta.write();
}
