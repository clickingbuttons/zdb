#include "table.h"

void Table::readSymbolFile()
{
  ifstream symbolStream(symbolPath);

  string line;
  int lineNum = 0;
  while (getline(symbolStream, line))
  {
    if (line.size() && line[line.size() - 1] == '\r')
      line = line.substr(0, line.size() - 1);
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
  for (size_t rowNum = fromRow; rowNum < toRow; rowNum++)
  {
    // Grab timestamp from first column
    long long ts;
    columnStreams[0].read(reinterpret_cast<char*>(&ts), sizeof(Timestamp));
    Row r(ts);
    for (size_t i = 1; i < numColumns; i++)
    {
      RowValue val;

      switch (schema.columns[i].type)
      {
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
        //    = $9,223,372,036,854.776000
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
