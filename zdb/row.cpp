#include "row.h"
#include "format.h"
#include "log.h"
#include <fmt/ostream.h>
#include <sstream>
#include <string.h>
#include <variant>

Row::Row(Timestamp timestamp)
{
  columns.push_back(RowValue(timestamp));
}

#pragma warning(push)
#pragma warning(disable: 4244)
Row::Row(VariantRow variantRow, Schema const& schema)
{
  for (size_t i = 0; i < variantRow.columns.size(); i++)
  {
    RowValue val;
    constexpr size_t maxStrLength = sizeof(val.sym) - 1;
    switch (schema.columns[i].type)
    {
    // 8 byte types
    case ColumnType::INT64:
    case ColumnType::TIMESTAMP:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.i64 = var;
        }, variantRow.columns[i]);
      break;
    case ColumnType::UINT64:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.ui64 = var;
        }, variantRow.columns[i]);
      break;
    case ColumnType::FLOAT64:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.f64 = var;
        }, variantRow.columns[i]);
      break;
    // 4 byte types
    case ColumnType::INT32:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.i32 = var;
        }, variantRow.columns[i]);
      break;
    case ColumnType::UINT32:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.ui32 = var;
        }, variantRow.columns[i]);
      break;
    case ColumnType::CURRENCY:
    case ColumnType::FLOAT32:
      visit([&](auto const& var) {
        using type = decay_t<decltype(var)>;
        if constexpr (!is_same<type, Symbol>::value)
          val.f32 = var;
        }, variantRow.columns[i]);
      break;
    case ColumnType::SYMBOL:
    {
      Symbol sym = get<Symbol>(variantRow.columns[i]);
      if (strlen(sym) > maxStrLength)
      {
        SymbolTooLongException ex(sym);
        zlog::error(ex.what());
        throw ex;
      }
      strcpy(val.sym, sym);
      break;
    }
    default:
      throw runtime_error("Unable to convert to " + schema.getColumnTypeName(schema.columns[i].type));
      break;
    }
    columns.push_back(val);
  }
}
#pragma warning(pop)

void Row::put(RowValue const &value)
{
  columns.push_back(value);
}

bool Row::operator<(const Row& other) const
{
  return columns[0].ts < other.columns[0].ts;
}

string formatCurrency(float64 dollars, size_t sigFigs = 7)
{
  string res(sigFigs + 4, '0'); // Fill with 0s
  if (dollars >= pow(10, sigFigs))
  {
    res = fmt::format("{:1.{}e}", dollars, sigFigs - 4);
    // Replace e+0 with e
    size_t start_pos = res.find("e+0");
    if (start_pos != string::npos)
      res.replace(start_pos + 1, 2, "");
    else
    {
      // Replace e-0 with e-
      start_pos = res.find("e-0");
      if (start_pos != string::npos)
        res.replace(start_pos + 2, 1, "");
    }
  }
  else
  {
    int numDigits = 0;
    float64 tmpDollars = dollars;
    while (tmpDollars > 1)
    {
      tmpDollars /= 10;
      numDigits++;
    }
    res = fmt::format("{:<{}.{}f}", dollars, numDigits, sigFigs - numDigits);
    // Remove trailing zeros
    if (sigFigs - numDigits > 0)
      rtrimZeros(res);
  }

  return fmt::format("{:<{}}", res, sigFigs + 1);
}

constexpr double micros_to_cents = 1000000;

string Row::toString(Schema schema)
{
  ostringstream os;
  for (size_t i = 0; i < schema.columns.size(); i++)
  {
    switch (schema.columns[i].type) {
    case ColumnType::TIMESTAMP:
      fmt::print(os, formatNanos(columns[i].ts));
      break;
    case ColumnType::CURRENCY:
    {
      int64 microCents = columns[i].i64;
      float64 dollars = microCents / micros_to_cents;
      fmt::print(os, formatCurrency(dollars));
      break;
    }
    case ColumnType::SYMBOL:
      fmt::print(os, "{:>7}", columns[i].sym);
      break;
    case ColumnType::UINT32:
      fmt::print(os, formatCurrency(columns[i].ui32));
      break;
    case ColumnType::UINT64:
      fmt::print(os, formatCurrency(columns[i].ui64));
      break;
    default:
      fmt::print(os, "?");
      break;
    }
    if (i != schema.columns.size() - 1)
      fmt::print(os, " ");
  }

  return os.str();
}
