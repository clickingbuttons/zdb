#pragma once
#include "config.h"
#include <iostream>
#include <fstream>
#include <fmt/core.h>
#include <fmt/ostream.h>

enum class LogLevel
{
  DEBUG,
  INFO,
  WARN,
  ERROR, // User request fails
  CRITICAL // App closes
};

// Handles opening/closing log files
// Thread safe since C++11
// https://stackoverflow.com/questions/1008019/c-singleton-design-pattern
class Logger
{
public:
  static Logger& getInstance()
  {
    static Logger instance;
    return instance;
  }
  // Don't copy by accident
  Logger(Logger const&) = delete;
  void operator=(Logger const&) = delete;
  
  template <typename S, typename... Args>
  inline void log(LogLevel l, const S& format_str, Args&&... args)
  {
    // Always log to file
    fmt::print(logStream, format_str, args...);
    // Log to cerr if above warning
    if (l >= LogLevel::ERROR)
      fmt::print(cerr, format_str, args...);
    else
      fmt::print(cout, format_str, args...);
  }
private:
  ofstream logStream;
  Logger()
    : logStream("zdb.log")
  {
    // We don't use C-style output from <stdio>
    ios::sync_with_stdio(false);
    // Use specified encoding
    locale::global(locale(Config::getGlobal().getOption("locale", "default", "en_US.UTF-8")));
  }
};

namespace zlog
{
  template <typename S, typename... Args>
  inline void debug(const S& format_str, Args&&... args)
  {
    Logger::getInstance().log(LogLevel::DEBUG, format_str, args...);
  }
  template <typename S, typename... Args>
  inline void info(const S& format_str, Args&&... args)
  {
    Logger::getInstance().log(LogLevel::INFO, format_str, args...);
  }
  template <typename S, typename... Args>
  inline void warn(const S& format_str, Args&&... args)
  {
    Logger::getInstance().log(LogLevel::WARN, format_str, args...);
  }
  template <typename S, typename... Args>
  inline void error(const S& format_str, Args&&... args)
  {
    Logger::getInstance().log(LogLevel::ERROR, format_str, args...);
  }
  template <typename S, typename... Args>
  inline void critical(const S& format_str, Args&&... args)
  {
    Logger::getInstance().log(LogLevel::CRITICAL, format_str, args...);
  }
}  // namespace log
