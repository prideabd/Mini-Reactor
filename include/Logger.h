#pragma once
#include "LogStream.h"

class Logger {
public:
    enum LogLevel {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    };

    Logger(const char* filename, int line, LogLevel level);
    ~Logger();

    LogStream& stream() { return impl_.stream_; }

private:
    class Impl {
    public:
        Impl(LogLevel level, const char* filename, int line);
        void FormatTime();

        LogStream stream_;
        LogLevel level_;
        int line_;
        std::string filename_; // 这里采用 string 类型
    };
    Impl impl_;
};

#define LOG_INFO Logger(__FILE__, __LINE__, Logger::INFO).stream()
#define LOG_WARN Logger(__FILE__, __LINE__, Logger::WARN).stream()
#define LOG_ERROR Logger(__FILE__, __LINE__, Logger::ERROR).stream()