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

    // 函数指针：重点是指针，指向了一个函数
    using OutputFunc = void (*)(const char* msg, int len);
    using FlushFunc = void (*)();
    // 两个输出回调函数的输入为一个函数指针
    static void SetOutput(OutputFunc);
    static void SetFlush(FlushFunc);

    LogStream& Stream() { return impl_.stream_; }

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

#define LOG_DEBUG Logger(__FILE__, __LINE__, Logger::DEBUG).Stream()
#define LOG_INFO Logger(__FILE__, __LINE__, Logger::INFO).Stream()
#define LOG_WARN Logger(__FILE__, __LINE__, Logger::WARN).Stream()
#define LOG_ERROR Logger(__FILE__, __LINE__, Logger::ERROR).Stream()
#define LOG_FATAL Logger(__FILE__, __LINE__, Logger::FATAL).Stream()