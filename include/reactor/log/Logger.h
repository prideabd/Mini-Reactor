/**
 * @file Logger.h
 * @brief 日志记录器前端，定义日志级别、格式化逻辑以及提供给用户使用的宏接口。
 */
#pragma once
#include "LogStream.h"

namespace reactor::log {

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

} // namespace reactor::log

#define LOG_DEBUG reactor::log::Logger(__FILE__, __LINE__, reactor::log::Logger::DEBUG).Stream()
#define LOG_INFO  reactor::log::Logger(__FILE__, __LINE__, reactor::log::Logger::INFO).Stream()
#define LOG_WARN  reactor::log::Logger(__FILE__, __LINE__, reactor::log::Logger::WARN).Stream()
#define LOG_ERROR reactor::log::Logger(__FILE__, __LINE__, reactor::log::Logger::ERROR).Stream()
#define LOG_FATAL reactor::log::Logger(__FILE__, __LINE__, reactor::log::Logger::FATAL).Stream()