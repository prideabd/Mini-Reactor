#include <cstdio>
#include <sys/time.h>
#include <time.h>
#include <cstdlib>

#include "reactor/log/Logger.h"

namespace reactor::log {

// 默认输出函数：向标准输出打印
void DefaultOutput(const char* msg, int len) {
    fwrite(msg, 1, len, stdout);
}

// 默认刷新函数
void DefaultFlush() {
    fflush(stdout);
}

// 定义全局回调指针，初始化为默认的控制台输出
Logger::OutputFunc g_output = DefaultOutput;
Logger::FlushFunc g_flush = DefaultFlush;

void Logger::SetOutput(Logger::OutputFunc out) {
    if (out == nullptr) {
        g_output = DefaultOutput;
    } else {
        g_output = out;
    }
}

void Logger::SetFlush(Logger::FlushFunc flush) {
    if (flush == nullptr) {
        g_flush = DefaultFlush;
    } else {
        g_flush = flush;
    }
}

const char* LevelNames[] = {
    "TRACE ",
    "DEBUG ",
    "INFO  ",
    "WARN  ",
    "ERROR ",
    "FATAL ",
};

Logger::Impl::Impl(LogLevel level, const char* filename, int line)
    : stream_(),
      level_(level),
      line_(line),
      filename_(filename) {
    FormatTime();           // 1. 写入时间戳
    stream_ << LevelNames[level]; // 2. 写入日志级别
}

void Logger::Impl::FormatTime() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    time_t time = tv.tv_sec;
    struct tm tm_time;
    localtime_r(&time, &tm_time);
    char buf[64] = {0};
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_time);
    stream_ << buf << "." << static_cast<int>(tv.tv_usec) << " ";
}

Logger::Logger(const char* filename, int line, LogLevel level)
    : impl_(level, filename, line) {
}

Logger::~Logger() {
    impl_.stream_ << " -- " << impl_.filename_ << ":" << impl_.line_ << "\n";
    const LogStream::Buffer& buf(Stream().GetBuffer());
    g_output(buf.Data(), buf.Length()); // 析构时真正执行输出回调
    
    if (impl_.level_ == FATAL) {
        g_flush();
        abort(); // FATAL 级别直接终止程序
    }
}

} // namespace reactor::log