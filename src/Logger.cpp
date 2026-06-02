// 简易实现
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include "Logger.h"

Logger::Impl::Impl(LogLevel level, const char* filename, int line)
    : level_(level),
      line_(line),
      filename_(filename)
{
    FormatTime();
}
void Logger::Impl::FormatTime() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    time_t time = tv.tv_sec;
    struct tm tm_time;
    localtime_r(&time, &tm_time);
    char str_time[64] = {0};
    snprintf(str_time, sizeof(str_time), "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] ",
             tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
             tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec, tv.tv_usec);
    stream_ << str_time;
}

Logger::Logger(const char* filename, int line, LogLevel level) : impl_(level, filename, line)
{
}
Logger::~Logger() {
    impl_.stream_ << " -- " << impl_.filename_ << ":" << impl_.line_ << "\n";
    const LogStream::Buffer& buf(impl_.stream_.buffer());
    std::cout.write(buf.data(), buf.length());
}