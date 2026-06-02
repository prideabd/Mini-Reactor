#include <algorithm>
#include "LogStream.h"

const char digits[] = "9876543210123456789";
const char* zero = digits + 9;

// 无 if/else 整型转字符（包括负数）
template <typename T>
void LogStream::FormatInteger(T v) {
    if (buffer_.avail() >= 32) {
        char* buf = buffer_.current();
        char* p = buf;
        T i = v;
        do {
            int lsd = static_cast<int>(i % 10);
            i /= 10;
            *p++ = zero[lsd]; // 如果为负数，lsd就是一个负数，正好对应上面的 digits
        } while (i != 0);
        if (v < 0) {
            *p++ = '-';
        }
        *p = '\0';
        std::reverse(buf, p);
        buffer_.Append(buf, p - buf);
    }
}

LogStream& LogStream::operator<<(short v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(unsigned short v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(int v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(unsigned int v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(long v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(unsigned long v) {
    FormatInteger(v);
    return *this;
} 
LogStream& LogStream::operator<<(long long v) {
    FormatInteger(v);
    return *this;
}
LogStream& LogStream::operator<<(unsigned long long v) {
    FormatInteger(v);
    return *this;
}  
LogStream& LogStream::operator<<(double v) {
    // 浮点数处理
    if (buffer_.avail() >= 32) {
        int len = snprintf(buffer_.current(), 32, "%.12g", v);
        buffer_.Append(buffer_.current(), len);
    }
    return *this;
}