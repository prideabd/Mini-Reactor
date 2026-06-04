#pragma once
#include <cstring>
#include <string>

// 日志流缓冲区
const int kSmallBuffer = 4000;        // 4KB : 格式化每条日志行的临时小 Buffer
const int kLargeBuffer = 4000 * 1000; // 4MB : 双缓冲区满载

// 极速日志内存容器类
template <int SIZE>
class LogBuffer {
public:
    LogBuffer() : cur_(data_) {}
    ~LogBuffer() {}

    // 向内存块追加数据
    void Append(const char* buf, size_t len) {
        if (static_cast<size_t>(Avail()) > len) {
            std::memcpy(cur_, buf, len);
            cur_ += len;
        }
    }
    const char* Data() const { return data_; } // 只读不发，加 const 修饰
    char* Current() { return cur_; }           // 写驱动
    int Length() const { return static_cast<int>(cur_ - data_); }
    int Avail() const { return static_cast<int>(data_ + SIZE - cur_); }
    void Add(size_t len) { cur_ += len; }      // 仅移动指针，避免冗余拷贝

    void Reset() { cur_ = data_; }
    void Bzero() { std::memset(data_, 0, sizeof(data_)); }

private:
    char data_[SIZE];
    char* cur_;
};

// 模拟 std::cout 流式写入器
class LogStream {
    typedef LogStream self;
public:
    using Buffer = LogBuffer<kSmallBuffer>;
    
    self& operator<<(bool v) {
        buffer_.Append(v ? "1" : "0", 1);
        return *this;
    }
    // 这里没有写在 .h 文件中，是因为需要调用 formatInterger 函数
    self& operator<<(short);
    self& operator<<(unsigned short);
    self& operator<<(int);
    self& operator<<(unsigned int);
    self& operator<<(long);
    self& operator<<(unsigned long);
    self& operator<<(long long);
    self& operator<<(unsigned long long);
    self& operator<<(float v) {
        *this << static_cast<double>(v);
        return *this;
    }
    self& operator<<(double);
    self& operator<<(char v) {
        buffer_.Append(&v, 1);
        return *this;
    }
    self& operator<<(const char* str) {
        if (str) {
            buffer_.Append(str, std::strlen(str));
        } else {
            buffer_.Append("(null)", 6);
        }
        return *this;
    }
    self& operator<<(const std::string& v) {
        buffer_.Append(v.c_str(), v.size());
        return *this;
    }
    
    const Buffer& GetBuffer() const { return buffer_; }
    void ResetBuffer() { buffer_.Reset();}

private:
    // 将整数变成字符
    template<typename T> void FormatInteger(T);
    Buffer buffer_;
};