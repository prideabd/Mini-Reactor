#ifndef BUFFER_H
#define BUFFER_H

#include <vector>
#include <string>
#include <algorithm>
#include <assert.h>

namespace reactor::net {

// 缓冲区
class Buffer {
public:
    static constexpr size_t kInitBufferSize = 1024;

    explicit Buffer(size_t init_size = kInitBufferSize)
        : buffer_(init_size),
          read_index_(0),
          write_index_(0)
    {}

    // 计算待读取字节
    size_t ReadableBytes() const {
        return write_index_ - read_index_;
    }
    // 计算剩余空间
    size_t WritableBytes() const {
        return buffer_.size() - write_index_;
    }
    // 计算已被读走，已经作废的空间
    size_t PrependableBytes() const {
        return read_index_;
    }
    // 返回可读的起始地址
    const char* Peek() const {
        return Begin() + read_index_;
    }
    // 写游标起始
    char* BeginWrite() {return Begin() + write_index_;}
    const char* BeginWrite() const { return Begin() + write_index_; }
    
    // 读取了 len 字节数据后，读游标右移
    void Retrieve(size_t len) {
        if (len < ReadableBytes()) {
            read_index_ += len;
        } else {
            RetrieveAll();
        }
    }
    // 全部清空
    void RetrieveAll() {
        read_index_ = 0;
        write_index_ = 0;
    }

    // 从临时缓冲区读取数据，写入buffer，保留传入const char*（应对内核裸指针、零拷贝）
    void Append(const char* data, size_t len) {
        EnsureWritableBytes(len);
        std::copy(data, data + len, BeginWrite());
        write_index_ += len;
    }
    // 上层语法糖，更加便利
    void Append(const std::string& str) {
        Append(str.data(), str.size());
    }
    // 无阻塞读取套接口
    ssize_t ReadFd(int fd, int* saved_errno);

    // 暴露给 HTTP 业务层的搜索 "\r\n" 函数
    const char* FindCRLF() const {
        const char* crlf = std::search(Peek(), BeginWrite(), "\r\n", "\r\n" + 2);
        return crlf == BeginWrite() ? nullptr : crlf;
    }
    // 暴露给 HTTP 业务层的读游标右移函数
    void RetrieveUntil(const char* end) {
        assert(Peek() <= end);
        assert(end <= BeginWrite());
        Retrieve(end - Peek());
    }


private:
    // 缓冲区起始
    char* Begin() { return &*buffer_.begin(); }
    const char* Begin() const { return &*buffer_.begin(); }

    // 确保空间有足够空间存储 len 字节
    void EnsureWritableBytes(size_t len) {
        if (WritableBytes() < len) {
            MakeSpace(len);
        }
    }
    // 扩容
    void MakeSpace(size_t len) {
        // 可存空间 + 废弃空间 小于 len,扩容
        if (WritableBytes() + PrependableBytes() < len) {
            buffer_.resize(write_index_ + len);
        } else {
            // 将可读数据移到最前面
            size_t readable = ReadableBytes();
            std::copy(Begin() + read_index_, Begin() + write_index_, Begin());
            read_index_ = 0;
            write_index_ = read_index_ + readable;
        }
    }

    std::vector<char> buffer_;
    size_t read_index_;
    size_t write_index_;


};

} // namespace reactor::net

#endif // BUFFER_H