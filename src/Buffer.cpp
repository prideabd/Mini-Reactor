#include <cerrno>
#include <unistd.h>
#include <sys/uio.h>
#include "Buffer.h"

ssize_t Buffer::ReadFd(int fd, int* saved_errno) {
    char extra_buf[65536]; // 64KB临时缓冲区
    struct iovec vec[2];
    const size_t writable = WritableBytes();

    // 第一块：指向当前可写空间
    vec[0].iov_base = BeginWrite();
    vec[0].iov_len = writable;
    // 第二块：指向一个极大栈内存临时缓冲区
    vec[1].iov_base = extra_buf;
    vec[1].iov_len = sizeof(extra_buf);

    // 如果当前 Buffer 剩下的地方还挺大（比如大于60KB），那就不动用第二块栈空间了
    const int iovcnt = (writable < sizeof(extra_buf)) ? 2 : 1;

    // 底层读取
    const ssize_t n = ::readv(fd, vec, iovcnt);

    if (n < 0) {
        *saved_errno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        // 读取的数据可以放到可写空间 writable 里
        write_index_ += n;
    } else {
        // 数据过大，溢出数据放在了 extra_buf 中
        write_index_ = buffer_.size(); // buffer 已经写满
        Append(extra_buf, n - writable); // 溢出了 n - writable 字节放在 extra_buf
    }
    return n;
}