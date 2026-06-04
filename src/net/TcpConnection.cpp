#include <cerrno>
#include <unistd.h>
#include "reactor/net/Channel.h"
#include "reactor/net/EventLoop.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/log/Logger.h"

TcpConnection::TcpConnection(EventLoop* loop, int conn_fd)
    : loop_(loop),
      conn_fd_(conn_fd),
      state_(kConnecting)
{
    channel_ = std::make_unique<Channel>(loop_, conn_fd_);

    // 绑定内部底层函数
    channel_->SetReadCallback(std::bind(&TcpConnection::HandleRead, this));
    channel_->SetWriteCallback(std::bind(&TcpConnection::HandleWrite, this));
    channel_->SetCloseCallback(std::bind(&TcpConnection::HandleClose, this));

}

TcpConnection::~TcpConnection() {
    // 资源都在各自的内部实现销毁
    LOG_INFO << "✨ [TcpConnection]: 客户端 FD [" << conn_fd_ << "] 资源已安全释放。";
}

void TcpConnection::ConnectionEstablished() {
    state_ = kConnected;
    channel_->EnableReading(); // 挂载红黑树，启动可读监听
}

void TcpConnection::ConnectionDestroyed() {
    state_ = kDisconnected;
    channel_->Remove(); // 从红黑树上移除
    ::close(conn_fd_);  // 物理断开套接字描述符
}

// 📥 底层读驱动
void TcpConnection::HandleRead() {
    int saved_errno = 0;
    bool error_or_close = false;
    // 必须一次性读完非阻塞套接字的所有内核积压，直到触发 EAGAIN
    while (true) {
        ssize_t n = input_buffer_.ReadFd(conn_fd_, &saved_errno);
        if (n > 0) {
            // 原始字节已经安全放入 input_buffer_, 通知 TcpServer 去拿
            if (message_callback_) {
                message_callback_(shared_from_this(), &input_buffer_);
            }
        } 
        // 核心拦截点：非阻塞套接字被薅空的标准信号
        else if (n < 0) {
            if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
                // ✅ 完美：内核缓冲区已经被我们彻底榨干、读空了！
                // 这是非阻塞 ET 模式下最健康的退出姿势
                break;
            } else {
                LOG_ERROR << "发生严重内核物理读取错误，FD: " << conn_fd_ << " errno: " << saved_errno;
                error_or_close = true;
                break;
            }
        } 
        // n == 0 代表对端主动断开连接 (收到了对端的 FIN 包)
        else if (n == 0) {
            error_or_close = true;
            break;
        }
    }
    // 如果对端下线或出错，触发清理
    if (error_or_close) {
        HandleClose();
    }
}

// 📤 底层写驱动：当内核发送缓冲区腾出空位时，被子 Epoll 叫醒
// 当输出不能一次性被发送完，在 send 函数中会将剩余输出存到输出缓冲区，并注册写监听事件
// 当内核发送空出来，就会发生变动被写监听，调用 HandleWrite
void TcpConnection::HandleWrite() {
    if (channel_->IsWriting()) {
        ssize_t n = ::write(conn_fd_, output_buffer_.Peek(), output_buffer_.ReadableBytes());
        if (n > 0) {
            output_buffer_.Retrieve(n); // 消费已经读取的字节
            if (output_buffer_.ReadableBytes() == 0) {
                // 读完了，注销写监听事件
                channel_->DisableWriting();
            }
        }
    }
}

void TcpConnection::HandleClose() {
    // 如果当前连接已经走在下线流向中了，子线程当场熔断返回，坚决拒绝二次进入！
    if (state_ == kDisconnecting || state_ == kDisconnected) {
        return; 
    }

    // 立刻上锁状态：当场在子线程标记为“正在断开”，后续的任何重复事件或循环都进不来了！
    state_ = kDisconnecting;

    // 让当前 Channel 注销红黑树上的所有事件监听，让 Epoll 树对该 FD 彻底保持沉默
    channel_->DisableAll();
    if (close_callback_) {
        close_callback_(shared_from_this());
    }
}

// 非阻塞无损发送
void TcpConnection::Send(const std::string& msg) {
    ssize_t nwrote = 0;
    size_t remaining = msg.size();
    bool faultError = false;

    // 输出缓冲区没有积压，直接放套接字
    if (output_buffer_.ReadableBytes() == 0) {
        // 套接字尽可能接收，剩下还存放在 msg 中
        nwrote = ::write(conn_fd_, msg.data(), msg.size());
        if (nwrote >= 0) {
            remaining = msg.size() - nwrote;
        } else {
            nwrote = 0;
            // 良性警告，无需触发 faultError
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                faultError = true;
            }
        }
    }
    // 多余出来的字节，存放到输出缓冲区 output_buffer_，并注册写监听事件
    if (!faultError && remaining > 0) {
        output_buffer_.Append(msg.data() + nwrote, remaining);
        if (!channel_->IsWriting()) {
            LOG_DEBUG << "🔄 [TcpConnection::Send]: 内核写缓冲区已满！剩余 [" << remaining 
                      << "] 字节暂存，启动 EPOLLOUT 监听。";
            channel_->EnableWriting(); // 注册写监听事件
        }
    }
}