#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include "reactor/net/EventLoop.h"
#include "reactor/net/Channel.h"
#include "reactor/net/Acceptor.h"
#include "reactor/log/Logger.h"

Acceptor::Acceptor(EventLoop* loop, int port)
    : loop_(loop), listen_fd_(-1)
{
    // 创建非阻塞监听套接字
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        LOG_FATAL << "创建监听 socket 失败！";
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (::bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(listen_fd_);
        LOG_FATAL << "绑定端口 8888 失败！";
    }

    // 实例化专属 channel, 只进行绑定， 当前只做配置
    // 在后续正式调用监听，设定可读，
    accept_channel_ = std::make_unique<Channel>(loop_, listen_fd_);
    accept_channel_->SetReadCallback(std::bind(&Acceptor::HandleRead, this));
}

Acceptor::~Acceptor() {
    accept_channel_->DisableAll();
    accept_channel_->Remove();
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
    }
}

void Acceptor::Listen() {
    if (::listen(listen_fd_, SOMAXCONN) < 0) {
        LOG_FATAL << "Listen 监听失败！";
    }
    // 这里才真正将 listen_fd_ 放置到 epoll 树上
    accept_channel_->EnableReading();
    LOG_INFO << "🌍 [网络网络]: 成功监听 8888 端口，正在等待客户端连接叩门...";
}

void Acceptor::Stop() {
    if (accept_channel_) {
        accept_channel_->DisableAll();
        accept_channel_->Remove();
    }
    LOG_INFO << "🛑 [Acceptor]: 已成功关闭端口 8888 的监听，大门已关闭，不再接受新连接。";
}

void Acceptor::HandleRead() {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    while (true) {
        int conn_fd = ::accept4(listen_fd_, (struct sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (conn_fd >= 0) {
            LOG_INFO << "🎉 [新客户上线]: 成功接入客户端 FD [" << conn_fd << "]！";
            if (new_connection_callback_) {
                new_connection_callback_(conn_fd);
            }
        } else {
            // ET 模式需要读取所有数据
            // 当全部读取完，由于非阻塞，内核会返回-1，并将 errno 设置为 EAGAIN
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) { // 排除信号中断
                continue;
            }
            LOG_ERROR << "accept4 发生未预期的底层异常, errno = " << errno;
            break;
        }
    }
}