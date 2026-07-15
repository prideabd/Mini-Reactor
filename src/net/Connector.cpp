#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <algorithm>

#include "reactor/net/Connector.h"
#include "reactor/net/EventLoop.h"
#include "reactor/net/Channel.h"
#include "reactor/log/Logger.h"

namespace reactor::net {

namespace {
constexpr double kInitRetryDelaySec = 0.5; // 初始退避 0.5s
constexpr double kMaxRetryDelaySec = 30.0; // 最大退避 30s

// 自连接检测：非阻塞 connect 偶发的「本地端口恰好等于目标端口」自环，需丢弃重连
bool IsSelfConnect(int sock_fd) {
    struct sockaddr_in local;
    struct sockaddr_in peer;
    socklen_t llen = sizeof(local);
    socklen_t plen = sizeof(peer);
    if (::getsockname(sock_fd, (struct sockaddr*)&local, &llen) < 0) {
        return false;
    }
    if (::getpeername(sock_fd, (struct sockaddr*)&peer, &plen) < 0) {
        return false;
    }

    return local.sin_family == AF_INET &&
           peer.sin_family == AF_INET &&
           local.sin_port == peer.sin_port &&
           local.sin_addr.s_addr == peer.sin_addr.s_addr;
}
} // namespace

Connector::Connector(EventLoop* loop, const std::string& ip, int port)
    : loop_(loop),
      ip_(ip),
      port_(port),
      state_(kDisconnected),
      connect_(false),
      retry_delay_sec_(kInitRetryDelaySec),
      retry_timer_(0)
{
}

Connector::~Connector() {
    // 正常情况下，StopInLoop() / RemoveAndResetChannel() 会提前清掉 channel_。
    // 若这里仍有 channel_，说明 owner 没有正确 Stop，或者还有异常路径未收敛。
    if (channel_) {
        LOG_ERROR << "⚠️ [Connector] 析构时仍在连接中，请确保先调用 Stop()";
    }
}

void Connector::Start() {
    connect_.store(true);

    auto self = shared_from_this();
    loop_->RunInLoop([self]() {
        self->StartInLoop();
    });
}

void Connector::StartInLoop() {
    if (!connect_.load()) {
        LOG_DEBUG << "🛑 [Connector] 已取消，不再发起连接";
        return;
    }
    if (state_.load() == kDisconnected) {
        Connect();
    }
}

void Connector::Stop() {
    connect_.store(false);

    auto self = shared_from_this();
    loop_->RunInLoop([self]() {
        self->StopInLoop();
    });
}

void Connector::StopInLoop() {
    if (retry_timer_ != 0) {
        loop_->CancelTimer(retry_timer_);
        retry_timer_ = 0;
    }
    if (state_.load() == kConnecting && channel_) {
        state_.store(kDisconnected);
        int sock_fd = RemoveAndResetChannel();
        ::close(sock_fd);
    }
}

void Connector::Connect() {
    // 创建 socket 套接口
    int sock_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (sock_fd < 0) {
        LOG_ERROR << "❌ [Connector] 创建 socket 失败, errno = " << errno;
        if (error_callback_) error_callback_();
        return;
    }

    struct sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr) <= 0) {
        LOG_ERROR << "❌ [Connector] 非法的上游 IP: " << ip_;
        ::close(sock_fd);
        if (error_callback_) error_callback_();
        return;
    }

    int ret = ::connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr));
    int saved_errno = (ret == 0) ? 0 : errno;
    switch (saved_errno) {
        // 连接已建立 / 正在进行中：注册可写，等待内核回报结果
        case 0:
        case EINPROGRESS:
        case EINTR:
        case EISCONN:
            Connecting(sock_fd);
            break;
        
        // 可恢复错误：退避后重试
        case EAGAIN:          // 本地临时端口耗尽
        case EADDRINUSE:
        case EADDRNOTAVAIL:
        case ECONNREFUSED:
        case ENETUNREACH:
            Retry(sock_fd);
            break;

        // 其余视为不可恢复：关闭并直接报错
        default:
            LOG_ERROR << "❌ [Connector] connect 不可恢复错误, errno = " << saved_errno;
            ::close(sock_fd);
            if (error_callback_) error_callback_();
            break;
    }
}

void Connector::Connecting(int sock_fd) {
    state_.store(kConnecting);
    channel_ = std::make_unique<Channel>(loop_, sock_fd);

    // 这里捕获 shared_ptr，是为了保证：只要 Channel 还挂在 epoll 上，Connector 就不会提前析构。
    // 该引用环会在 RemoveAndResetChannel() -> ResetChannel() 时被打破。
    auto self = shared_from_this();
    channel_->SetWriteCallback([self]() {
        self->HandleWrite();
    });
    // 非阻塞 connect 的完成/失败都会通过 epoll 事件通知：
    //   - EPOLLOUT：连接有结果，可能成功也可能失败；
    //   - EPOLLERR：socket 出现异步错误。
    // 两种情况都必须用 getsockopt(SO_ERROR) 获取最终 connect 结果，
    // 因此统一收敛到 HandleWrite()。
    channel_->SetErrorCallback([self]() {
        self->HandleWrite();
    });

    channel_->EnableWriting();
}

void Connector::HandleWrite() {
    if (state_.load() != kConnecting) {
        return; // 状态已变（例如已被 Stop），忽略本次事件
    }
    int sock_fd = RemoveAndResetChannel(); // 先摘 channel，拿回裸 fd

    // 用 SO_ERROR 判定非阻塞 connect 的真实结果
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        err = errno;
    }
    if (err != 0) {
        LOG_ERROR << "❌ [Connector] 连接失败, SO_ERROR = " << err;
        Retry(sock_fd);
        return;
    }
    if (IsSelfConnect(sock_fd)) {
        LOG_ERROR << "♻️ [Connector] 检测到自连接，丢弃并重试";
        Retry(sock_fd);
        return;
    }

    // 连接成功
    state_.store(kConnected);
    retry_delay_sec_ = kInitRetryDelaySec; // 成功连接后复位退避
    if (connect_.load()) {
        LOG_INFO << "🔗 [Connector] 已连上 上层 C++ 后端 " << ip_ << ":" << port_
                 << " FD [" << sock_fd << "]";
        if (new_connection_callback_) {
            new_connection_callback_(sock_fd); // 移交 fd
        } else {
            ::close(sock_fd);
        }
    } else {
        ::close(sock_fd); // 连接成功前已被 Stop，弃用该 fd
    }
}

void Connector::Retry(int sock_fd) {
    ::close(sock_fd);
    state_.store(kDisconnected);

    if (!connect_.load()) {
        LOG_DEBUG << "🛑 [Connector] 已被取消，停止重试";
        return;
    }

    // 每次失败都通知上层（上层可在回调里调用 Stop() 主动放弃）
    if (error_callback_) error_callback_();

    LOG_INFO << "🔄 [Connector] " << retry_delay_sec_ << "s 后重连 "
             << ip_ << ":" << port_;
    
    auto self = shared_from_this();
    retry_timer_ = loop_->RunAfter(retry_delay_sec_, [self]() {
        self->retry_timer_ = 0;
        self->StartInLoop();
    });
    retry_delay_sec_ = std::min(retry_delay_sec_ * 2, kMaxRetryDelaySec);
}

int Connector::RemoveAndResetChannel() {
    channel_->DisableAll();
    channel_->Remove();
    int sock_fd = channel_->GetFd();

    // 不能在 Channel 自己的回调栈内直接销毁 Channel。
    // 用 self 延长 Connector 生命周期，直到 ResetChannel() 执行完毕。
    auto self = shared_from_this();
    loop_->QueueInLoop([self]() {
        self->ResetChannel();
    });
    return sock_fd;
}

void Connector::ResetChannel() {
    channel_.reset(); // 打破 channel_ -> callback -> shared_ptr<Connector> 的引用环
}

} // namespace reactor::net
