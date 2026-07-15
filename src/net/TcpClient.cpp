#include <unistd.h>

#include "reactor/net/TcpClient.h"
#include "reactor/net/Connector.h"
#include "reactor/net/EventLoop.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/log/Logger.h"

namespace reactor::net {

TcpClient::Ptr TcpClient::Create(EventLoop* loop, const std::string& ip, int port) {
    Ptr client(new TcpClient(loop, ip, port));
    client->InitCallbacks();
    return client;
}

TcpClient::TcpClient(EventLoop* loop, const std::string& ip, int port)
    : loop_(loop),
      connector_(std::make_unique<Connector>(loop, ip, port)),
      connect_(true)
{
}

TcpClient::~TcpClient()
{
    // 析构函数里不再主动调用 connector_->Stop()。
    // 原因：Stop() 可能跨线程异步投递任务。虽然 Connector 已经用 shared_ptr 自保，
    // 但析构函数仍不应该承担异步停机语义。
    //
    // 正确流程：owner 在释放 TcpClient 前显式调用 Stop()/Disconnect()。
    // 在本项目阶段一里，ProxySession::Teardown() 应负责调用 client_->Disconnect()/Stop()。
    LOG_INFO << "✨ [TcpClient] destroyed";
}

void TcpClient::InitCallbacks() {
    std::weak_ptr<TcpClient> weak_self = weak_from_this();

    // Connector 连接成功后，把裸 fd 交给 TcpClient 包成 TcpConnection
    connector_->SetNewConnectionCallback([weak_self](int sock_fd) {
        if (auto self = weak_self.lock()) {
            self->NewConnection(sock_fd);
        } else {
            // TcpClient 已销毁，但 Connector 异步晚到并成功连上。
            // 没有 owner 接管这个 fd，必须关闭，避免 fd 泄漏。
            ::close(sock_fd);
        }
    });

    connector_->SetErrorCallback([weak_self]() {
        if (auto self = weak_self.lock()) {
            if (self->error_callback_) {
                self->error_callback_();
            }
        }
    });
}

void TcpClient::Connect() {
    connect_.store(true);

    if (connector_) {
        connector_->Start(); // 开始尝试连接
    }
}

void TcpClient::Stop() {
    connect_.store(false);

    if (connector_) {
        connector_->Stop();
    }
}

void TcpClient::Disconnect() {
    connect_.store(false);

    std::weak_ptr<TcpClient> weak_self = weak_from_this();
    loop_->RunInLoop([weak_self]() {
        if (auto self = weak_self.lock()) {
            if (self->connection_) {
                self->connection_->Shutdown();
            }
        }
    });
}


// 运行在 loop_ 线程（由 Connector::HandleWrite 触发），fd 已完成三次握手
void TcpClient::NewConnection(int sock_fd) {
    // 如果 owner 已经不希望连接了，直接关闭 fd。
    if (!connect_.load()) {
        ::close(sock_fd);
        return;
    }

    auto conn = std::make_shared<TcpConnection>(loop_, sock_fd);

    conn->SetMessageCallback(message_callback_);

    std::weak_ptr<TcpClient> weak_self = weak_from_this();
    conn->SetCloseCallback([weak_self](const TcpConnectionPtr& c) {
        if (auto self = weak_self.lock()) {
            self->RemoveConnection(c);
        } else {
            // TcpClient 已经不在了，至少把 TcpConnection 自身资源清掉。
            c->ConnectionDestroyed();
        }
    });

    connection_ = conn;
    conn->ConnectionEstablished();        // state = kConnected，挂上 epoll 读监听
    if (connection_callback_) {
        connection_callback_(conn);       // 通知上层：上游已就绪（state == kConnected）
    }
}

// 运行在 loop_ 线程（上游 close 事件源自本 loop）
void TcpClient::RemoveConnection(const TcpConnectionPtr& conn) {
    if (connection_callback_) {
        connection_callback_(conn);       // 通知上层：上游已断开（state == kDisconnecting）
    }

    if (connection_ == conn) {
        connection_.reset();              // 计数减一
    }
    
    loop_->QueueInLoop([conn]() {
        conn->ConnectionDestroyed();      // 延后销毁：摘除 channel + close(fd)
    });
}

} // namespace reactor::net