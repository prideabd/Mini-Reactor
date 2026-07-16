#include "reactor/proxy/ProxySession.h"
#include "reactor/net/EventLoop.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/log/Logger.h"

namespace reactor::net {

ProxySession::ProxySession(EventLoop* loop, const std::string& up_ip, int up_port,
                           const TcpConnectionPtr& down_conn)
    : loop_(loop),
      client_(TcpClient::Create(loop, up_ip, up_port)),
      down_conn_(down_conn)
{
}

ProxySession::~ProxySession() {
    LOG_INFO << "✨ [ProxySession] destroyed";
}

void ProxySession::Start() {
    // Start() 必须在 ProxySession 已经由 std::shared_ptr 管理后调用。
    // 正确顺序：
    //   auto session = std::make_shared<ProxySession>(...);
    //   down_conn->SetContext(session);
    //   session->Start();
    //
    // 这里使用 weak_from_this()：
    //   1. 避免异步迟到回调访问已析构的 ProxySession；
    //   2. 避免 shared_from_this() 强捕获导致引用环：
    //      ProxySession -> TcpClient -> callback -> ProxySession。
    std::weak_ptr<ProxySession> weak_self = weak_from_this();
    
    client_->SetConnectionCallback([weak_self](const TcpConnectionPtr& conn) {
        if (auto self = weak_self.lock()) {
            self->OnUpstreamConnection(conn);
        }
    });

    client_->SetMessageCallback([weak_self](const TcpConnectionPtr& conn, Buffer* buf) {
        if (auto self = weak_self.lock()) {
            self->OnUpstreamData(conn, buf);
        }
    });

    client_->SetErrorCallback([weak_self]() {
        if (auto self = weak_self.lock()) {
            self->OnUpstreamError();
        }
    });

    client_->Connect();
}

void ProxySession::OnUpstreamConnection(const TcpConnectionPtr& up_conn) {
    if (tearing_down_) {
        return;
    }

    if (up_conn->GetState() == TcpConnection::kConnected) {
        // ---- 上游连接成功：桥接建立 ----
        up_conn_= up_conn;
        up_ready_ = true;

        LOG_INFO << "🔗 [Proxy] 上游已就绪，桥接建立 FD [" << up_conn->GetFd() << "]";

        // 上游连接建立前，下游可能已经发来数据；此处一次性冲刷。
        if (!pending_up_.empty()) {
            up_conn_->Send(pending_up_);
            pending_up_.clear();
        }
        return;
    }

    // ---- 上游断开：关闭下游 ----
    up_ready_ = false;
    if (up_conn_ == up_conn) {
        up_conn_.reset();
    }

    LOG_INFO << "🔌 [Proxy] 上游断开，优雅关闭下游";

    if (down_conn_) {
        down_conn_->Shutdown(); // 阶段六可升级为返回 502/504 错误页
    }
}

void ProxySession::OnUpstreamError() {
    if (tearing_down_) {
        return;
    }

    // 当前 Connector 的 ErrorCallback 可能是“每次连接失败/重试”都会触发。
    // 阶段一先不在这里主动关闭下游，避免上游短暂失败时立即断开。
    // 如果你希望 connect 失败即快速失败，可在这里 down_conn_->Shutdown()
    // 或阶段六升级为给下游回 502。
    LOG_ERROR << "❌ [Proxy] 上游连接失败或正在重试";
}

// 下游 -> 上游
void ProxySession::OnDownstreamData(Buffer* buf) {
    if (tearing_down_) {
        buf->RetrieveAll();
        return;
    }

    std::string data(buf->Peek(), buf->ReadableBytes());
    buf->RetrieveAll();

    if (data.empty()) {
        return;
    }

    if (up_ready_ && up_conn_) {
        up_conn_->Send(data);
    } else {
        // 上游握手尚未完成时先暂存。
        // 注意：阶段一暂未做 pending_up_ 大小限制，阶段二/六应加背压和上限
        pending_up_ += data;
    }
}

// 上游 -> 下游
void ProxySession::OnUpstreamData(const TcpConnectionPtr& /*up_conn*/, Buffer* buf) {
    if (tearing_down_) {
        buf->RetrieveAll();
        return;
    }

    std::string data(buf->Peek(), buf->ReadableBytes());
    buf->RetrieveAll();

    if (data.empty()) {
        return;
    }

    if (down_conn_) {
        down_conn_->Send(data);
    }
}

void ProxySession::Teardown() {
    if (tearing_down_) {
        return;
    }
    tearing_down_ = true;

    LOG_INFO << "🧹 [Proxy] 下游断开，收拾上游连接";   

    if (client_) {
        client_->Disconnect(); // 若上游已建立，优雅关闭写端
        client_->Stop();       // 若 Connector 仍在连接/重试，停止它
    }

    up_ready_ = false;
    up_conn_.reset();
    pending_up_.clear();

    // 打破 down_conn.context -> ProxySession -> down_conn_ 的引用环。
    down_conn_.reset();

    // 释放 TcpClient。TcpClient 内部回调使用 weak_ptr，不会访问已析构对象。
    client_.reset();
}

} // namespace reactor::net