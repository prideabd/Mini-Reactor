#pragma once

/**
 * @file HttpHandler.h
 * @brief 应用业务层（Application Layer）的 HTTP 请求处理接口声明。
 * @details 
 * 本文件属于纯业务层（App Layer）。它完全不感知底层的 Socket 读写和 Epoll 多线程细节，
 * 仅定义了供协议层回调的高级业务函数接口。通过控制反转（IoC）机制，它将被注入到 HttpCodec 中，
 * 实现业务逻辑与网络引擎的完美去耦。
 */

#include <memory>

namespace reactor::net {
    class TcpConnection;
}

namespace reactor::http {
    class HttpRequest;
}

// 用户特定的业务命名空间
namespace app {
/**
 * @brief 核心 HTTP 业务处理器
 * @param conn 传输层连接强引用指针（用于最后将响应 Send 回网路）
 * @param req  协议层洗干净的结构化 HTTP 请求体对象
 */

 void HandleHttpRequest(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                        const reactor::http::HttpRequest& req);

} // namespace app