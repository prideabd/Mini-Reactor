#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

/**
 * @file HttpServer.h
 * @brief 应用协议服务器门面类（Facade Pattern）的声明。
 * @details
 * 作为连接“通用传输层网络库（TcpServer）”与“特定应用层协议（HttpCodec）”的中央枢纽。
 * 主要职责包括：
 * 1. 组合通用的多线程 `TcpServer` 网络底座，负责并发连接接收与传输层 Buffer 托管；
 * 2. 内嵌 `HttpCodec` 状态机，负责将原始字节流泵入 HTTP 协议层破译；
 * 3. 暴露外部门面接口 `SetHttpCallback`，供 `main.cpp` 动态注入最上层的业务回调。
 */

#include <functional>

#include "reactor/net/TcpServer.h"
#include "reactor/http/HttpCodec.h"

namespace reactor::http {

class HttpServer {
public:
    HttpServer(reactor::net::EventLoop* loop, int port, size_t thread_count);
    ~HttpServer() = default;

    // 供业务程序把具体的业务处理函数（比如 ProcessWebBusiness）注入进来
    void SetHttpCallback(const HttpCodec::HttpCallback& cb) {
        http_codec_.SetHttpCallback(cb);
    }

    void Start();

private:
    // 当底层 TcpServer 收到原始字节数据时，触发此函数
    void OnConnectionMessage(const std::shared_ptr<reactor::net::TcpConnection>& conn, 
                             reactor::net::Buffer* buf);
    reactor::net::TcpServer tcp_server_;
    HttpCodec http_codec_;
};

} // namespace reactor::http

#endif // HTTP_SERVER_H