/**
 * @file HttpServer.cpp
 * @brief 应用协议服务器门面类的具体实现与传输/协议层的代理接力。
 * @details
 * 实现了 HttpServer 的构造中转：
 * 1. 在构造函数内部，通过 `std::bind` 将底座 `tcp_server_` 的数据接收事件（`setMessageCallback`）
 * 硬绑定到自身的 `OnConnectionMessage` 代理函数上；
 * 2. 当多线程子反应堆（Sub-Reactor）捕获到套接字数据并薅入 Buffer 后，`OnConnectionMessage`
 * 立刻原封不动地将连接和缓冲区指针抛给 `http_codec_.OnMessage()` 状态机进行协议解析，
 * 从而完美完成了从“网路流”向“协议帧”的跨层接力。
 */
#include "reactor/http/HttpServer.h"

namespace reactor::http {

HttpServer::HttpServer(reactor::net::EventLoop* loop, int port, size_t thread_count)
    : tcp_server_(loop, port, thread_count)
{
    tcp_server_.setMessageCallback(
        std::bind(&HttpServer::OnConnectionMessage, this, std::placeholders::_1, std::placeholders::_2)
    );
}

void HttpServer::Start() {
    tcp_server_.Start();
}

void HttpServer::OnConnectionMessage(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                                      reactor::net::Buffer* buf)
{
    // 收到原始字节，踩进 http_codec 进行解包状态机转换
    http_codec_.OnMessage(conn, buf);
}

}   // namespace reactor::http