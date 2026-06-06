/**
 * @file HttpHandler.cpp
 * @brief 应用业务层（Application Layer）的 HTTP 路由分发与业务逻辑具体实现。
 * @details
 * 负责消费由协议层编解码器（HttpCodec）拆包出的 HttpRequest 结构体。
 * 在此文件中完成：
 * 1. 像素级的 URL 路由分发（如 `/`、`/api/status` 等）；
 * 2. 拼装标准的物理 HTTP 响应帧（状态行、响应头、响应体）；
 * 3. 顺藤摸瓜调用 `conn->Send()` 将响应交付给底层缓冲区，由专属 Sub-Reactor 异步回喷给客户端。
 */
#include "HttpHandler.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/http/HttpCodec.h"
#include "reactor/log/Logger.h"

namespace app {

void HandleHttpRequest(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                       const reactor::http::HttpRequest& req)
{
    LOG_INFO << "💡 [HttpHandler]: 成功捕获请求！当前路由 Path: " << req.path;

    std::string body;
    std::string status_line = "HTTP/1.1 200 OK\r\n";
    std::string content_type = "text/html; charset=utf-8\r\n";

    // 🌟 标准的直观路由分发
    if (req.path == "/" || req.path == "/index.html") {
        body = "<html>"
               "<head><title>Mini-Reactor</title></head>"
               "<body>"
               "<h1>🎉 恭喜！HttpHandler 业务层重构成功！</h1>"
               "<p>业务代码已成功搬迁至 /app 目录，名字比以前更直观了！</p>"
               "</body>"
               "</html>";
    } 
    // 模拟一个后端 API 接口
    else if (req.path == "/api/status") {
        content_type = "application/json\r\n";
        body = "{\"status\":\"healthy\",\"engine\":\"Mini-Reactor-v2\"}";
    } 
    // 404 错误页面兜底
    else {
        status_line = "HTTP/1.1 404 Not Found\r\n";
        body = "<h1>404 Not Found</h1><p>你访问的路由在 Reactor 宇宙中不存在。</p>";
    }

    // 拼装标准的物理 HTTP 响应帧
    std::string response = status_line +
                           "Content-Type: " + content_type +
                           "Content-Length: " + std::to_string(body.size()) + "\r\n"
                           "Connection: Keep-Alive\r\n" 
                           "\r\n" + body;

    // 🌟 顺藤摸瓜，通过连接将拼装好的数据回喷给内核缓冲区
    conn->Send(response);
}

} // namespace app