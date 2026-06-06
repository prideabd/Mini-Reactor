#include "reactor/net/Buffer.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/net/EventLoop.h"
#include "reactor/log/Logger.h"
#include "reactor/http/HttpCodec.h"

namespace reactor::http {

void HttpCodec::OnMessage(const TcpConnectionPtr& conn, reactor::net::Buffer* buf) {
    // 第一次连接，先创建专属 HTTP 状态机
    if (!conn->HasContext()) {
        conn->SetContext(std::make_shared<HttpContext>());
    }
    // 拿出状态机指针
    // HttpContext* ctx = std::any_cast<HttpContext>(conn->GetMutableContext());
    auto ctx = std::any_cast<std::shared_ptr<HttpContext>>(conn->GetContext());

    // 可能有粘包，半包等情况
    // 加入 while 循环，只要缓冲区里还有可读字节，就不断尝试驱动状态机
    while (buf->ReadableBytes() > 0) {
        // 真正解析函数
        if (!ctx->ParseRequest(buf)) {
            // 报文非法，直接喷回 400 Bad Request 并安全阻断连接
            LOG_WARN << "⚠️ [HttpCodec]: 收到畸变非法 HTTP 请求，强行阻断连接！";
            conn->Send("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
            conn->Shutdown();
            return;
        }
        // 解析成功
        if (ctx->GotAll()) {
            LOG_INFO << "🔔 [HttpCodec]: 成功剥离完整 HTTP 请求帧！Path: " << ctx->GetRequest().path;

            // 真正的回复消息
            if (http_callback_) {
                http_callback_(conn, ctx->GetRequest());
            }

            // 长连接复用，重置状态机
            ctx->Reset();
        } else {
            // 如果执行到这里 ctx->GotAll() 为 false，说明发生了【半包】
            // 缓冲区里剩下的那点字节不够拼成一个完整的 HTTP 行/块了。
            // 此时必须 break 退出循环，挂起并等待下一次 EPOLLIN 事件唤醒 OnMessage！
            break;
        }
    }
}


} // namespace reactor::http