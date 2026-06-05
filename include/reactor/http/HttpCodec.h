#ifndef HTTP_CODEC_H
#define HTTP_CODEC_H

/**
 * @file HttpCodec.h
 * @brief HTTP 协议编解码器，封装协议解析细节，实现传输层与应用层（HTTP 协议）的解耦。
 */

#include <functional>
#include <memory>
#include "reactor/net/http/HttpContext.h"


// 向前声明传输层的 Buffer，避免头文件相互包含的强耦合
namespace reactor::net {
    class Buffer;
    class TcpConnection;
}

namespace reactor::http {

class HttpCodec {
public:
    // 当 TcpServer 收到消息时，调用此接口进行解析
    // 解析成功后，会通过回调函数将结构化数据传给 HttpServer
    // void onMessage(const std::shared_ptr<TcpConnection>& conn, Buffer* buf);

private:
    // HttpContext context_; // 每个连接应当对应一个解析上下文
};

} // namespace reactor::http

#endif // HTTP_CODEC_H
