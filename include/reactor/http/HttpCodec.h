#ifndef HTTP_CODEC_H
#define HTTP_CODEC_H

/**
 * @file HttpCodec.h
 * @brief HTTP 协议编解码器，封装协议解析细节，实现传输层与应用层（HTTP 协议）的解耦。
 */

#include <functional>
#include <memory>
#include "reactor/http/HttpContext.h"


// 向前声明传输层的 Buffer，避免头文件相互包含的强耦合
namespace reactor::net {
    class Buffer;
    class TcpConnection;
}

namespace reactor::http {

class HttpCodec {
public:
    using TcpConnectionPtr = std::shared_ptr<reactor::net::TcpConnection>;
    using HttpCallback = std::function<void(const TcpConnectionPtr&, const HttpRequest&)>;

    // 保留一个默认构造函数
    HttpCodec() = default;
    
    explicit HttpCodec(const HttpCallback& cb) : http_callback_(cb) {}

    // 当 TcpServer 收到消息时，调用此接口进行解析
    // 解析成功后，会通过回调函数将结构化数据传给 HttpServer
    void OnMessage(const TcpConnectionPtr& conn, reactor::net::Buffer* buf);

    // 新增一个设置/更换回调的注册接口
    void SetHttpCallback(const HttpCallback& cb) {
        http_callback_ = cb;
    }

private:
    HttpCallback http_callback_;
    // HttpContext http_context_;
};

} // namespace reactor::http

#endif // HTTP_CODEC_H
