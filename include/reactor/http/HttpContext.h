#ifndef HTTP_CONTEXT_H
#define HTTP_CONTEXT_H

/**
 * @file HttpContext.h
 * @brief HTTP 解析状态机，负责维护单个连接的解析状态，并将字节流转换为结构化的 HTTP 请求。
 */

 #include <string>
 #include <unordered_map>

// 向前声明传输层的 Buffer，避免头文件相互包含的强耦合
namespace reactor::net {
    class Buffer;
}

namespace reactor::http {

// 承载 HTTP 请求结果的干净结构体（纯业务对象）
struct HttpRequest {
    std::string method;     // 请求方法，如 "GET", "POST"
    std::string path;       // 请求路径，如 “/index.html"
    std::string query;      // URL 中的参数，如 "id=123"
    std::string version;    // HTTP 版本，如 "HTTP/1.1"
    std::unordered_map<std::string, std::string> headers;   // 所有的请求头键值对
    std::string body;
    // 可以根据需要增加 Timestamp receive_time; 

    // 长连接复用时，快速重置结构体
    void Reset() {
        method.clear();
        path.clear();
        query.clear();
        version.clear();
        headers.clear();
        body.clear();
    }
};

class HttpContext {
public:
    enum HttpRequestParseState {
        kExpectRequestLine, // 期待解析请求行（GET /index.html HTTP/1.1）
        kExpectHeaders,     // 期待解析 Header
        kExpectBody,        // 期待解析 Body
        kGotAll,            // 解析完成
    };

    HttpContext() : state_(kExpectRequestLine) {}

    // 解析函数
    bool ParseRequest(reactor::net::Buffer* buf);

    // 盘查是否解析成功
    bool GotAll() const { return state_ == kGotAll; }
    
    // 长连接处理完一轮请求后，重置状态机
    void Reset() {
        state_= kExpectRequestLine;
        request_.Reset();
    }

    // 暴露给上层业务的接口
    const HttpRequest& GetRequest() const { return request_; }
    HttpRequest& GetRequest() { return request_; }

private:
    bool ProcessRequestLine(const char* begin, const char* end);

    HttpRequestParseState state_;   // 当前解析状态
    HttpRequest request_;           // 关联的请求对象，用于存储解析结果
};

} // namespace reactor::http

#endif // HTTP_CONTEXT_H
