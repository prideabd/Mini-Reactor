#include <algorithm>
#include <cctype>

#include "reactor/http/HttpContext.h"
#include "reactor/net/Buffer.h"

namespace reactor::http {

bool HttpContext::ParseRequest(reactor::net::Buffer* buf) {
    bool ok = true;      // 标记解析是否合法
    bool hasMore = true; // 是否还有未处理的字节
    
    while (hasMore) {
        // 期待解析请求行
        if (state_ == kExpectRequestLine) {
            // !!!!修改的地方，是否考虑在 Buffer 类加一个接口函数用来解析
            const char* crlf = std::search(buf->Peek(), buf->BeginWrite(), "\r\n", buf->BeginWrite());

            if (crlf != buf->BeginWrite()) {
                ok = ProcessRequestLine(buf->Peek(), crlf);
                if (ok) {
                    //!!! 需要 Buffer 类添加 RetrieveUntil 接口函数
                    buf->RetrieveUntil(crlf + 2); // 确认消费这一行，连同 \r\n 移动读指针
                    state_= kExpectHeaders;
                } else {
                    // 请求行格式非法
                    hasMore = false;
                }
            } else {
                // !!! 为什么半包无 \r\n
                // 半包，无 \r\n
                hasMore = false;
            }

        }
    }
}

bool HttpContext::ProcessRequestLine(const char* begin, const char* end) {
    bool succeed = false;
    const char* start = begin;

    // 搜索第一个空格，切出 GET/POST
    const char* space = std::find(start, end, ' ');
    if (space != end) {
        request_.method.assign(start, space);
        start = space + 1; // 走过空格，来到 URL 

        // 搜索第二个空格，切出 URL 
        space = std::find(start, end, ' ');
        if (space != end) {
            const char* path_end = space;
            // 查看是否携带参数（即带有 ？）
            const char* question = std::find(start, path_end, '?');
            if (question != path_end) {
                // 带有参数，比如 /index.html?id=123
                request_.path.assign(start, question);      // 截取 /index.html
                request_.query.assign(question, path_end);  // 截取 id=123
            } else {
                // 不带参数，比如 /index.html
                request_.path.assign(start, path_end);
            }
            
            // 剩下部分是 HTTP 版本号
            start = space + 1;
            request_.version.assign(start, end);
            succeed = true;
        }
    }
    return succeed;
}

}