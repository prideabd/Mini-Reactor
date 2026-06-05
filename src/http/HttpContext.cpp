#include <algorithm>
#include <cctype>

#include "reactor/http/HttpContext.h"
#include "reactor/net/Buffer.h"

namespace reactor::http {

bool HttpContext::ParseRequest(reactor::net::Buffer* buf) {
    bool ok = true;      // 标记解析是否合法
    bool hasMore = true; // 是否还有未处理的字节
    
    while (hasMore && ok) {
        // ==================== 1. 期待解析请求行 ====================
        if (state_ == kExpectRequestLine) {
            const char* crlf = buf->FindCRLF();
            if (crlf != nullptr) {
                ok = ProcessRequestLine(buf->Peek(), crlf);
                if (ok) {
                    buf->RetrieveUntil(crlf + 2); // 确认消费这一行，连同 \r\n 移动读指针
                    state_= kExpectHeaders;
                } else {
                    // 请求行格式非法
                    hasMore = false;
                }
            } else {
                // 半包，无 \r\n
                hasMore = false;
            }
        }
        // ==================== 2. 期待解析请求头 ====================
        // 例如: Host: 127.0.0.1\r\n
        else if (state_ == kExpectHeaders) {
            const char* crlf = buf->FindCRLF();
            if (crlf != nullptr) {
                // 边界特判：如果 crlf 恰好等于当前读指针 Peek()，说明遇到连续的第二个 \r\n
                // 这在 HTTP 协议中代表致命的【空行分割线】，标志着 Header 彻底结束了！
                if (crlf == buf->Peek()) {
                    buf->RetrieveUntil(crlf + 2);
                    // 根据 content-length (注意：存储时已转小写) 判断状态流转
                    auto it = request_.headers.find("content-length");
                    if (it != request_.headers.end()) {
                        int body_len = 0;
                        try {
                            body_len = std::stoi(it->second);
                        } catch (...) {
                            ok = false; // 非法长度字段
                            hasMore = false;
                        }
                        if (ok && body_len > 0) {
                            state_ = kExpectBody; // 开启下一阶段：期待请求体！
                        } else {
                            state_ = kGotAll;     // 有字段但长度为0，直接完结
                            hasMore = false;
                        }
                    } else {
                        // 没有携带 Content-Length，默认为 GET 等无 Body 请求，直接完结
                        state_ = kGotAll;
                        hasMore = false;
                    }
                } else {
                    // 正常 header 头，类似 key : value
                    const char* colon = std::find(buf->Peek(), crlf, ':');
                    if (colon != crlf) {
                        // 切除 key
                        std::string key(buf->Peek(), colon);
                        // 统一转为小写，解决 HTTP 头部大小写不敏感的问题
                        std::transform(key.begin(), key.end(), key.begin(), ::tolower);

                        // 切除 value
                        const char* v_begin = colon + 1;
                        while (v_begin < crlf && std::isspace(*v_begin)) {
                            v_begin++;
                        }
                        std::string value(v_begin, crlf);
                        
                        // 放入结构体的哈希表
                        request_.headers[key] = value;
                    } else {
                        // 非空行又没有冒号，非法报文
                        ok = false;
                        hasMore = false;
                    }
                    buf->RetrieveUntil(crlf + 2); // 顺带消费 \r\n
                }
            } else {
                // 半包，无 \r\n
                hasMore = false;
            }
        }
        // ==================== 3. 期待请求体 (Body) ====================
        else if (state_ == kExpectBody) {
            // 此时必须通过 Content-Length 强制控制读取的边界，绝对不能再找 \r\n！
            // 此时 header key 已经是小写，且在状态转换时已校验过合法性
            int expect_body_len = std::stoi(request_.headers["content-length"]);
            
            // 检查 Buffer 里的可读字节数是否已经大于等于 Body 所需的字节数
            if (buf->ReadableBytes() >= static_cast<size_t>(expect_body_len)) {
                // 完整的 Body 已经全部进缓冲区了，一把抓走
                request_.body.assign(buf->Peek(), expect_body_len);
                buf->Retrieve(expect_body_len); // 精准消费
                
                state_ = kGotAll; // 整个 HTTP 报文（请求行+头+体）完美收官！
                hasMore = false;
            } else {
                // Body 发生半包（比如大文件只传了一半），挂起退出，等待下一次可读事件触发
                hasMore = false; 
            }
        }
    }
    return ok;
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
                request_.query.assign(question + 1, path_end);  // 截取 id=123
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

} // namespace reactor::http