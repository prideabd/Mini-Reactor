#pragma once

#include <memory>
#include <atomic>

namespace reactor::net { class TcpConnection; }
namespace reactor::http { class HttpRequest; }

namespace app {

extern std::atomic<uint64_t> g_global_request_count;
extern std::atomic<bool> g_agent_cooldown_mode;

/**
 * @brief HTTP 路由分发总线 (Dispatcher)
 * 采用 O(1) 的哈希表映射取代冗长的 if-else，负责将请求精准分发到对应的业务控制器。
 */
class HttpDispatcher{
public:
    static void Dispatch(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                         const reactor::http::HttpRequest& req);
};

} // namespace app