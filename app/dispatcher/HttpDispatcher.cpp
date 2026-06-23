/**
 * @file HttpDispatcher.cpp
 * @brief 应用业务层（Application Layer）的 HTTP 路由分发器。
 * @details
 * 采用 O(1) 的 std::unordered_map 哈希路由表替代冗长的 if-else，
 * 将各个 API 接口剥离为独立的静态处理函数，实现极致的高内聚低耦合。
 */
#include <functional>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>
#include <cmath> 

#include "nlohmann/json.hpp" // 第三库函数

#include "HttpDispatcher.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/http/HttpCodec.h"
#include "reactor/log/Logger.h"
#include "common/AppUtils.h"
#include "daemons/SysMetricsDaemon.h"
#include "repository/CommentRepository.h"
#include "repository/BlacklistManager.h"

namespace app {

using json = nlohmann::json;

// ==========================================
// 全局原子状态变量实体 (在 .cpp 中分配内存)
// ==========================================
std::atomic<uint64_t> g_global_request_count{0};
std::atomic<bool> g_agent_cooldown_mode{false};

// 定义标准 HTTP 路由上下文
struct RouteContext {
    const reactor::http::HttpRequest& req; // 接收报文信息
    std::string& status_line; // 回送报文的信息
    std::string& content_type;
    std::string& body;
    const std::string& client_ip;
};

// 路由挂载函数签名
using RouteHandler = std::function<void(RouteContext&)>;

// ==========================================
// 具体的业务处理器 (Handlers)
// ==========================================

// 分支 A：硬核后端动态核心监控 API
static void HandleMonitor(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

    static auto start_time = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    auto uptime_sec = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

    int live_connections = 1;
    int active_threads = 3;

    // 异步线程采集物理硬件
    auto current_metrics = SysMetricsDaemon::GetMetrics();

    // 组装 body
    json resp;
    resp["status"] = g_agent_cooldown_mode.load(std::memory_order_relaxed) ? "CIRCUIT_BREAKER" : "RUNNING";
    resp["uptime_seconds"] = uptime_sec;
    resp["live_connections"] = live_connections;
    resp["sub_reactors"] = active_threads;
    resp["total_requests"] = g_global_request_count.load(std::memory_order_acquire);
    resp["qps"] = current_metrics.qps;
    resp["cpu_usage"] = std::round(current_metrics.cpu_usage * 10.0) / 10.0;
    resp["mem_available"] = std::round(current_metrics.mem_available * 10.0) / 10.0;
    resp["disk_io"] = std::round(current_metrics.disk_io_mbps * 10.0) / 10.0;
    
    resp["cooldown_mode"] = g_agent_cooldown_mode.load(std::memory_order_relaxed);
    resp["engine"] = current_metrics.is_linux_env ? "Mini-Reactor-v2.5-(BareMetal)" : "Mini-Reactor-v2.5-(Mock)";

    ctx.body = resp.dump();
}

// 分支 B：提交留言（POST 写入）
static void HandlePostComment(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

    // 安全审计：前端表单格式校验
    if (ctx.req.headers.find("content-type") == ctx.req.headers.end() ||
        ctx.req.headers.at("content-type").find("application/x-www-form-urlencoded") == std::string::npos) {
        
        ctx.status_line = "HTTP/1.1 415 Unsupported Media Type\r\n";
        json err; 
        err["result"] = "ERROR"; 
        err["msg"] = "[网关拒绝]: 留言投递只认 urlencoded 标准表单格式！";
        ctx.body = err.dump();
        return;
    }

    // 检查熔断
    if (g_agent_cooldown_mode.load(std::memory_order_acquire)) {
        ctx.status_line = "HTTP/1.1 200 OK\r\n";
        json err;
        err["result"] = "ERROR"; 
        err["msg"] = "[AI 熔断保护中] 服务器当前遭遇高并发冲击，留言板已进入紧急安全冷却模式。";
        ctx.body = err.dump();
        return;
    }

    std::string raw_body = ctx.req.body;
    std::string nick = "匿名极客";
    std::string text = raw_body;
    if (!raw_body.empty()) {
        std::string key_value;
        std::stringstream ss(raw_body);
        while (std::getline(ss, key_value, '&')) {
            size_t pos = key_value.find('=');
            if (pos != std::string::npos) {
                std::string key = key_value.substr(0, pos);
                std::string decode_value = UrlDecode(key_value.substr(pos + 1));
                if (key == "name") nick = decode_value;
                else if (key == "content") text = decode_value;
            }
        }
    }

    LOG_INFO << "📥 [留言投递] 来源 IP: " << (ctx.client_ip.empty() ? "unknown" : ctx.client_ip)
         << " | 昵称: " << nick;

    // 在写入之前，多维度黑名单校验（昵称 / IP / 关键词）
    BlockResult verdict = BlacklistManager::CheckComment(nick, ctx.client_ip, text);
    if (verdict.blocked()) {
        ctx.status_line = "HTTP/1.1 403 Forbidden\r\n";

        std::string user_msg;
        std::string log_dim;
        switch (verdict.reason) {
            case BlockReason::NICKNAME:
                user_msg = "[安全拦截]: 您的账号（" + nick + "）因违规已被限制留言。";
                log_dim = "昵称";
                break;
            case BlockReason::IP:
                user_msg = "[安全拦截]: 您当前的网络环境因违规已被限制留言。";
                log_dim = "IP";
                break;
            case BlockReason::KEYWORD:
                user_msg = "[安全拦截]: 留言包含违禁内容，已被拦截，请修改后重试。";
                log_dim = "关键词";
                break;
            default:
                user_msg = "[安全拦截]: 留言因违规已被拦截。";
                log_dim = "未知";
                break;
        }

        json err;
        err["result"] = "ERROR";
        err["msg"] = user_msg;
        ctx.body = err.dump();

        LOG_WARN << "🛡️ [黑名单拦截] 维度: " << log_dim
             << " | 命中规则: " << verdict.detail
             << " | 昵称: " << nick
             << " | 来源 IP: " << (ctx.client_ip.empty() ? "unknown" : ctx.client_ip);
        return;
    }

    // 写入留言环形缓冲区
    CommentRepository::PushComment(nick, text, ctx.client_ip);
    json ok; 
    ok["result"] = "SUCCESS"; 
    ok["msg"] = "写入成功";
    ctx.body = ok.dump();
}

// 分支 C：拉取留言列表（GET 读取）
static void HandleGetComments(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
    
    // 从留言缓冲区获取留言
    auto comments = CommentRepository::GetLatestComments();
    json arr = json::array();
    for (const auto& c : comments) {
        json item;
        item["nick"] = c.first;
        item["text"] = c.second;
        arr.push_back(item);
    }
    ctx.body = arr.dump();
}

// 分支 D: 压力测试接口
static void HandleStress(RouteContext& ctx) {
    ctx.content_type = "Content-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\n"; 
    json ok; 
    ok["status"] = "ok"; 
    ok["msg"] = "Boom! C++ Reactor Core Handled This Successfully.";
    ctx.body = ok.dump();
}

// 分支E: Agent 测试数据获取
static void HandleAgentTelemetry(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
    
    uint64_t current_reqs = g_global_request_count.load(std::memory_order_relaxed);
    
    uint64_t current_seq;
    std::vector<CommentSnapshot> snapshots;
    CommentRepository::GetTelemetrySnapshot(current_seq, snapshots);

    json root;
    root["metrics"]["total_requests"] = current_reqs;
    root["metrics"]["current_sequence"] = current_seq;
    
    json comments_arr = json::array();
    for (const auto& c : snapshots) {
        json item;
        item["seq"] = c.sequence;
        item["nick"] = c.nickname;
        item["text"] = c.content;
        item["ip"] = c.ip;
        comments_arr.push_back(item);
    }
    root["comments"] = comments_arr;
    ctx.body = root.dump();
}

// 分支F: Agent 反向控制
static void HandleAgentControl(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
    std::string action_taken = "NONE";

    try {
        json payload = json::parse(ctx.req.body);

        // 模式 A: 一键全站紧急降级冷却
        if (payload.contains("cooldown") && payload["cooldown"].is_boolean()) {
            bool is_cooldown = payload["cooldown"].get<bool>();
            g_agent_cooldown_mode.store(is_cooldown, std::memory_order_release);
            action_taken = is_cooldown ? "SYSTEM_COOLDOWN_ON" : "SYSTEM_COOLDOWN_OFF";
            if (is_cooldown) LOG_WARN << "🚨 [AI Agent]: 全站紧急冷却模式已激活！";
            else LOG_INFO << "🟢 [AI Agent]: 紧急冷却解封，恢复常态。";
        }
        // 模式 B: 批量/单点拦截违规留言
        else {
            std::vector<uint64_t> target_sequences;
            if (payload.contains("block_seqs") && payload["block_seqs"].is_array()) {
                for (const auto& item : payload["block_seqs"]) {
                    if (item.is_number_unsigned()) {
                        target_sequences.push_back(item.get<uint64_t>());
                    }
                }
            } else if (payload.contains("block_seq") && payload["block_seq"].is_number()) {
                target_sequences.push_back(payload["block_seq"].get<uint64_t>());
            }

            if (!target_sequences.empty()) {
                int success_count = 0;
                std::vector<std::string> banned_nicks;
                std::vector<std::string> banned_ips;
                for (uint64_t seq : target_sequences) {
                    std::string banned_nick, banned_ip;
                    if (CommentRepository::EraseComment(seq, banned_nick, banned_ip)) {
                        success_count++;
                        // 根据昵称 + IP 拉黑
                        if (!banned_nick.empty() && banned_nick != "匿名极客") {
                             banned_nicks.push_back(banned_nick);
                        }
                        if (!banned_ip.empty()) {
                            banned_ips.push_back(banned_ip);
                        }
                        LOG_WARN << "🛡️ [AI Agent]: 成功对 seq:" << seq << " 执行原子擦除。";
                    }
                }
                 // 整批擦除完成后，一次性写锁 + 一次性回写黑名单文件
                BlacklistManager::AddBatch(banned_nicks, banned_ips);
                action_taken = "BATCH_ERASED_" + std::to_string(success_count);
            } else {
                action_taken = "NO_VALID_TARGETS";
            }
        }
    } catch (const json::parse_error& e) {
        // 降级保底, 纯文本协议兼容
        std::string cmd = ctx.req.body;
        if (cmd.rfind("block_seq:", 0) == 0 || cmd.rfind("block_seq=", 0) == 0) {
            try {
                uint64_t seq = std::stoull(cmd.substr(10));
                std::string banned_nick, banned_ip;
                if (CommentRepository::EraseComment(seq, banned_nick, banned_ip)) {
                    // 单条擦除同样走批量接口，保证「一次写锁 + 一次回写」的统一语义
                    std::vector<std::string> nk, ip_vec;
                    if (!banned_nick.empty() && banned_nick != "匿名极客") nk.push_back(banned_nick);
                    if (!banned_ip.empty()) ip_vec.push_back(banned_ip);
                    BlacklistManager::AddBatch(nk, ip_vec);
                    action_taken = "LEGACY_COMMENT_ERASED";
                }
                else action_taken = "COMMENT_VERSION_MISMATCH";
            } catch (...) { action_taken = "PARSE_ERROR"; }
        } else {
            action_taken = "PARSE_ERROR";
            LOG_ERROR << "❌ [HttpDispatcher]: 非法协议包, Payload: " << ctx.req.body;
        }
    }
    // 组装 body
    json resp;
    resp["status"] = (action_taken == "PARSE_ERROR") ? "ERROR" : "SUCCESS";
    resp["action"] = action_taken;
    ctx.body = resp.dump();
}

// 分支 G：万能静态文件托管引擎
static void HandleStaticFile(RouteContext& ctx) {
    if (ctx.req.path.find("..") != std::string::npos) {
        ctx.status_line = "HTTP/1.1 403 Forbidden\r\n";
        ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
        ctx.body = "{\"error\":\"[网关物理拦截]: 检测到非法路径穿越尝试！已将您的 IP 记录在案。\"}";
        LOG_WARN << "🚨 [安全拦截]: 拒绝访问非法路径 -> " << ctx.req.path;
        return;
    }

    std::string target_path = "./www" + (ctx.req.path == "/" ? "/index.html" : ctx.req.path);
    std::ifstream file(target_path, std::ios::binary);
    
    if (file.is_open()) {
        std::stringstream ss; ss << file.rdbuf();
        ctx.body = ss.str();
        ctx.content_type = "Content-Type: " + GetMimeType(target_path) + "\r\n";
    } else {
        ctx.status_line = "HTTP/1.1 404 Not Found\r\n";
        ctx.content_type = "Content-Type: text/html\r\n";
        ctx.body = "<html><head><title>404</title></head>"
            "<body style='background:#111; color:#ff3333; font-family:monospace; padding:50px;'>"
            "<h1>🚨 [Mini-Reactor 报错]: 404 资源未找到！</h1>"
            "<p>物理磁盘路径不存在: " + target_path + "</p>"
            "</body></html>";
    }
}

// 分支 H：管理员动态热加载黑名单
static void HandleAdminReloadBlacklist(RouteContext& ctx) {
    ctx.content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

    // 要求 HTTP Header 中必须有 Authorization 字段，且内容匹配
    auto it = ctx.req.headers.find("authorization");
    if (it == ctx.req.headers.end() || it->second != "Bearer MY_SUPER_SECRET_TOKEN_2026") {
        ctx.status_line = "HTTP/1.1 401 Unauthorized\r\n";
        json err;
        err["status"] = "ERROR";
        err["msg"] = "抱歉，非管理用户无法修改黑名单! ";
        ctx.body = err.dump();
        LOG_WARN << "🚨 拦截到一次非法的热加载尝试，IP 已记录。";
        return;
    }

    BlacklistManager::Reload();
    json ok;
    ok["status"] = "SUCCESS";
    ok["msg"] = "黑名单规则已成功热加载至内存！";
    ctx.body = ok.dump();
}

// ==========================================
// 核心分发总线入口 (Dispatcher Router)
// ==========================================
void HttpDispatcher::Dispatch(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                               const reactor::http::HttpRequest& req)
{
    g_global_request_count.fetch_add(1, std::memory_order_relaxed);
    LOG_INFO << "💡 [HttpDispatcher]: 路由分发 -> Method: " << req.method << " | Path: " << req.path;

    // 给定默认
    std::string status_line = "HTTP/1.1 200 OK\r\n";
    std::string content_type = "Content-Type: text/html; charset=utf-8\r\n";
    std::string body;
    std::string client_ip = conn->GetPeerIp();
    RouteContext ctx{req, status_line, content_type, body, client_ip};

    // O(1) 静态路由挂载表
    static const std::unordered_map<std::string, RouteHandler> router = {
        {"GET:/api/monitor", HandleMonitor},
        {"POST:/api/comment", HandlePostComment},
        {"GET:/api/comment", HandleGetComments},
        {"GET:/api/stress", HandleStress},
        {"GET:/api/agent/telemetry", HandleAgentTelemetry},
        {"POST:/api/agent/control", HandleAgentControl},
        {"POST:/api/admin/reload_blacklist", HandleAdminReloadBlacklist}
    };

    std::string route_key = req.method + ":" + req.path;
    // 精确匹配 O(1) 分发
    if (router.count(route_key)) {
        router.at(route_key)(ctx);
    } 
    // 兼容空方法的 GET 请求（底层编解码器若无 Method 字段时的容错）
    else if (router.count("GET:" + req.path)) {
        router.at("GET:" + req.path)(ctx);
    } 
    // 兜底进入静态资源托管
    else {
        HandleStaticFile(ctx);
    }

    // 拼装标准的物理 HTTP 响应帧
    std::stringstream response_ss;
    response_ss << ctx.status_line 
                << ctx.content_type
                << "Content-Length: " << ctx.body.size() << "\r\n"
                << "Connection: keep-alive\r\n\r\n" 
                << ctx.body << "\r\n";
                
    conn->Send(response_ss.str());

}

}// namespace app