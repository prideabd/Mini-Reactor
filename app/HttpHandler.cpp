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
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <vector>
#include <array>
#include <string>

#include "nlohmann/json.hpp" // 第三库函数

#include "HttpHandler.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/http/HttpCodec.h"
#include "reactor/log/Logger.h"

namespace app {

using json = nlohmann::json;

// 全局变量初始化，必须在.cpp中初始化
std::atomic<uint64_t> g_global_request_count{0};
constexpr size_t MAX_MEM_COMMENTS = 50;

struct MemoryComment {
    std::string nickname;
    std::string content;
    uint64_t sequence{0};
};

// 预分配定长静态内存矩阵
std::array<MemoryComment, MAX_MEM_COMMENTS> g_comment_ring_buffer;
std::atomic<uint64_t> g_comment_sequence{0};

// agent 控制原子变量
std::atomic<bool> g_agent_cooldown_mode{false};
std::atomic<bool> g_agent_filter_mode{false};

// 往纯内存中原子无锁写入一条 昵称-评论
void PushMemoryComment(const std::string& nickname, const std::string& content) {
    // 1. 原子抢占槽位（多线程在此处各奔东西，分流到不同槽位，完全无锁）
    uint64_t seq = g_comment_sequence.fetch_add(1, std::memory_order_relaxed);
    size_t index = seq % MAX_MEM_COMMENTS;
    // 2. 写入结构化数据到专属内存空间
    g_comment_ring_buffer[index].nickname = nickname;
    g_comment_ring_buffer[index].content = content;
    // 3. 释放屏障：更新版本戳（标记当前槽位写入就绪，其值为当前真实的全局 seq + 1）
    g_comment_ring_buffer[index].sequence = seq + 1;
}

// 纯内存高并发安全拉取最新的结构化留言列表
std::vector<std::pair<std::string, std::string>> GetMemoryComments() {
    std::vector<std::pair<std::string, std::string>> comments;
    uint64_t current_max_seq = g_comment_sequence.load(std::memory_order_relaxed);
    // 计算当前由于循环覆盖所产生的合法起始时间边界
    uint64_t start_seq = (current_max_seq > MAX_MEM_COMMENTS) ? (current_max_seq - MAX_MEM_COMMENTS) : 0;
    for (uint64_t i = start_seq; i < current_max_seq; ++i) {
        size_t index = i % MAX_MEM_COMMENTS;
        // 双重校验/乐观读锁：如果槽位的版本戳刚好等于 s + 1，说明该槽位已经写完且在此刻未发生写覆盖，数据安全
        if (g_comment_ring_buffer[index].sequence == i + 1) {
            comments.push_back(std::make_pair(g_comment_ring_buffer[index].nickname, g_comment_ring_buffer[index].content));
        }
    }
    return comments;
}

// 根据文件后缀名，自动匹配 HTTP 标准媒体类型（MIME Type）
std::string GetMimeType(const std::string& path) {
    static const std::unordered_map<std::string, std::string> mime_types = {
        {".html", "text/html; charset=utf-8"},
        {".css", "text/css; charset=utf-8"},
        {".js", "application/javascript; charset=utf-8"},
        {".png", "image/png"},
        {".jpg", "image/jpeg"},
        {".gif", "image/gif"},
        {".ico", "image/x-icon"},
        {".svg", "image/svg+xml"},
        {".json", "application/json; charset=utf-8"}
    };
    size_t dot_pos = path.find_last_of('.');
    if (dot_pos != std::string::npos) {
        std::string ext = path.substr(dot_pos);
        auto it = mime_types.find(ext);
        if (it != mime_types.end()) {
            return it->second;
        }
    }
    return "text/plain; charset=utf-8"; // 默认
}

// 抽取出的独立高可用 URL 物理解码函数
std::string UrlDecode(const std::string& str) {
    std::string result;
    result.reserve(str.size()); // 优化内存分配，榨干高并发下的吞吐性能
    
    for (size_t i = 0; i < str.length(); ++i) {
        if (str[i] == '+') {
            result += ' '; // 标准表单规范：将 '+' 还原为空格
        } else if (str[i] == '%' && i + 2 < str.length()) {
            // 物理把两个十六进制字符（如 E4）拼成一个真实字节
            char high = str[i + 1];
            char low = str[i + 2];
            
            auto HexToChar = [](char c) -> int {
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= '0' && c <= '9') return c - '0';
                return 0;
            };
            
            int byte_val = (HexToChar(high) << 4) + HexToChar(low);
            result += static_cast<char>(byte_val);
            i += 2; // 指针前移，跳过已经消费的两个十六进制位
        } else {
            result += str[i];
        }
    }
    return result;
}

void HandleHttpRequest(const std::shared_ptr<reactor::net::TcpConnection>& conn,
                       const reactor::http::HttpRequest& req)
{
    g_global_request_count.fetch_add(1, std::memory_order_relaxed);
    LOG_INFO << "💡 [HttpHandler]: 成功捕获请求！Method: " << req.method << " | Path: " << req.path;

    std::string body;
    std::string status_line = "HTTP/1.1 200 OK\r\n";
    std::string content_type = "text/html; charset=utf-8\r\n";
    std::string connection_header = "Connection: keep-alive\r\n"; // 默认全局长连接复用

    // ==========================================
    // 分支 A：硬核后端动态核心监控 API 接口
    // ==========================================
    if (req.path == "/api/monitor") {
        content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

        // 1. 计算服务器点火至今的运行时间
        static auto start_time = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        auto uptime_sec = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

        // 2. 模拟/读取底层多线程与高并发监控指标
        // 生产环境下可以从 TcpServer 的全局对象中引出 connections_.size()
        int live_connections = 1; 
        int active_threads = 3;   // SubReactors 数量

        // 增加对 linux 物理系统指标采集
        static double last_cpu = 5.0;
        static double last_mem = 78.4;
        static double last_io = 0.2;

        static int64_t last_req_snapshot = 0;
        int64_t current_req_snapshot = g_global_request_count.load(std::memory_order_relaxed);
        int64_t instant_qps = current_req_snapshot - last_req_snapshot;
        if (instant_qps < 0) {
            instant_qps = 0;
        }
        last_req_snapshot = current_req_snapshot;

        // 模拟计算，真实应该是读取 /proc
        // 根据当前的 QPS 压测猛烈程度，物理映射物理指标的飙升
        double target_cpu = 2.0 + (instant_qps / 1400.0) * 85.0; // QPS 达到 1400 时 CPU 飙升到 87%
        if (target_cpu > 98.0) target_cpu = 98.4;
        last_cpu = last_cpu * 0.4 + target_cpu * 0.6; // 平滑滤波

        double target_mem = 82.3 - (instant_qps / 1400.0) * 15.2; // 压测时剩余内存减少
        if (target_mem < 12.0) target_mem = 12.1;
        last_mem = last_mem * 0.7 + target_mem * 0.3;

        double target_io = 0.1 + (instant_qps / 1400.0) * 45.8; // 磁盘 I/O 吞吐随压测猛烈爆发 (MB/s)
        last_io = last_io * 0.3 + target_io * 0.7;

        // 3. 使用 nlohmann/json 优雅组装监控指标
        json resp;
        resp["status"] = "RUNNING";
        resp["uptime_seconds"] = uptime_sec;
        resp["live_connections"] = live_connections;
        resp["sub_reactors"] = active_threads;
        resp["total_requests"] = g_global_request_count.load(std::memory_order_relaxed);
        
        // nlohmann 支持直接对浮点数进行格式化映射，无需 std::setprecision
        resp["cpu_usage"] = std::round(last_cpu * 10.0) / 10.0;
        resp["mem_available"] = std::round(last_mem * 10.0) / 10.0;
        resp["disk_io"] = std::round(last_io * 10.0) / 10.0;
        
        resp["cooldown_mode"] = g_agent_cooldown_mode.load(std::memory_order_relaxed);
        resp["engine"] = "Mini-Reactor-v2.0";

        body = resp.dump(); // 极速序列化输出
    } 
    // ==========================================
    // 分支 B：提交留言（POST 写入）- 熔断修复
    // ==========================================
    else if (req.path == "/api/comment" && req.method == "POST") {
        content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

        // 安全审计：前端表单格式校验
        if (req.headers.find("content-type") == req.headers.end() || 
            req.headers.at("content-type").find("application/x-www-form-urlencoded") == std::string::npos) {
            status_line = "HTTP/1.1 415 Unsupported Media Type\r\n";
            json err; 
            err["result"] = "ERROR"; 
            err["msg"] = "[网关拒绝]: 留言投递只认 urlencoded 标准表单格式！";
            body = err.dump();
        }
        // 检查原子变量熔断
        else if (g_agent_cooldown_mode.load(std::memory_order_acquire)) {
            status_line = "HTTP/1.1 200 OK\r\n"; 
            json err;
            err["result"] = "ERROR"; 
            err["msg"] = "[AI 熔断保护中] 服务器当前遭遇高并发冲击，留言板已进入紧急安全冷却模式。";
            body = err.dump();
        }
        else {
            // 1. 正常提取表单数据进行无锁写入
            std::string raw_body = req.body;
            std::string nick = "匿名极客";
            std::string text = raw_body;
            if (!raw_body.empty()) {
                std::string key_value;
                std::stringstream ss(raw_body);
                while (std::getline(ss, key_value, '&')) {
                    size_t pos = key_value.find('=');
                    if (pos != std::string::npos) {
                        std::string key = key_value.substr(0, pos);
                        std::string value = key_value.substr(pos + 1);
                        std::string decode_value = UrlDecode(value);
                        if (key == "name") nick = decode_value;
                        else if (key == "content") text = decode_value;
                    }
                }
            }
            PushMemoryComment(nick, text);
            json ok; 
            ok["result"] = "SUCCESS"; 
            ok["msg"] = "纯内存原子抢占成功";
            body = ok.dump();
        }
    }
    // ==========================================
    // 分支 C：拉取留言列表（GET 读取）
    // ==========================================
    else if (req.path == "/api/comment" && (req.method == "GET" || req.method.empty())) {
        content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
        
        // 1. 从内存抠出最新留言
        auto comments = GetMemoryComments();

        // 2. 利用 json::array() 优雅生成 JSON 数组，彻底告别拼逗号的烦恼
        json arr = json::array();
        for (const auto& c : comments) {
            json item;
            item["nick"] = c.first;   // 自动转义双引号和特殊字符
            item["text"] = c.second;  // 自动转义双引号和特殊字符
            arr.push_back(item);
        }
        body = arr.dump();
    }
    // ==========================================
    // 分支 D：压力测试高速接口
    // ==========================================
    else if (req.path == "/api/stress" && (req.method == "GET" || req.method.empty())) {
        content_type = "Content-Type: application/json\r\n";
        // 显式注入允许跨域，方便客户端多路复用
        content_type += "Access-Control-Allow-Origin: *\r\n"; 
        json ok; 
        ok["status"] = "ok"; 
        ok["msg"] = "Boom! C++ Reactor Core Handled This Successfully.";
        body = ok.dump();
    }
    // ==========================================
    // 分支 E：Agent 专属全测序数据上报
    // ==========================================
    else if (req.path == "/api/agent/telemetry") {
        content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
        
        // 1. 获取当前性能快照
        uint64_t current_reqs = g_global_request_count.load(std::memory_order_relaxed);
        // 2. 获取当前环形原子变量的最新状态
        uint64_t current_seq = g_comment_sequence.load(std::memory_order_relaxed);
        // 3. 计算最新50条留言的起点
        uint64_t start_seq = (current_seq > MAX_MEM_COMMENTS) ? (current_seq - MAX_MEM_COMMENTS) : 0;

        json root;
        root["metrics"]["total_requests"] = current_reqs;
        root["metrics"]["current_sequence"] = current_seq;
        json comments_arr = json::array();
        // 4. 标准无锁乐观锁遍历，把新鲜数据拼给大模型
        for (uint64_t s = start_seq; s < current_seq; ++s) {
            size_t index = s % MAX_MEM_COMMENTS; //
            
            // 乐观锁身份与完整性对账：写完了且没被改动才上报
            if (g_comment_ring_buffer[index].sequence == s + 1) {
                json item;
                item["seq"] = s;
                item["nick"] = g_comment_ring_buffer[index].nickname;
                item["text"] = g_comment_ring_buffer[index].content;
                comments_arr.push_back(item);
            }
        }
        root["comments"] = comments_arr;
        body = root.dump();
    }
    // ==========================================
    // 分支 F：agent 反向控制
    // ==========================================
    else if (req.path == "/api/agent/control" && req.method == "POST") {
        content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";

        // 1. 解析 Agent 发过来的原始控制指令
        // 期望格式样例：cooldown:true 或 block_seq:[10,20,..]
        std::string action_taken = "NONE";
        
        try {
            json payload = json::parse(req.body);
            // 模式 A：一键全站紧急降级冷却
            if (payload.contains("cooldown") && payload["cooldown"].is_boolean()) {
                bool is_cooldown = payload["cooldown"].get<bool>();
                g_agent_cooldown_mode.store(is_cooldown, std::memory_order_relaxed);
                action_taken = is_cooldown ? "SYSTEM_COOLDOWN_ACTIVATED" : "SYSTEM_COOLDOWN_DEACTIVATED";
                if (is_cooldown) LOG_WARN << "🚨 [AI Agent]: 全站紧急冷却模式已激活！";
                else LOG_INFO << "🟢 [AI Agent]: 紧急冷却解封，恢复常态。";
            }
            // 模式 B: 批量拦截违规留言
            else {
                std::vector<uint64_t> targets;
                // 兼容最新版的数组批量发送格式: {"block_seqs": [10, 15]}
                if (payload.contains("block_seqs") && payload["block_seqs"].is_array()) {
                    for (const auto& item : payload["block_seqs"]) {
                        // 强校验：不仅要是数字，还必须能无损地塞进 uint64_t（防止大数溢出截断）
                        if (item.is_number_unsigned()) { 
                            uint64_t raw_val = item.get<uint64_t>();
                            
                            // 可选高危防御：如果数字大得离谱（比如大于当前系统产生过的最大 sequence），直接判定非法
                            if (raw_val < g_comment_sequence.load(std::memory_order_relaxed) + 100) {
                                targets.push_back(raw_val);
                            } else {
                                LOG_WARN << "⚠️ [AI Agent Control]: 拒绝了超大幻觉序列号: " << raw_val;
                            }
                        }
                    }
                }
                // 兼容单数发送格式: {"block_seq": 10}
                else if (payload.contains("block_seq") && payload["block_seq"].is_number()) {
                    targets.push_back(payload["block_seq"].get<uint64_t>());
                }

                if (!targets.empty()) {
                    int success_count = 0;
                    for (uint64_t seq : targets) {
                        size_t index = seq % MAX_MEM_COMMENTS;
                        if (g_comment_ring_buffer[index].sequence == seq + 1) {
                            g_comment_ring_buffer[index].nickname = "[AI 已拦截]";
                            g_comment_ring_buffer[index].content = "⚠️ 此条留言因违规已被 AI Agent 执行无锁原子抹除。";
                            success_count++;
                            LOG_WARN << "🛡️ [AI Agent]: 成功对 seq:" << seq << " 执行原子擦除。";
                        }
                    }
                    action_taken = "BATCH_ERASED_" + std::to_string(success_count);
                } else {
                    action_taken = "NO_VALID_TARGETS";
                }
            }
        } catch (const json::parse_error& e) {
            // 降级兜底：如果 Agent 没发纯正 JSON，尝试旧版的纯文本提取兼容 (比如 "block_seq:30")
            // 保证系统可用性，防止 Agent 断连
            std::string cmd = req.body;
            if (cmd.rfind("block_seq:", 0) == 0 || cmd.rfind("block_seq=", 0) == 0) {
                try {
                    uint64_t seq = std::stoull(cmd.substr(10));
                    size_t index = seq % MAX_MEM_COMMENTS;
                    if (g_comment_ring_buffer[index].sequence == seq + 1) {
                        g_comment_ring_buffer[index].nickname = "[AI 已拦截]";
                        g_comment_ring_buffer[index].content = "⚠️ [旧版协议] 已抹除。";
                        action_taken = "LEGACY_COMMENT_ERASED";
                    } else action_taken = "COMMENT_VERSION_MISMATCH";
                } catch (...) { action_taken = "PARSE_ERROR"; }
            } else {
                action_taken = "PARSE_ERROR";
                LOG_ERROR << "❌ [HttpHandler]: 非法协议包, Payload: " << req.body;
            }
        }

        // 2. 回喷标准的执行状态确认帧给 Agent
        json resp;
        resp["status"] = (action_taken == "PARSE_ERROR") ? "ERROR" : "SUCCESS";
        resp["action"] = action_taken;
        body = resp.dump();
    }
    // ==========================================
    // 分支 G：万能静态文件托管引擎（磁盘文件映射）
    // ==========================================
    else {
        // 物理阻断路径穿越攻击
        if (req.path.find("..") != std::string::npos) {
            status_line = "HTTP/1.1 403 Forbidden\r\n";
            content_type = "Content-Type: " + GetMimeType(".json") + "\r\n";
            body = "{\"error\":\"[网关物理拦截]: 检测到非法路径穿越尝试！已将您的 IP 记录在案。\"}";
            LOG_WARN << "🚨 [安全拦截]: 拒绝访问非法路径 -> " << req.path;
        } else {
            // 1. 定位物理文件路径，默认根路由指向 index.html
            std::string target_path = "./www" + req.path;
            if (req.path == "/") {
                target_path = "./www/index.html";
            }

            // 2. 以二进制流的形式跨越硬件磁盘读取资源
            std::ifstream file(target_path, std::ios::binary);
            if (file.is_open()) {
                std::stringstream file_ss;
                file_ss << file.rdbuf();
                body = file_ss.str();
                file.close();

                // 3. 动态识别文件后缀，精确判定 Content-Type 保证网页皮肤和特效不丢失
                content_type = "Content-Type: " + GetMimeType(target_path) + "\r\n";
            } 
            // 4. 磁盘上挖不出这个文件 -> 优雅下行，降级回喷 404
            else {
                status_line = "HTTP/1.1 404 Not Found\r\n";
                content_type = "Content-Type: " + GetMimeType(".html") + "\r\n";
                body = "<html><head><title>404</title></head>"
                    "<body style='background:#111; color:#ff3333; font-family:monospace; padding:50px;'>"
                    "<h1>🚨 [Mini-Reactor 报错]: 404 资源未找到！</h1>"
                    "<p>物理磁盘路径不存在: " + target_path + "</p>"
                    "</body></html>";
            }
        }
    }

    // 拼装标准的物理 HTTP 响应帧
    std::stringstream response_ss;
    response_ss << status_line
                << content_type
                << "Content-Length: " << body.size() << "\r\n"
                << connection_header  // 规范注入 Keep-Alive
                << "\r\n"             // 切分 Header 与 Body 的核心空行
                << body << "\r\n";    // 🌟 加上标准尾部结束标记，彻底阻断 ab 提前闪退
    
    // 顺藤摸瓜，通过连接将拼装好的数据回喷给内核缓冲区
    conn->Send(response_ss.str());
}

} // namespace app