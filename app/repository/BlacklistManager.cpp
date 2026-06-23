#include <fstream>
#include <iostream>
#include <filesystem>
#include <mutex>
#include <algorithm>
#include <cctype>

#include "BlacklistManager.h"
#include "reactor/log/Logger.h"

namespace app
{
// ==========================================
// 匿名命名空间：纯函数工具（不对外泄露符号）
// ==========================================
namespace {
    // 仅对 ASCII 字节做小写化；>= 0x80 的多字节（UTF-8 中文等）原样保留，避免破坏编码
    std::string ToLower(const std::string& s) {
        std::string out = s;
        std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
            return (c < 0x80) ? static_cast<char>(std::tolower(c)) : static_cast<char>(c);
        });
        return out;
    }

    // 去掉首位空白
    std::string Trim(const std::string& s) {
        size_t b = s.find_first_not_of(" \t");
        if (b == std::string::npos) return "";
        size_t e = s.find_last_not_of(" \t");
        return s.substr(b, e - b + 1);
    }
}

// 静态成员变量显示初始化
std::unordered_set<std::string> BlacklistManager::m_nick_blacklist;
std::unordered_set<std::string> BlacklistManager::m_ip_blacklist;
std::vector<std::string> BlacklistManager::m_keyword_blacklist;
std::shared_mutex BlacklistManager::m_rw_mtx;
std::string BlacklistManager::m_source;

// ==========================================
// 核心业务逻辑
// ==========================================

// 初始化
bool BlacklistManager::Initialize(const std::string& config_source) 
{
    m_source = config_source;
    std::error_code ec;

    if (!std::filesystem::exists(m_source, ec)) {
        LOG_WARN << "⚠️ [BlacklistManager]: 黑名单文件不存在，正在尝试自动创建: " << m_source;

        // 创建新文件
        std::ofstream new_file(m_source);
        if (!new_file.is_open()) {
            // 如果是目录权限不足等原因导致无法创建，直接宣告初始化失败
            LOG_ERROR << "❌ [BlacklistManager]: 无法创建黑名单文件！请检查目录读写权限。";
            return false;
        }
        // 写入分区模板，打开文件即知道怎么填
        new_file << "# 留言系统黑名单配置（单文件分区格式）\n";
        new_file << "# 用 [nick] / [ip] / [kw] 切换维度，每行一个条目，# 开头为注释\n";
        new_file << "[nick]\n";
        new_file << "[ip]\n";
        new_file << "[kw]\n";
        new_file.close();
    }

    Reload();
    LOG_INFO << "🛡️ [BlacklistManager]: 初始化彻底完成，守护服务已就绪。";
    return true;
}

bool BlacklistManager::IsBlackListed(const std::string& nickname)
{
    std::shared_lock<std::shared_mutex> read_lock(m_rw_mtx);
    return m_nick_blacklist.find(nickname) != m_nick_blacklist.end();
}

BlockResult BlacklistManager::CheckComment(const std::string& nickname,
                                           const std::string& ip,
                                           const std::string& content)
{
    std::shared_lock<std::shared_mutex> read_lock(m_rw_mtx);

    // 1. 昵称
    if (m_nick_blacklist.find(nickname) != m_nick_blacklist.end()) {
        return {BlockReason::NICKNAME, nickname};
    }
    // 2) IP：精确匹配
    if (!ip.empty() && m_ip_blacklist.find(ip) != m_ip_blacklist.end()) {
        return {BlockReason::IP, ip};
    }
    // 3) 关键词：大小写不敏感的子串匹配，返回首个命中的关键词
    if (!m_keyword_blacklist.empty()) {
        const std::string lower_content = ToLower(content);
        for (const auto& kw : m_keyword_blacklist) {
            if (!kw.empty() && lower_content.find(kw) != std::string::npos) {
                return {BlockReason::KEYWORD, kw};
            }
        }
    }
    return {BlockReason::NONE, ""};
}

void BlacklistManager::Reload()
{
    BlackListData fresh = FetchLatestBlackList();

    size_t nick_n = 0, ip_n = 0, kw_n = 0;
    {
        std::unique_lock<std::shared_mutex> write_lock(m_rw_mtx);
        std::swap(m_nick_blacklist, fresh.nicks);
        std::swap(m_ip_blacklist, fresh.ips);
        std::swap(m_keyword_blacklist, fresh.keywords);
        nick_n = m_nick_blacklist.size();
        ip_n = m_ip_blacklist.size();
        kw_n = m_keyword_blacklist.size();
    }
    LOG_INFO << "🔄 [BlacklistManager]: 黑名单热加载完毕 → 昵称 " << nick_n
             << " | IP " << ip_n << " | 关键词 " << kw_n << " 条";
}

// ==========================================
// 运行时动态拉黑（写接口）
// ==========================================
void BlacklistManager::AddNickname(const std::string& nickname) 
{
    AddBatch({nickname}, {});
}

void BlacklistManager::AddIp(const std::string& ip)
{
    AddBatch({}, {ip});
}

void BlacklistManager::AddKeyword(const std::string& keyword)
{
    if (keyword.empty()) return;
    const std::string kw = ToLower(keyword);
    std::unique_lock<std::shared_mutex> write_lock(m_rw_mtx);
    if (std::find(m_keyword_blacklist.begin(), m_keyword_blacklist.end(), kw) == m_keyword_blacklist.end()) {
        m_keyword_blacklist.push_back(kw);
        PersistToFile();
        LOG_INFO << "🛡️ [BlacklistManager]: 已动态拉黑关键词: " << kw;
    }
}

size_t BlacklistManager::AddBatch(const std::vector<std::string>& nicknames,
                                  const std::vector<std::string>& ips)
{
    // 整批拉黑只加一次写锁，循环插入完成后仅在确有新增时回写一次文件，
    // 把原本 O(N) 次磁盘重写压缩为 1 次，避免批量拦截时的 I/O 风暴。
    std::unique_lock<std::shared_mutex> write_lock(m_rw_mtx);
    size_t added = 0;

    for (const auto& n : nicknames) {
        if (n.empty()) continue;
        if (m_nick_blacklist.insert(n).second) {
            ++added;
            LOG_INFO << "🛡️ [BlacklistManager]: 已动态拉黑昵称: " << n;
        }
    }
    for (const auto& ip : ips) {
        if (ip.empty()) continue;
        if (m_ip_blacklist.insert(ip).second) {
            ++added;
            LOG_INFO << "🛡️ [BlacklistManager]: 已动态拉黑 IP: " << ip;
        }
    }

    // 仅当确有新增时才触碰磁盘；整批共享这一次回写（PersistToFile 要求调用者已持写锁，此处正好满足）
    if (added > 0) {
        PersistToFile();
        LOG_INFO << "🛡️ [BlacklistManager]: 批量拉黑落盘完成，本次新增 " << added << " 条规则。";
    }
    return added;
}

size_t BlacklistManager::GetSize()
{
    std::shared_lock<std::shared_mutex> read_lock(m_rw_mtx);
    return m_nick_blacklist.size() + m_ip_blacklist.size() + m_keyword_blacklist.size();
}

// ==========================================
// 底层 I/O 隔离实现
// ==========================================

BlacklistManager::BlackListData BlacklistManager::FetchLatestBlackList()
{
    BlackListData data;
    std::ifstream file(m_source);

    if (!file.is_open()) {
        LOG_WARN << "⚠️ [BlacklistManager]: 未找到黑名单配置 (" << m_source << ")。将以【0拦截】状态放行所有请求。";
        return data;
    }

    BlockReason cur = BlockReason::NONE;
    std::string line;
    while (std::getline(file, line)) {
        // 处理可能的\r\n
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        line = Trim(line);

        // 跳过空行和 # 开头注释行
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // 分区: [nick] / [ip] / [keyword]
        if (line.front() == '[' && line.back() == ']') {
            const std::string tag = line.substr(1, line.size() - 2);
            if (tag == "nick") cur = BlockReason::NICKNAME;
            else if (tag == "ip") cur = BlockReason::IP;
            else if (tag == "kw") cur = BlockReason::KEYWORD;
            else cur = BlockReason::NONE;
            continue;
        }

        switch (cur) {
            case BlockReason::NICKNAME: data.nicks.insert(line); break;
            case BlockReason::IP:       data.ips.insert(line); break;
            case BlockReason::KEYWORD:  data.keywords.push_back(ToLower(line)); break;
            case BlockReason::NONE:     break;
        }
    }
    return data;
}

void BlacklistManager::PersistToFile() 
{
    // 调用者必须已持有 m_rw_mtx 写锁。低频动作（管理 / Agent 触发），直接全量重写最简单且无竞态。
    std::ofstream out(m_source, std::ios::trunc);
    if (!out.is_open()) {
        LOG_ERROR << "❌ [BlacklistManager]: 无法回写黑名单文件，本次拉黑仅在内存生效: " << m_source;
        return;
    }
    out << "# 留言系统黑名单配置（单文件分区格式，由系统自动维护）\n";
    out << "# 用 [nick] / [ip] / [kw] 切换维度，每行一个条目，# 开头为注释\n";
    out << "[nick]\n";
    for (const auto& n : m_nick_blacklist) out << n << "\n";
    out << "[ip]\n";
    for (const auto& ip : m_ip_blacklist) out << ip << "\n";
    out << "[kw]\n";
    for (const auto& kw : m_keyword_blacklist) out << kw << "\n";
}

} // namespace app