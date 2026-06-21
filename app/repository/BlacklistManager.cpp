#include <fstream>
#include <iostream>
#include <filesystem>

#include "BlacklistManager.h"
#include "reactor/log/Logger.h"

namespace app
{
// 静态成员变量显示初始化
std::unordered_set<std::string> BlacklistManager::m_blacklist;
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
        // 顺手写入一行友好的注释（打开文件时就知道怎么填了）
        new_file << "# 请在此处添加黑名单用户昵称，每行一个\n";
        new_file.close();
    }

    Reload();
    LOG_INFO << "🛡️ [BlacklistManager]: 初始化彻底完成，守护服务已就绪。";
    return true;
}

bool BlacklistManager::IsBlackListed(const std::string& nickname)
{
    std::shared_lock<std::shared_mutex> read_lock(m_rw_mtx);
    return m_blacklist.find(nickname) != m_blacklist.end();
}

void BlacklistManager::Reload()
{
    std::unordered_set<std::string> new_blacklist = FetchLatestBlackList();

    {
        std::unique_lock<std::shared_mutex> write_lock(m_rw_mtx);
        std::swap(m_blacklist, new_blacklist);
    }
    LOG_INFO << "🔄 [BlacklistManager]: 黑名单热加载完毕，最新拦截规模: " << GetSize() << " 条";
}

size_t BlacklistManager::GetSize()
{
    std::shared_lock<std::shared_mutex> read_lock(m_rw_mtx);
    return m_blacklist.size();
}

// ==========================================
// 底层 I/O 隔离实现
// ==========================================

std::unordered_set<std::string> BlacklistManager::FetchLatestBlackList()
{
    std::unordered_set<std::string> black_data;
    std::ifstream file(m_source);

    if (!file.is_open()) {
        LOG_WARN << "⚠️ [BlacklistManager]: 未找到黑名单配置 (" << m_source << ")。将以【0拦截】状态放行所有请求。";
        return black_data;
    }

    std::string line;
    while (std::getline(file, line)) {
        // 处理可能的\r\n
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        // 跳过空行和 # 开头注释行
        if (!line.empty() && line[0] != '#') {
            black_data.insert(line);
        }
    }
    return black_data;
}

} // namespace app