#pragma once
#include <unordered_set>
#include <string>
#include <shared_mutex>

namespace app {

/** 
 * @brief 黑名单管理器（单例静态类）
 * @details
 * BlacklistManager 负责在高并发服务器环境下对用户昵称进行黑名单拦截。
 * 黑名单规则以文本文件形式持久化存储，每行一个昵称，支持 '#' 开头的注释行
*/

class BlacklistManager {
public:
    // 初始化
    static bool Initialize(const std::string& config_source);

    // 高频接口: 判断是否在黑名单中
    static bool IsBlackListed(const std::string& nickname);

    // 低频接口: 动态加载黑名单
    static void Reload();

    // 获取黑名单数量
    static size_t GetSize();

private:
    //  I/O读取黑名单文件函数
    static std::unordered_set<std::string> FetchLatestBlackList();

    static std::unordered_set<std::string> m_blacklist;
    static std::shared_mutex m_rw_mtx; // 读写锁
    static std::string m_source;  // 黑名单配置文件，默认放在./config下
};
} // namespace app