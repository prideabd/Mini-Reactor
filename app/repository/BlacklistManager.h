#pragma once
#include <unordered_set>
#include <string>
#include <shared_mutex>
#include <vector>

namespace app {

/**
 * @brief 黑名单管理器（单例静态类）
 * @details
 * BlacklistManager 负责在高并发服务器环境下对留言进行多维度黑名单拦截：
 *   - 昵称（nickname）：精确匹配
 *   - IP 地址（ip）：精确匹配
 *   - 关键词（keyword）：大小写不敏感的子串匹配
 * 规则以单文件分区格式持久化（[nick] / [ip] / [kw] 三段），支持 '#' 注释行；
 * 运行时可通过 AddXxx 接口动态拉黑并自动回写文件，重启 / Reload 后依然生效。
 */

// 维度标识（既用于校验命中结果，也用于配置解析的分区状态）
enum class BlockReason {
    NONE,      // 未命中，放行
    NICKNAME,  // 命中昵称黑名单
    IP,        // 命中 IP 黑名单
    KEYWORD    // 命中关键词黑名单
};

// 多维度检验结果结构体
struct BlockResult {
    BlockReason reason = BlockReason::NONE;
    std::string detail;
    bool blocked() const {
        return reason != BlockReason::NONE;
    }
};

class BlacklistManager {
public:
    // 初始化
    static bool Initialize(const std::string& config_source);

    // 高频接口: 仅判断昵称是否在黑名单中
    static bool IsBlackListed(const std::string& nickname);

    // 高频接口（多维度）：昵称 / IP / 关键词一并校验，返回首个命中的维度
    static BlockResult CheckComment(const std::string& nickname,
                                    const std::string& ip,
                                    const std::string& content);

    // 低频接口: 动态加载黑名单
    static void Reload();

    // 运行时动态拉黑（线程安全：更新内存 + 回写文件持久化；重复添加自动去重）
    static void AddNickname(const std::string& nickname);
    static void AddIp(const std::string& ip);
    static void AddKeyword(const std::string& keyword);
    // 批量动态拉黑（昵称 + IP）：全程只加一次写锁、只回写一次文件
    static size_t AddBatch(const std::vector<std::string>& nicknames,
                       const std::vector<std::string>& ips);

    // 获取黑名单数量
    static size_t GetSize();

private:
    // 一次 I/O 读出三类规则集合
    struct BlackListData {
        std::unordered_set<std::string> nicks;
        std::unordered_set<std::string> ips;
        std::vector<std::string> keywords;
    };
    //  I/O读取黑名单文件函数
    static BlackListData FetchLatestBlackList();

    // 将当前内存中的三类规则整体回写到文件（调用者必须已持有 m_rw_mtx 写锁）
    static void PersistToFile();

    static std::unordered_set<std::string> m_nick_blacklist;
    static std::unordered_set<std::string> m_ip_blacklist;
    static std::vector<std::string> m_keyword_blacklist;
    static std::shared_mutex m_rw_mtx; // 读写锁
    static std::string m_source;  // 黑名单配置文件，默认放在./config下
};
} // namespace app