#pragma once

#include <string>
#include <vector>
#include <utility>
#include <cstdint>

namespace app {

// 留言结构快照
struct CommentSnapshot {
    uint64_t sequence;
    std::string nickname;
    std::string content;
};

/**
 * @brief 内存留言仓储引擎 (Repository)
 * 封装高并发下的无锁环形缓冲区(Ring Buffer)操作，对外屏蔽原子序列号与槽位覆盖细节。
 */
class CommentRepository {
public:
    // 写入 昵称-评论
    static void PushComment(const std::string& nickname, const std::string& content);

    // 前端网页：安全获取最新的留言
    static std::vector<std::pair<std::string, std::string>> GetLatestComments();

    // Agent: 获取完整留言结构体与当前最大 Sequence
    static void GetTelemetrySnapshot(uint64_t& out_current_seq, std::vector<CommentSnapshot>& out_comments);

    // Agent: 控制干预，删除指定 seq 的恶意留言
    static bool EraseComment(uint64_t seq);
};
}