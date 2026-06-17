#include <array>
#include <atomic>
#include <mutex>

#include "CommentRepository.h"

namespace app {

// ==========================================
// 仓储私有数据区 (匿名命名空间，杜绝全局符号泄露)
// ==========================================
namespace {
    constexpr size_t MAX_MEM_COMMENTS = 50;
    struct MemoryComment {
        std::string nickname;
        std::string content;
        std::atomic<uint64_t> sequence{0};
        std::mutex slot_mtx; // 自旋锁，防止多个线程同时修改
    };
    std::atomic<uint64_t> g_comment_sequence{0};
    std::array<MemoryComment, MAX_MEM_COMMENTS> g_comment_ring_buffer;
}

// ==========================================
// 仓储对外公开的业务接口实现
// ==========================================

void CommentRepository::PushComment(const std::string& nickname, const std::string& content) {
    uint64_t seq = g_comment_sequence.fetch_add(1, std::memory_order_relaxed);
    size_t index = seq % MAX_MEM_COMMENTS;

    std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);

    g_comment_ring_buffer[index].nickname = nickname;
    g_comment_ring_buffer[index].content = content;
    g_comment_ring_buffer[index].sequence.store(seq + 1, std::memory_order_release);
}

std::vector<std::pair<std::string, std::string>> CommentRepository::GetLatestComments() {
    std::vector<std::pair<std::string, std::string>> comments;
    uint64_t current_max_seq = g_comment_sequence.load(std::memory_order_relaxed);
    uint64_t start_seq = (current_max_seq > MAX_MEM_COMMENTS) ? (current_max_seq - MAX_MEM_COMMENTS) : 0;
    for (uint64_t i = start_seq; i < current_max_seq; ++i) {
        size_t index = i % MAX_MEM_COMMENTS;
        // 乐观锁
        std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);
        if (g_comment_ring_buffer[index].sequence.load(std::memory_order_acquire) == i + 1) {
            comments.emplace_back(g_comment_ring_buffer[index].nickname, g_comment_ring_buffer[index].content);
        }
    }
    return comments;
}

void CommentRepository::GetTelemetrySnapshot(uint64_t& out_current_seq, std::vector<CommentSnapshot>& out_comments) {
    out_current_seq = g_comment_sequence.load(std::memory_order_relaxed);
    uint64_t start_seq = (out_current_seq > MAX_MEM_COMMENTS) ? (out_current_seq - MAX_MEM_COMMENTS) : 0;

    for (uint64_t i = start_seq; i < out_current_seq; ++i) {
        size_t index = i % MAX_MEM_COMMENTS;
        std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);
        if (g_comment_ring_buffer[index].sequence.load(std::memory_order_acquire) == i + 1) {
            out_comments.push_back({
                i,
                g_comment_ring_buffer[index].nickname,
                g_comment_ring_buffer[index].content
            });
        }
    }
}

bool CommentRepository::EraseComment(uint64_t seq) {
    uint64_t index = seq % MAX_MEM_COMMENTS;
    std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);

    if (g_comment_ring_buffer[index].sequence.load(std::memory_order_acquire) == seq + 1) {
        g_comment_ring_buffer[index].nickname = "[AI 已拦截]";
        g_comment_ring_buffer[index].content = "⚠️ 此条留言因违规已被 AI Agent 执行无锁原子抹除。";
        return true;
    }
    return false;
}

} // namespace app