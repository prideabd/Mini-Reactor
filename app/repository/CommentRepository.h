#pragma once

#include <string>
#include <vector>
#include <utility>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>

struct sqlite3;
struct sqlite3_stmt;

namespace app {

// 留言结构快照
struct CommentSnapshot {
    uint64_t sequence;
    std::string nickname;
    std::string content;
    std::string ip;
};

/**
 * @brief 内存留言仓储引擎 (Repository)
 * 封装高并发下的无锁环形缓冲区(Ring Buffer)操作，对外屏蔽原子序列号与槽位覆盖细节。
 */
class CommentRepository {
public:
    // 初始化仓储：拉起数据库、加载历史数据回内存
    static bool Initialize(const std::string& db_path);

    // 优雅关闭仓储：安全停止后台线程
    static void Shutdown();

    // 写入 昵称-评论
    static void PushComment(const std::string& nickname, const std::string& content, const std::string& ip);

    // 前端网页：安全获取最新的留言
    static std::vector<std::pair<std::string, std::string>> GetLatestComments();

    // Agent: 获取完整留言结构体与当前最大 Sequence
    static void GetTelemetrySnapshot(uint64_t& out_current_seq, std::vector<CommentSnapshot>& out_comments);

    // Agent: 控制干预，删除指定 seq 的恶意留言
    static bool EraseComment(uint64_t seq, std::string& out_nick, std::string& out_ip);

private:
    // ==========================================
    // 异步 DB 写入引擎内部组件
    // ==========================================
    enum class DBAction { INSERT, UPDATE };
    struct DBTask {
        DBAction action;
        uint64_t sequence;
        std::string nickname;
        std::string content;
        std::string ip;
    };

    // 投递任务到后台队列
    static void EnqueueDBTask(DBTask&& task);
    // 后台线程工作循环
    static void DBWorkerLoop();
    // SQLite 内部操作封装
    static bool InitSqlite(const std::string& db_path);
    static bool ExecuteTaskToSqlite(const DBTask& task);

    // ==========================================
    // 异步控制成员变量
    // ==========================================
    static std::queue<DBTask> m_db_queue;
    static std::mutex m_queue_mtx;
    static std::condition_variable m_queue_cv;

    static std::thread m_worker_thread;
    static std::atomic<bool> m_stop_requested;

    // SQLite 句柄与预编译语句
    static sqlite3* m_db;
    static sqlite3_stmt* m_insert_stmt;
    static sqlite3_stmt* m_update_stmt;

};
}