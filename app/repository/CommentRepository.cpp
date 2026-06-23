#include <array>
#include <filesystem>

#include "sqlite/sqlite3.h"

#include "CommentRepository.h"
#include "reactor/log/Logger.h"

namespace app {

// ==========================================
// 仓储私有数据区 (匿名命名空间，杜绝全局符号泄露)
// ==========================================
namespace {
    constexpr size_t MAX_MEM_COMMENTS = 50;
    struct MemoryComment {
        std::string nickname;
        std::string content;
        std::string ip;
        std::atomic<uint64_t> sequence{0};
        std::mutex slot_mtx; // 互斥锁，防止多个线程同时修改
    };
    std::atomic<uint64_t> g_comment_sequence{0};
    std::array<MemoryComment, MAX_MEM_COMMENTS> g_comment_ring_buffer;
}

// ==========================================
// 静态成员变量显式初始化
// ==========================================
std::queue<CommentRepository::DBTask> CommentRepository::m_db_queue;
std::mutex CommentRepository::m_queue_mtx;
std::condition_variable CommentRepository::m_queue_cv;
std::thread CommentRepository::m_worker_thread;
std::atomic<bool> CommentRepository::m_stop_requested{false};

sqlite3* CommentRepository::m_db = nullptr;
sqlite3_stmt* CommentRepository::m_insert_stmt = nullptr;
sqlite3_stmt* CommentRepository::m_update_stmt = nullptr;

// ==========================================
// 仓储对外公开的业务接口实现
// ==========================================

bool CommentRepository::Initialize(const std::string& db_path) {
    m_stop_requested = false;
    // 1. 初始化 SQLits 环境并建表
    if (!InitSqlite(db_path)) {
        return false;
    }
    // 2. 从数据库恢复 MAX_MEM_COMMENTS 数据
    std::string select_sql = "SELECT sequence, nickname, content, ip FROM comments ORDER BY sequence DESC LIMIT ?;";
    sqlite3_stmt* select_stmt = nullptr;

    if (sqlite3_prepare_v2(m_db, select_sql.c_str(), -1, &select_stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_int64(select_stmt, 1, MAX_MEM_COMMENTS);

        // 时间正序 (Sequence从小到大）填入环形缓冲区
        std::vector<DBTask> history_data;
        uint64_t max_seq = 0;
        bool has_data = false;

        while (sqlite3_step(select_stmt) == SQLITE_ROW) {
            has_data = true;
            uint64_t seq = static_cast<uint64_t>(sqlite3_column_int64(select_stmt, 0));
            std::string nick = reinterpret_cast<const char*>(sqlite3_column_text(select_stmt, 1));
            std::string cont = reinterpret_cast<const char*>(sqlite3_column_text(select_stmt, 2));
            // ip 列可空（旧库迁移 / 匿名场景），NULL 时 sqlite3_column_text 返回 nullptr，需兜底
            const unsigned char* ip_text = sqlite3_column_text(select_stmt, 3);
            std::string ip = ip_text ? reinterpret_cast<const char*>(ip_text) : "";

            if (seq > max_seq) {
                max_seq = seq;
            }
            history_data.push_back({DBAction::INSERT, seq, nick, cont, ip});
        }
        sqlite3_finalize(select_stmt);

        // 恢复全局 sequence 计数器
        if (has_data) {
            g_comment_sequence.store(max_seq + 1, std::memory_order_release);
        }

        // 正序放入内存缓冲区
        for (auto it = history_data.rbegin(); it != history_data.rend(); ++it) {
            size_t index = it->sequence % MAX_MEM_COMMENTS;
            g_comment_ring_buffer[index].nickname = it->nickname;
            g_comment_ring_buffer[index].content = it->content;
            g_comment_ring_buffer[index].ip = it->ip;
            g_comment_ring_buffer[index].sequence.store(it->sequence + 1, std::memory_order_release);
        }
    }

    m_worker_thread = std::thread(&CommentRepository::DBWorkerLoop);
    return true;
}

void CommentRepository::Shutdown() {
     // 线程未启动则直接跳过，避免对未 init 的对象操作
    if (!m_worker_thread.joinable() && m_db == nullptr) {
        return;
    }
    // 停止信号并唤醒后台线程
    m_stop_requested = true;
    m_queue_cv.notify_one();

    // 等待后台线程将队列清空
    if (m_worker_thread.joinable()) {
        m_worker_thread.join();
    }

    // 释放 SQLite 预编译语句句柄与数据库连接
    if (m_insert_stmt) sqlite3_finalize(m_insert_stmt);
    if (m_update_stmt) sqlite3_finalize(m_update_stmt);
    if (m_db) sqlite3_close(m_db);

    m_insert_stmt = nullptr;
    m_update_stmt = nullptr;
    m_db = nullptr;
}

// ==========================================
// 业务公开接口实现
// ==========================================

void CommentRepository::PushComment(const std::string& nickname, const std::string& content, const std::string& ip) {
    uint64_t seq = g_comment_sequence.fetch_add(1, std::memory_order_relaxed);
    size_t index = seq % MAX_MEM_COMMENTS;

    std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);

    g_comment_ring_buffer[index].nickname = nickname;
    g_comment_ring_buffer[index].content = content;
    g_comment_ring_buffer[index].ip = ip;
    g_comment_ring_buffer[index].sequence.store(seq + 1, std::memory_order_release);

    // 异步投递写盘任务
    EnqueueDBTask({DBAction::INSERT, seq, nickname, content, ip});
}

std::vector<std::pair<std::string, std::string>> CommentRepository::GetLatestComments() {
    std::vector<std::pair<std::string, std::string>> comments;
    uint64_t current_max_seq = g_comment_sequence.load(std::memory_order_acquire);
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
    out_current_seq = g_comment_sequence.load(std::memory_order_acquire);
    uint64_t start_seq = (out_current_seq > MAX_MEM_COMMENTS) ? (out_current_seq - MAX_MEM_COMMENTS) : 0;

    for (uint64_t i = start_seq; i < out_current_seq; ++i) {
        size_t index = i % MAX_MEM_COMMENTS;
        std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);
        if (g_comment_ring_buffer[index].sequence.load(std::memory_order_acquire) == i + 1) {
            out_comments.push_back({
                i,
                g_comment_ring_buffer[index].nickname,
                g_comment_ring_buffer[index].content,
                g_comment_ring_buffer[index].ip
            });
        }
    }
}

bool CommentRepository::EraseComment(uint64_t seq, std::string& out_nick, std::string& out_ip) {
    uint64_t index = seq % MAX_MEM_COMMENTS;
    std::lock_guard<std::mutex> lock(g_comment_ring_buffer[index].slot_mtx);

    if (g_comment_ring_buffer[index].sequence.load(std::memory_order_acquire) == seq + 1) {
        // 覆盖前先捕获原始作者信息，供上层按 昵称 + IP 自动拉黑
        out_nick = g_comment_ring_buffer[index].nickname;
        out_ip = g_comment_ring_buffer[index].ip;

        std::string intercepted_nick = "[AI 已拦截]";
        std::string intercepted_content = "⚠️ 此条留言因违规已被 AI Agent 执行无锁原子抹除。";
        g_comment_ring_buffer[index].nickname = intercepted_nick;
        g_comment_ring_buffer[index].content = intercepted_content;
        // 原始 ip 保留不动，便于事后取证

        // 异步更新（仅改 nickname/content，ip 字段不变，故传空串）
        EnqueueDBTask({DBAction::UPDATE, seq, intercepted_nick, intercepted_content, ""});
        return true;
    }
    return false;
}

// ==========================================
// 内部私有辅助逻辑实现
// ==========================================

bool CommentRepository::InitSqlite(const std::string& db_path) {
    // 防御编程，正常是/config/xxx.db 判断是否存在父目录
    std::filesystem::path db_file(db_path);
    if (db_file.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(db_file.parent_path(), ec);
        if (ec) { 
            LOG_ERROR << "无法创建数据库目录"; 
            return false; 
        }
    }
    if (sqlite3_open_v2(db_path.c_str(), &m_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
        LOG_ERROR << "无法打开 SQLite 数据库: " << sqlite3_errmsg(m_db);
        return false;
    }

    // 开启高并发 WAL 模式与合理的同步级别
    sqlite3_exec(m_db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(m_db, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);

    // 建立数据表
    const char* create_table_sql = 
        "CREATE TABLE IF NOT EXISTS comments ("
        "sequence INTEGER PRIMARY KEY," // 评论序号，作为主键（唯一且自动建索引）
        "nickname TEXT NOT NULL,"       // 用户昵称，文本类型，不能为空
        "content TEXT NOT NULL,"         // 评论内容，文本类型，不能为空
        "ip TEXT"                       // 投递者来源 IP，可空（旧库 / 匿名场景）
        ");";

    if (sqlite3_exec(m_db, create_table_sql, nullptr, nullptr, nullptr) != SQLITE_OK) {
        LOG_ERROR << "建表失败: " << sqlite3_errmsg(m_db);
        return false;
    }

    sqlite3_exec(m_db, "ALTER TABLE comments ADD COLUMN ip TEXT;", nullptr, nullptr, nullptr);

    // 预编译 SQL 语句以最大化吞吐性能
    const char* insert_sql = "INSERT OR REPLACE INTO comments (sequence, nickname, content, ip) VALUES (?, ?, ?, ?);";
    const char* update_sql = "UPDATE comments SET nickname = ?, content = ? WHERE sequence = ?;";

    if (sqlite3_prepare_v2(m_db, insert_sql, -1, &m_insert_stmt, nullptr) != SQLITE_OK ||
        sqlite3_prepare_v2(m_db, update_sql, -1, &m_update_stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR << "预编译 SQL 语句失败: " << sqlite3_errmsg(m_db);
        return false;
    }

    return true;
}

void CommentRepository::EnqueueDBTask(DBTask&& task) {
    {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        m_db_queue.push(std::move(task));
    }
    m_queue_cv.notify_one();
}

void CommentRepository::DBWorkerLoop() {
    while (true) {
        std::queue<DBTask> local_queue;
        {
            std::unique_lock<std::mutex> lock(m_queue_mtx);
            m_queue_cv.wait(lock, [] {
                return !m_db_queue.empty() || m_stop_requested.load();
            });
            if (m_db_queue.empty() && m_stop_requested.load()) {
                break;
            }
            std::swap(m_db_queue, local_queue);
        }

        // 开启批量事务，将 local_queue 里的所有 I/O 聚合提交
        char* err_msg = nullptr;
        if (sqlite3_exec(m_db, "BEGIN TRANSACTION;", nullptr, nullptr, &err_msg) != SQLITE_OK) {
            LOG_ERROR << "开启事务失败，本批写盘任务被跳过: " << (err_msg ? err_msg : "unknown");
            sqlite3_free(err_msg);
            continue;
        }

        bool all_ok = true;
        while (!local_queue.empty()) {
            if (!ExecuteTaskToSqlite(local_queue.front())) {
                all_ok = false;
            }
            local_queue.pop();
        }

        const char* end_sql = all_ok ? "COMMIT;" : "ROLLBACK;";
        if (sqlite3_exec(m_db, end_sql, nullptr, nullptr, &err_msg) != SQLITE_OK) {
            LOG_ERROR << "事务结束(" << end_sql << ")失败: " << (err_msg ? err_msg : "unknown");
            sqlite3_free(err_msg);
        }
    }
}

bool CommentRepository::ExecuteTaskToSqlite(const DBTask& task) {
    sqlite3_stmt* stmt = (task.action == DBAction::INSERT) ? m_insert_stmt : m_update_stmt;
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    if (task.action == DBAction::INSERT) {
        sqlite3_bind_int64(m_insert_stmt, 1, task.sequence);
        sqlite3_bind_text(m_insert_stmt, 2, task.nickname.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(m_insert_stmt, 3, task.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(m_insert_stmt, 4, task.ip.c_str(), -1, SQLITE_TRANSIENT);
    }
    else if (task.action == DBAction::UPDATE) {
        sqlite3_bind_text(m_update_stmt, 1, task.nickname.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(m_update_stmt, 2, task.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(m_update_stmt, 3, task.sequence);
    }

     if (sqlite3_step(stmt) != SQLITE_DONE) {
        LOG_ERROR << "写入 SQLite 失败 (seq=" << task.sequence << "): " << sqlite3_errmsg(m_db);
        return false;
    }
    return true;
}

} // namespace app