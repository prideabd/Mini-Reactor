#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace app {

/**
 * @brief 瞬时物理硬件指标静态快照结构体
 */
struct SysMetricsSnapshot {
    double cpu_usage{2.0};
    double mem_available{80.0};
    double disk_io_mbps{0.1};
    bool is_linux_env{false};
    uint64_t qps{0};
};

/**
 * @brief 指标采集守护进程 (Daemon)
 * 默默运行在独立的旁路后台线程中，每秒轮询一次系统内核，为接口层提供 O(1) 极速快取。
 */
class SysMetricsDaemon {
public:
    // 独立线程启动
    static void Start();

    // 终止线程
    static void Stop();

    // 获取物理指标
    static SysMetricsSnapshot GetMetrics();

private:
    static std::thread m_daemon_thread;
    static std::atomic<bool> m_running;

    static std::mutex m_mtx;
    static std::condition_variable m_cv;
};
} // namespace app