#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <array>
#include <cstdio>
#include <algorithm>

#include "SysMetricsDaemon.h"
#include "reactor/log/Logger.h"
#include "dispatcher/HttpDispatcher.h"

namespace app {

// ==========================================
// 静态成员变量显式初始化
// ==========================================
std::thread SysMetricsDaemon::m_daemon_thread;
std::atomic<bool> SysMetricsDaemon::m_running{false};
std::mutex SysMetricsDaemon::m_mtx;
std::condition_variable SysMetricsDaemon::m_cv;

// ==========================================
// 采集服务私有数据与底层结构（完全隐形）
// ==========================================
namespace {
    struct CpuTicks {
        uint64_t user{0}, nice{0}, system{0}, idle{0}, iowait{0}, irq{0}, softirq{0}, steal{0};
        uint64_t Total() const { return user + nice + system + idle + iowait + irq + softirq + steal; }
        uint64_t Active() const { return user + nice + system + irq + softirq + steal; }
    };

    std::mutex g_metrics_mtx;
    SysMetricsSnapshot g_cached_metrics;

    uint64_t g_last_req_count = 0; // 记录上一秒的请求数量，用于计算 qps

    // 熔断判断配置项
    constexpr double CPU_TRIGGER_THRESHOLD = 95.0;    // 熔断触发 CPU 阈值
    constexpr uint64_t QPS_TRIGGER_THRESHOLD = 10000; // 熔断触发 CPU 阈值

    constexpr double   CPU_RECOVER_THRESHOLD = 80.0;  // 自愈恢复 CPU 阈值
    constexpr uint64_t QPS_RECOVER_THRESHOLD = 7000;  // 自愈恢复 QPS 阈值

}

// ==========================================
// 守护进程业务接口具体实现
// ==========================================
void SysMetricsDaemon::Start() {
    // 防止重复启动
    if (m_running.exchange(true)) {
        return;
    }
    // 拉起线程
    LOG_INFO << "⚙️ [SysMetricsDaemon]: 后台硬件指标轮询守护线程已成功点火。";

    m_daemon_thread = std::thread([]() {
        CpuTicks last_cpu_ticks{};
        uint64_t last_disk_sectors = 0;
        bool is_first_run = true; // 解决冷启动爬坡与开机数据污染

        while (m_running.load(std::memory_order_relaxed)) {
            SysMetricsSnapshot temp_cache;
            temp_cache.is_linux_env = false; // 默认为 false，直至被物理文件自证
            temp_cache.qps = 0;
            // 1. 采集 CPU 报文
            std::ifstream stat_file("/proc/stat");
            if (stat_file.is_open()) {
                std::string line;
                if (std::getline(stat_file, line)) {
                    LOG_DEBUG << "[SysMetricsDaemon] 成功读取 /proc/stat 原始流 -> " << line;
                    std::string label;
                    CpuTicks current_cpu;
                    std::stringstream ss(line);

                    if (ss >> label >> current_cpu.user >> current_cpu.nice >> current_cpu.system
                        >> current_cpu.idle >> current_cpu.iowait >> current_cpu.irq >> current_cpu.softirq
                        >> current_cpu.steal) {

                        temp_cache.is_linux_env = true;
                        uint64_t total_delta = current_cpu.Total() - last_cpu_ticks.Total();
                        uint64_t active_delta = current_cpu.Active() - last_cpu_ticks.Active();
                        if (!is_first_run && total_delta > 0) {
                            temp_cache.cpu_usage = (static_cast<double>(active_delta) / total_delta) * 100.0;
                        } else {
                            temp_cache.cpu_usage = 2.0;
                        }
                        last_cpu_ticks = current_cpu;
                    }
                    stat_file.close();
                }
            }
            // 2. 采集内存报文
            std::ifstream mem_file("/proc/meminfo");
            if (mem_file.is_open()) {
                std::string line;
                uint64_t total = 0, available = 0;
                while (std::getline(mem_file, line)) {
                    if (line.rfind("MemTotal:", 0) == 0) std::sscanf(line.c_str(), "MemTotal: %lu", &total);
                    else if (line.rfind("MemAvailable:", 0) == 0) std::sscanf(line.c_str(), "MemAvailable: %lu", &available);
                }
                if (total > 0) temp_cache.mem_available = (static_cast<double>(available) / total) * 100.0;
                mem_file.close();
            }
            // 3. 采集 I/O 磁盘报文
            std::ifstream disk_file("/proc/diskstats");
            if (disk_file.is_open()) {
                std::string line;
                uint64_t curr_sectors = 0;
                while (std::getline(disk_file, line)) {
                    unsigned int major, minor; char name[32];
                    uint64_t rio, rmerge, rsect, ruse, wio, wmerge, wsect;
                    if (std::sscanf(line.c_str(), "%u %u %31s %lu %lu %lu %lu %lu %lu %lu",
                                    &major, &minor, name, &rio, &rmerge, &rsect, &ruse, &wio, &wmerge, &wsect) >= 10) {
                        std::string dev(name);
                        if (dev.rfind("sd", 0) == 0 || dev.rfind("nvme", 0) == 0) {
                            curr_sectors += (rsect + wsect);
                        }
                    }
                }
                uint64_t sector_delta = (curr_sectors >= last_disk_sectors) ? (curr_sectors - last_disk_sectors) : 0;
                temp_cache.disk_io_mbps = (static_cast<double>(sector_delta) * 512.0) / (1024.0 * 1024.0); 
                last_disk_sectors = curr_sectors;
                disk_file.close();
            }

            // 4. 严格 1s 计算 qps
            uint64_t current_req_count = g_global_request_count.load(std::memory_order_relaxed);
            uint64_t computed_qps = (current_req_count >= g_last_req_count) ? (current_req_count - g_last_req_count) : 0;
            temp_cache.qps = computed_qps;
            g_last_req_count = current_req_count;

            // 5. 回退机制
            if (!temp_cache.is_linux_env) {
                double loadRatio = std::min(static_cast<double>(computed_qps) / 1400.0, 1.0);
                temp_cache.cpu_usage = 2.0 + loadRatio * 85.0;
                temp_cache.mem_available = 82.3 - loadRatio * 15.2;
                temp_cache.disk_io_mbps = 0.1 + loadRatio * 45.8;
                LOG_WARN << "仿真计算 CPU 指标 (QPS: " << computed_qps << ")";
            }

            // 6. 加锁实施数学平滑平摊（EMA 滤波算法）并送入全局缓存
            {
                std::lock_guard<std::mutex> lock(g_metrics_mtx);
               if (is_first_run && temp_cache.is_linux_env) {
                    // 首次运行不执行 EMA 滤波，直接让真机初始值到位，杜绝低位爬坡
                    g_cached_metrics = temp_cache;
                    is_first_run = false; 
                } else {
                    // 后续轮询，实施正常的平滑平摊（EMA 滤波）
                    g_cached_metrics.cpu_usage = g_cached_metrics.cpu_usage * 0.3 + temp_cache.cpu_usage * 0.7;
                    g_cached_metrics.mem_available = g_cached_metrics.mem_available * 0.7 + temp_cache.mem_available * 0.3;
                    g_cached_metrics.disk_io_mbps = g_cached_metrics.disk_io_mbps * 0.4 + temp_cache.disk_io_mbps * 0.6;
                    g_cached_metrics.is_linux_env = temp_cache.is_linux_env;
                    g_cached_metrics.qps = temp_cache.qps;
                }

                // 7. 高并发的带来的熔断
                if (g_cached_metrics.cpu_usage > CPU_TRIGGER_THRESHOLD || g_cached_metrics.qps > QPS_TRIGGER_THRESHOLD) {
                    // 若当前处于正常运行状态，则果断拉响闸刀
                    if (!g_agent_cooldown_mode.load(std::memory_order_relaxed)) {
                        g_agent_cooldown_mode.store(true, std::memory_order_release);
                        LOG_WARN << "🚨 [System Guard]: 硬件超载检测 (CPU: " << g_cached_metrics.cpu_usage 
                                 << "%, QPS: " << g_cached_metrics.qps << "). 正在强制拉响全局熔断闸刀！";
                    }
                }
                // 高并发结束，自动复位
                else if (g_cached_metrics.cpu_usage < CPU_RECOVER_THRESHOLD && g_cached_metrics.qps < QPS_RECOVER_THRESHOLD) {
                    // 若当前处于熔断状态，且流量已过，自动合闸恢复服务
                    if (g_agent_cooldown_mode.load(std::memory_order_relaxed)) {
                        g_agent_cooldown_mode.store(false, std::memory_order_release);
                        LOG_INFO << "🟢 [System Guard]: 高并发流量洪峰已过，系统指标回落正常 (CPU: " << g_cached_metrics.cpu_usage 
                                 << "%, QPS: " << g_cached_metrics.qps << ")。熔断器自动解除，全线业务恢复正常！";
                    }
                }
            }

            std::unique_lock<std::mutex> cv_lock(m_mtx);
            m_cv.wait_for(cv_lock, std::chrono::seconds(1), [] {
                return !m_running.load(std::memory_order_relaxed);
            });
        }
        LOG_INFO << "⚙️ [SysMetricsDaemon]: 收到停机指令，指标采集循环已安全终止。";
    });
}

void SysMetricsDaemon::Stop() {
    if (m_running.exchange(false)) {
        // 1. 发送唤醒信号
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_cv.notify_one();
        }
        // 2. 等待后台线程执行完毕
        if (m_daemon_thread.joinable()) {
            m_daemon_thread.join();
        }
        LOG_INFO << "✅ [SysMetricsDaemon]: 后台守护进程已彻底回收释放。";
    }
}

SysMetricsSnapshot SysMetricsDaemon::GetMetrics() {
    std::lock_guard<std::mutex> lock(g_metrics_mtx);
    return g_cached_metrics;
}

} // namespace app
