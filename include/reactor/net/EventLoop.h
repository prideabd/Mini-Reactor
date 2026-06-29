#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

/**
 * @file EventLoop.h
 * @brief EventLoop 类，Reactor 模式的核心，每个线程一个 EventLoop，负责事件循环和事件分发。
 */
#include <sys/epoll.h>
#include <vector>
#include <functional>
#include <pthread.h>
#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
#include <map>
#include <set>
#include <unordered_map>
#include <cstdint>

namespace reactor::net {

class Channel; // 前置声明
class EventLoop {
public:
    using Functor = std::function<void()>;
    using TimerCallback = std::function<void()>;
    using TimerId = uint64_t;
    EventLoop();
    ~EventLoop();

    void Loop();   // 启动 epoll 循环
    void Wakeup(); // 跨线程唤醒主线程
    void Quit();   // 终止信号
    void QuitFromSignal(); // 不带输出日志的终止版本

    // 给 Channel 提供的 epoll 树控制接口
    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);

    // 智能化调度接口。在专属线程内则就地同步运行，否则降级投递异步排队
    void RunInLoop(Functor cb);

    // 允许任意外部线程（如主线程、业务线程池）向当前 Loop 线程无条件投递异步任务
    void QueueInLoop(Functor cb);

    // 定时器对外接口
    TimerId RunAfter(double delay_sec, TimerCallback cb);    // 延迟 delay 秒执行一次
    TimerId RunEvery(double interval_sec, TimerCallback cb); // 间隔 interval 秒重置
    void CancelTimer(TimerId id);                            //  取消未到期的定时器

    // 高频盘查内联函数。判断发起调用的线程，是否就是当前 Loop 专属绑定的线程
    bool IsInLoopThread() const { return thread_id_ == std::this_thread::get_id(); }

private:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    struct Timer {
        TimerId id;
        TimerCallback cb;
        double interval_sec;
    };

    // 定时器内部实现
    void HandleTimerfd();
    void AddTimerInLoop(TimerId id, TimePoint when, double interval_sec, TimerCallback cb);
    void CancelTimerInLoop(TimerId id);
    void ResetTimerfd(TimePoint earliest);

    void HandleRead();       // 处理 eventfd 的可读事件
    void DoPendingFunctors();// 主线程执行外部子线程提供的任务  

    int epoll_fd_;      // epoll (红黑树）索引
    int wakeup_fd_;    // 内核事件通知描述符
    std::atomic<bool> quit_; 
    std::vector<struct epoll_event> events_;
    std::unique_ptr<Channel> wakeup_channel_;
    std::vector<Functor> pending_functors_;
    pthread_mutex_t mutex_;
    const std::thread::id thread_id_;

    // 定时器成员
    int timer_fd_;
    std::unique_ptr<Channel> timer_channel_;
    std::multimap<TimePoint, Timer> timers_;        // 有序，begin() 即刚到期
    std::unordered_map<TimerId, TimePoint> active_; // id → 它在 timers_ 里的时间键
    bool calling_expired_timers_ = false;
    std::set<TimerId> canceling_timers_;            // 仅在触发期间临时使用,每轮清空
    std::atomic<TimerId> next_timer_id_{1};
};

} // namespace reactor::net

#endif // EVENT_LOOP_H