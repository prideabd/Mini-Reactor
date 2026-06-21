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

namespace reactor::net {

class Channel; // 前置声明
class EventLoop {
public:
    using Functor = std::function<void()>;
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

    // 高频盘查内联函数。判断发起调用的线程，是否就是当前 Loop 专属绑定的线程
    bool IsInLoopThread() const { return thread_id_ == std::this_thread::get_id(); }

private:
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
};

} // namespace reactor::net

#endif // EVENT_LOOP_H