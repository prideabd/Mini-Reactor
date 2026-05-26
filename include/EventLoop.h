#ifndef EVENT_LOOP_H
#define EVENT_LOOP

#include <sys/epoll.h>
#include <vector>
#include <functional>
#include <pthread.h>
#include <memory>

class Channel; // 前置声明
class EventLoop {
public:
    using Functor = std::function<void()>;
    EventLoop();
    ~EventLoop();

    void Loop();   // 启动 epoll 循环
    void Wakeup(); // 跨线程唤醒主线程
    void Quit() { quit_ = true; }

    // 给 Channel 提供的 epoll 树控制接口
    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);

    // 允许子线程安全向主线程 Loop 线程提供任务
    void QueueInLoop(Functor cb);

private:
    void HandleRead();       // 处理 eventfd 的可读事件
    void DoPendingFunctors();// 主线程执行外部子线程提供的任务  

    int epoll_fd_;      // epoll (红黑树）索引
    int wakeup_fd_;    // 内核事件通知描述符
    bool quit_; 
    std::vector<struct epoll_event> events_;
    std::unique_ptr<Channel> wakeup_channel_;
    std::vector<Functor> pending_functors_;
    pthread_mutex_t mutex_;
};

#endif // EVENT_LOOP_H