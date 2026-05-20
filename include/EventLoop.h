#ifndef EVENT_LOOP_H
#define EVENT_LOOP

#include <sys/epoll.h>
#include <vector>

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    void Loop();   // 启动 epoll 循环
    void Wakeup(); // 跨线程唤醒主线程

private:
    void HandleRead(); // 处理 eventfd 的可读事件
    int epoll_fd_;      // epoll (红黑树）索引
    int wakeup_fd_;    // 内核事件通知描述符
    bool quit_; 
    std::vector<struct epoll_event> events_;
};

#endif // EVENT_LOOP_H