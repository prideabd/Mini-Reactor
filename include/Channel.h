#ifndef CHANNEL_H
#define CHANNEL_H

#include <functional>

// 前置声明 EventLoop 类
class EventLoop;

// Channel 存储 “套接字以及围着这个套接字转的所有网络事件和回调函数”
// 包括监听新连接的 listen_fd, 已连接的客户 client_fd, 还有
class Channel {
public:
    using EventCallback = std::function<void()>;

    // 每个 channel 必须绑定一个 loop
    Channel(EventLoop* loop, int fd);
    ~Channel() = default;

    // 由 EventLoop 统一调用，根据内核返回的实际事件分发执行对应的回调
    void HandleEvent();

    // 绑定具体业务逻辑回调
    void SetReadCallback(EventCallback cb) { read_callback_ = std::move(cb); }
    void SetWriteCallback(EventCallback cb) { write_callback_ = std::move(cb); }
    void SetCloseCallback(EventCallback cb) { close_callback_ = std::move(cb); }
    void SetErrorCallback(EventCallback cb) { error_callback_ = std::move(cb); }

    // 改变对事件兴趣，并同步到 epoll 树
    void EnableReading();  // 可读
    void DisableReading(); // 不可读
    void EnableWriting();  // 可写
    void DisableWriting(); // 不可写
    void DisableAll();     // 什么都不可

    // 把当前 channel 从 epoll 树上删除
    // 该函数需要被上层业务层调用，需要被暴露出去
    void Remove();

    int GetFd() const { return fd_; }
    uint32_t GetEvents() const { return events_; }
    void SetRevents(uint32_t revents) {revents_ = revents; }
    bool IsNoneEvent() const { return events_ == 0; }

    // 增加接口函数，用于读取当前 channel 在 epoll 树上状态
    // -1: 全新， 1:已经在树上， 2:曾经在树上，但现在被注销了
    int GetIndex() const { return index_; }
    void SetIndex(int index) { index_ = index; } 
    EventLoop* GetLoop() const { return loop_; }

private:  
    void Update(); // 向 epoll 树发起更新/添加请求

    EventLoop* loop_;
    const int fd_;
    
    uint32_t events_; // 注册事件(期望)
    uint32_t revents_; // 活跃事件(实际)

    int index_; // 标志当前 channel 在 epoll 上的状态
    // 四大业务回调闭包
    EventCallback read_callback_;
    EventCallback write_callback_;
    EventCallback close_callback_;
    EventCallback error_callback_;
};

#endif // CHANNEL_H