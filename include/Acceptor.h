#ifndef ACCEPTOR_H
#define ACCEPTOR_H

#include <functional>
#include <memory>

class EventLoop;
class Channel;
class Acceptor {
public:
    using NewConnectionCallback = std::function<void(int conn_fd)>;
    Acceptor(EventLoop* loop, int port);
    ~Acceptor();

    // 回调函数
    void SetNewConnectionCallback(NewConnectionCallback cb) {
        new_connection_callback_ = std::move(cb);
    }
    void Listen(); // 监听函数
    void Stop();

private:
    void HandleRead(); // 可读时的内核事件处理函数

    EventLoop* loop_; 
    int listen_fd_;

    NewConnectionCallback new_connection_callback_;
    std::unique_ptr<Channel> accept_channel_; // listen_fd 专属 channel
};

#endif // ACCEPTOR_H