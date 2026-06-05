#ifndef ACCEPTOR_H
#define ACCEPTOR_H

/**
 * @file Acceptor.h
 * @brief Acceptor 类，负责监听新的 TCP 连接请求，并将其分发给 TcpServer 处理。
 */
#include <functional>
#include <memory>

namespace reactor::net {

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

} // namespace reactor::net

#endif // ACCEPTOR_H