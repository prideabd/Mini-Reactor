#ifndef TCP_CONNECTION_H
#define TCP_CONNECTION_H

/**
 * @file TcpConnection.h
 * @brief TcpConnection 类，表示一个已建立的 TCP 连接，负责数据的读写和状态管理。
 */
#include <memory>
#include <string>
#include <functional>
#include "reactor/net/Buffer.h"

namespace reactor::net {

class EventLoop;
class Channel;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    // 注入 TcpServer 上层回调
    using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*)>;
    using CloseCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;

    TcpConnection(EventLoop* loop, int conn_fd);
    ~TcpConnection();

    // 增加状态机枚举
    enum StateE { kConnecting, kConnected, kDisconnecting, kDisconnected };
    
    void SetState(StateE s) { state_ = s; }
    StateE GetState() const { return state_; }

    EventLoop* GetLoop() const { return loop_; }
    int GetFd() const { return conn_fd_; }

    // 业务层 TcpSever 调用的非阻塞发送
    void Send(const std::string& msg);
    // 连接建立时的初始化（挂载红黑树）
    void ConnectionEstablished();
    // 连接解体时的资源销毁
    void ConnectionDestroyed();

    // 设置回调函数
    void SetMessageCallback(MessageCallback cb) { message_callback_ = std::move(cb); }
    void SetCloseCallback(CloseCallback cb) { close_callback_ = std::move(cb); }

private:
    // 内核 -> channel(获取 fd) -> TcpConnection(获取 input_buffer_) -> TcpServer
    void HandleRead();  // 给 channel 的底层读驱动
    void HandleWrite(); // 底层驱动写：output_buffer_ -> 内核
    void HandleClose(); // 底层驱动关闭 -> 叫醒 TcpServer 清理 Map

    StateE state_;
    EventLoop* loop_;
    int conn_fd_;

    std::unique_ptr<Channel> channel_;
    Buffer input_buffer_;
    Buffer output_buffer_;

    MessageCallback message_callback_;
    CloseCallback close_callback_;
};

} // namespace reactor::net

#endif // TCP_CONNECTION_H