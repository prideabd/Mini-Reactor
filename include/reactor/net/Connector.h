#ifndef CONNECTOR_H
#define CONNECTOR_H

/**
 * @file Connector.h
 * @brief Connector 类，主动发起 TCP 连接（Acceptor 的对偶）。
 *        非阻塞 connect + 指数退避「有界」重试；连接成功后把裸 sockfd 交给上层，
 *        由上层（TcpClient）包成 TcpConnection。重试预算（次数/时限）耗尽后，
 *        触发一次 ErrorCallback 告知调用方彻底放弃（反代据此回 502）。
 */

#include <functional>
#include <memory>
#include <string>
#include <atomic>
#include <cstdint>
#include <chrono>

namespace reactor::net {
    
class EventLoop;
class Channel;

class Connector {
public:
    using NewConnectionCallback = std::function<void(int sock_fd)>;
    using ErrorCallback = std::function<void()>;

    Connector(EventLoop* loop, const std::string& ip, int port);
    ~Connector();

    void SetNewConnectionCallback(NewConnectionCallback cb) {
        new_connection_callback_ = std::move(cb);
    }
    void SetErrorCallback(ErrorCallback cb) {
        error_callback_ = std::move(cb);
    }

    void Start(); // 发起连接，内部RunInLoop
    void Stop();  // 停止并取消未决的重试定时器

private:
    enum State {
        kDisconnected,
        kConnecting,
        kConnected
    };

    void StartInLoop();
    void StopInLoop();
    void Connect();               // socket() + 非阻塞 connect()
    void Connecting(int sock_fd); // 注册可写事件，进入 kConnecting
    void HandleWrite();           // 可写触发：getsockopt(SO_ERROR) 判成功/失败
    void Retry(int sock_fd);      // 关闭旧 fd → 通知上层 → RunAfter 退避后重连
    int RemoveAndResetChannel();  // 摘除 channel，返回裸 fd 以便移交
    void ResetChannel();

    EventLoop* loop_;
    std::string ip_;
    int port_;
    std::atomic<State> state_;
    std::atomic<bool> connect_;       // 是否仍希望连接（Stop 时置 false）
    std::unique_ptr<Channel> channel_;// 连接中 fd 的专属 channel（监听 EPOLLOUT）
    double retry_delay_sec_;          // 当前退避时长（每次翻倍，封顶）
    uint64_t retry_timer_;             // EventLoop::TimerId，未决重试定时器；Stop 时取消

    NewConnectionCallback new_connection_callback_;
    ErrorCallback error_callback_;
};
} // namespace reactor::net

#endif // CONNECTOR_H