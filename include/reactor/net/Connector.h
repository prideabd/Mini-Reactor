#ifndef CONNECTOR_H
#define CONNECTOR_H

/**
 * @file Connector.h
 * @brief Connector 类，主动发起 TCP 连接（Acceptor 的对偶）。
 *
 * 生命周期模型：
 *   - Connector 必须由 std::shared_ptr 管理，通过 Connector::Create() 创建。
 *   - 所有跨线程 / 延迟 / Channel 回调都使用 shared_from_this() 延长 Connector 生命周期，
 *     避免异步任务捕获裸 this 后对象提前析构导致悬空指针。
 *   - TcpClient 持有 std::shared_ptr<Connector>。
 */

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace reactor::net {
    
class EventLoop;
class Channel;

class Connector : public std::enable_shared_from_this<Connector> {
public:
    using Ptr = std::shared_ptr<Connector>;
    using NewConnectionCallback = std::function<void(int sock_fd)>;
    using ErrorCallback = std::function<void()>;

    // 静态工厂
    static Ptr Create(EventLoop* loop, const std::string& ip, int port) {
        return Ptr(new Connector(loop, ip, port));
    }

    ~Connector();

    // 禁用赋值和拷贝
    Connector(const Connector&) = delete;
    Connector& operator=(const Connector&) = delete;

    void SetNewConnectionCallback(NewConnectionCallback cb) {
        new_connection_callback_ = std::move(cb);
    }
    void SetErrorCallback(ErrorCallback cb) {
        error_callback_ = std::move(cb);
    }

    void Start(); // 发起连接，内部 RunInLoop，并用 shared_ptr 自保
    void Stop();  // 停止连接 / 重试，内部 RunInLoop，并用 shared_ptr 自保

private:
    enum State {
        kDisconnected,
        kConnecting,
        kConnected
    };

    Connector(EventLoop* loop, const std::string& ip, int port);

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