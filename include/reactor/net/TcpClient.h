#ifndef TCP_CLIENT_H
#define TCP_CLIENT_H

/**
 * @file TcpClient.h
 * @brief TcpClient 类，主动连接的客户端封装（对称于 TcpServer）。
 *        内部持有一个 Connector 负责非阻塞 connect；连接成功后在【同一个 loop】
 *        上生成一条上游 TcpConnection，并把连接建立/断开、收到数据等事件抛给上层。
 *
 * 生命周期模型：
 *   - TcpClient 必须由 std::shared_ptr 管理，通过 TcpClient::Create() 创建。
 *   - TcpClient 持有 std::shared_ptr<Connector>。
 *   - Connector 回调只捕获 weak_ptr<TcpClient>，避免 Connector 被异步任务延寿后，
 *     回调访问已经析构的 TcpClient。
 *   - TcpClient 自身对外异步投递也不捕获裸 this。
 */

#include <functional>
#include <memory>
#include <string>
#include <atomic>

#include "reactor/net/Buffer.h"

namespace reactor::net {

class EventLoop;
class Connector;
class TcpConnection;

class TcpClient : public std::enable_shared_from_this<TcpClient> {
public:
    using Ptr = std::shared_ptr<TcpClient>;
    using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
    using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
    using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*)>;
    using ErrorCallback = std::function<void()>;
    
    static Ptr Create(EventLoop* loop, const std::string& ip, int port);

    ~TcpClient();

    // 禁止赋值和拷贝
    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    void SetConnectionCallback(ConnectionCallback cb) {
        connection_callback_ = std::move(cb);
    }
    void SetMessageCallback(MessageCallback cb) {
        message_callback_ = std::move(cb);
    }

    void SetErrorCallback(ErrorCallback cb) { 
        error_callback_ = std::move(cb); 
    }

    void Connect();     // 发起上游连接（内部经 Connector::Start → RunInLoop）
    void Disconnect();  // 优雅关闭已建立的上游连接（半关闭写端）
    void Stop();        // connect 途中主动放弃（停掉 Connector 的重试）

    EventLoop* GetLoop() const { return loop_; }
    TcpConnectionPtr GetConnection() const { return connection_; }

private:
    TcpClient(EventLoop* loop, const std::string& ip, int port);

    void InitCallbacks();
    void NewConnection(int sock_fd);                       // Connector 回调：拿到已连通的裸 fd
    void RemoveConnection(const TcpConnectionPtr& conn);   // 上游连接关闭时的清理

    EventLoop* loop_;
    std::shared_ptr<Connector> connector_;

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;
    ErrorCallback error_callback_;

    std::atomic<bool> connect_;    // 是否仍希望保持连接
    TcpConnectionPtr connection_;  // 当前上游连接（单条）
};

} // namespace reactor::net

#endif // TCP_CLIENT_H
