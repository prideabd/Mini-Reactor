#ifndef TCP_SERVER_H
#define TCP_SERVER_H

/**
 * @file TcpServer.h
 * @brief TCP 服务器核心类，负责监听新连接、管理连接池以及将连接分发到 EventLoop 线程池。
 */
#include <unordered_map>
#include <memory>
#include <functional>

#include "reactor/net/Buffer.h"

namespace reactor::net {

class Acceptor;
class EventLoopThreadPool;
class EventLoop;
class Buffer;
class TcpConnection;

class TcpServer {
public:
    using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*)>;

    TcpServer(EventLoop* main_loop, int port, size_t thread_count);
    ~TcpServer();

    void Start();

    // 提供给外部（如 HTTP 模块）注入协议解析器的接口
    void setMessageCallback(const MessageCallback& cb) { message_callback_ = cb; }

private:
    void NewConnection(int conn_fd);    // 接管 Acceptor 传上来的新连接描述符
    void RemoveConnection(const std::shared_ptr<TcpConnection>& conn); 

    // 业务处理入口
    void OnMessage(const std::shared_ptr<TcpConnection>& conn, Buffer* buf);

    EventLoop* main_loop_;
    // 独占拥有，同时表示生命周期和 TcpServer 对象相同，当 TcpServer 销毁时，自动调用独享指针的析构
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> thread_pool_;

    // buffer 和 channel 全部放到了 TcpConnection
    std::unordered_map<int, std::shared_ptr<TcpConnection>> connections_;

    MessageCallback message_callback_; // 回调函数
};

} // namespace reactor::net

#endif // TCP_SERVER_H