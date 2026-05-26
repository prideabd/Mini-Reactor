#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include <unordered_map>
#include <memory>

class Acceptor;
class EventLoopThreadPool;
class EventLoop;
class Channel;

class TcpServer {
public:
    TcpServer(EventLoop* main_loop, int port, size_t thread_count);
    ~TcpServer();

    void Start();

private:
    void NewConnection(int conn_fd);    // 接管 Acceptor 传上来的新连接描述符
    void HandleClientRead(int conn_fd); // 客户端发来信息时 I/O 回调
    void HandleClientClose(int conn_fd);// 客户端断开连接时 I/O 回调 

    EventLoop* main_loop_;
    // 独占拥有，同时表示生命周期和 TcpServer 对象相同，当 TcpServer 销毁时，自动调用独享指针的析构
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> thread_pool_;

    // 🌟 为了延长每一个客户端连接衍生出的 Channel 的生命周期
    // 在内存中使用一张哈希表，保护好所有在线客人的 Channel 对象
    std::unordered_map<int, std::unique_ptr<Channel>> client_channels_;
};

#endif // TCP_SERVER_H