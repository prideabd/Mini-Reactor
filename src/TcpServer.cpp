#include <unistd.h>
#include <cstring>
#include <iostream>

#include "TcpServer.h"
#include "EventLoop.h"
#include "Channel.h"
#include "EventLoopThreadPool.h"
#include "Acceptor.h"

TcpServer::TcpServer(EventLoop* main_loop, int port, size_t thread_count)
    : main_loop_(main_loop)
{
    // 初始化 acceptor 接收器
    acceptor_ = std::make_unique<Acceptor>(main_loop_, port);
    acceptor_->SetNewConnectionCallback(std::bind(&TcpServer::NewConnection, this, std::placeholders::_1));

    // 初始化 线程池
    thread_pool_ = std::make_unique<EventLoopThreadPool>(main_loop_, thread_count);
}

TcpServer::~TcpServer() {
    std::cout << "🧬 [TcpServer]: 正在执行工业级防弹停机流程..." << std::endl;
    if (acceptor_) {
        acceptor_->Stop();
    }
    if (thread_pool_) {
        thread_pool_->Stop();
    }
}

void TcpServer::Start() {
    thread_pool_->Start();
    acceptor_->Listen();
}

void TcpServer::NewConnection(int conn_fd) {
    // 采用轮询，首先找到一个工作子线程 subReactor
    EventLoop* sub_loop = thread_pool_->GetNextLoop();

    std::cout << "🔀 [MainReactor]: 成功通过轮询算法，将客人的 FD [" << conn_fd 
              << "] 跨线程指派给指定的 SubReactor [" << sub_loop << "] 托管！" << std::endl;
    
    // 为客户端连接实例化专属高级 Channel 经纪人，
    auto conn_channel = std::make_unique<Channel>(sub_loop, conn_fd);
    conn_channel->SetReadCallback(std::bind(&TcpServer::HandleClientRead, this, conn_fd));
    conn_channel->SetCloseCallback(std::bind(&TcpServer::HandleClientClose, this, conn_fd));

    Channel* conn_channel_ptr = conn_channel.get();
    client_channels_[conn_fd] = std::move(conn_channel);

    // 主线程告诉子线程，现在把自己绑定到 epoll 树上
    sub_loop->QueueInLoop([conn_channel_ptr]() {
        conn_channel_ptr->EnableReading();
    });
}

void TcpServer::HandleClientRead(int conn_fd) {
    char buf[1024];
    ::memset(buf, 0, sizeof(buf));
    ssize_t n = ::read(conn_fd, buf, sizeof(buf));

    if (n > 0) {
        std::string msg(buf, n);
        pthread_t cur_tid = ::pthread_self();
        std::cout << "📩 [SubReactor 线程 " << cur_tid << "]: 收到 FD [" << conn_fd << "] 的原始消息: " << msg;

        // 回复
        std::string reply = "【工业级 Multi-Reactor 满血版】处理回显: " + msg;

        // 这里需要修改，大文件、慢网络时会出现问题
        ::write(conn_fd, reply.c_str(), reply.size());
    } else if (n == 0) {
        // 主动发送 Fin 包下线（ctrl + c)
        HandleClientClose(conn_fd);
    } else if (n < 0 && errno != EAGAIN) {
        // 发生严重错误
        HandleClientClose(conn_fd);
    }
}

void TcpServer::HandleClientClose(int conn_fd) {
    auto it = client_channels_.find(conn_fd);
    if (it != client_channels_.end()) {
        std::cout << "👋 [TcpServer]: 监测到客户端 FD [" << conn_fd << "] 请求下线，准备斩草除根..." << std::endl;
        // 提取出智能指针，release 函数将独占指针变为普通指针
        // 这样当前 Channel 的生命周期就被这个局部变量一把抱住，哈希表立刻 erase 也不会引发析构！
        auto channel_ptr_owner = it->second.release();

        // 从服务器哈希表删除
        client_channels_.erase(it);

        EventLoop* sub_loop = channel_ptr_owner->GetLoop();

        sub_loop->QueueInLoop([this, conn_fd, channel_ptr_owner]() {
            channel_ptr_owner->Remove();
            ::close(conn_fd);
            delete channel_ptr_owner;
            std::cout << "✨ [SubReactor]: FD [" << conn_fd << "] 资源安全释放完毕。" << std::endl;
        });
        // 这里会出现问题，子线程在Remove()，但是主线程已经删除了 Channel
        // 这会导致找不到对应 channel，访问到非法内存，从而导致段错误
        // client_channels_.erase(it);
    }
}

