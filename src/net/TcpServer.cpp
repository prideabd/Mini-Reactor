#include <unistd.h>
#include <cstring>

#include "reactor/net/TcpServer.h"
#include "reactor/net/EventLoop.h"
#include "reactor/net/EventLoopThreadPool.h"
#include "reactor/net/Acceptor.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/log/Logger.h"

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
    LOG_INFO << "🧬 [TcpServer]: 正在执行工业级防弹停机流程...";
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

    LOG_INFO << "🔀 [MainReactor]: 成功通过轮询算法，将客人的 FD [" << conn_fd 
             << "] 指派给指定的 SubReactor [" << sub_loop << "] 托管！";
    
    // 实例化连接组件
    auto conn = std::make_shared<TcpConnection>(sub_loop, conn_fd);
    // 挂载关联回调
    conn->SetMessageCallback(std::bind(&TcpServer::OnMessage, this, std::placeholders::_1,std::placeholders::_2));
    conn->SetCloseCallback(std::bind(&TcpServer::RemoveConnection, this, std::placeholders::_1));

    connections_[conn_fd] = conn;

    // 告诉子线程，现在把自己绑定到 epoll 树上
    sub_loop->QueueInLoop([conn]() {
        conn->ConnectionEstablished();
    });
}

// 信息处理函数
void TcpServer::OnMessage(const std::shared_ptr<TcpConnection>& conn, Buffer* buf) {
    while (buf->ReadableBytes() > 0) {
        const char* peek_pos = buf->Peek();
        // 扫描是否有回车 '\n'
        const char* crlf = std::find(peek_pos, peek_pos + buf->ReadableBytes(), '\n');
        if (crlf == peek_pos + buf->ReadableBytes()) {
            LOG_WARN << "⚠️ [TcpServer 业务层]: 收到残缺业务帧，挂起留存。";
            break;
        }
        size_t package_len = crlf - peek_pos + 1;
        std::string single_msg(peek_pos, package_len - 1); // 剥离掉 \n
        buf->Retrieve(package_len); // 确认消费

        if (single_msg.empty()) {
            continue;
        }

        LOG_INFO << "📩 [TcpServer 业务层]: 成功从 FD [" << conn->GetFd() << "] 拆出包: " << single_msg;

        // 响应回显
        std::string reply = "【解耦满血版 TcpConnection】回显: " + single_msg;
        conn->Send(reply);
    }
    // if (message_callback_) {
    //     message_callback_(conn, buf);
    // }
}

// 清理下线连接：利用双重 QueueInLoop 保证无锁化跨线程生命周期延续
void TcpServer::RemoveConnection(const std::shared_ptr<TcpConnection>& conn) {
    main_loop_->QueueInLoop([this, conn]() {
        LOG_INFO << "👋 [TcpServer]: 监测到客户端下线，清理 Map 记录，FD [" << conn->GetFd() << "]";
        // 从主线程擦除，此时因闭包捕获，conn 计数为 2，对象绝不会死亡
        connections_.erase(conn->GetFd());
        EventLoop* sub_loop = conn->GetLoop();
        
        sub_loop->QueueInLoop([conn]() {
            conn->ConnectionDestroyed();
        });
    });
}
