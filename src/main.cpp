#include "EventLoop.h"
#include "TcpServer.h"
#include <iostream>
#include <signal.h> // 🌟 引入信号处理头文件

// 🌟 信号全局处理函数：屏蔽致命的 SIGPIPE 信号
void IgnoreSigPipe() {
    struct sigaction sa;
    sa.sa_handler = SIG_IGN; // 设置为忽略信号
    ::sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (::sigaction(SIGPIPE, &sa, nullptr) < 0) {
        std::cerr << "❌ [警告]: 屏蔽 SIGPIPE 信号失败！" << std::endl;
    } else {
        std::cout << "🛡️ [系统防护]: 成功屏蔽 SIGPIPE 信号，防止对端断开导致服务器暴毙！" << std::endl;
    }
}

int main() {
    std::cout << "=========================================================" << std::endl;
    std::cout << "🎮 [系统初始化]: 开启工业级 Multi-Reactor 多反应堆网络服务..." << std::endl;
    std::cout << "=========================================================" << std::endl;

    // 🌟 工业级标配：第一步先对进程装甲进行加固，忽略内核管道破裂信号
    IgnoreSigPipe();

    // 1. 实例化主反应堆（只负责管理 Acceptor 的连接接收事件）
    EventLoop main_loop;

    // 2. 实例化高性能总服务器类
    // 参数含义：绑定的主 loop 指针、监听端口 8888、并发 SubReactor 子线程数量设为 3 个
    TcpServer server(&main_loop, 8888, 3);

    // 3. 启动大管家（内部会同步拉起 3 个 Sub-Reactor 工作子线程）
    server.Start();

    // 4. 主线程开启永不停歇的事件轮盘
    main_loop.Loop();

    return 0;
}