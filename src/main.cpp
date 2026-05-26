#include "EventLoop.h"
#include "TcpServer.h"
#include <iostream>

int main() {
    std::cout << "=========================================================" << std::endl;
    std::cout << "🎮 [系统初始化]: 开启工业级 Multi-Reactor 多反应堆网络服务..." << std::endl;
    std::cout << "=========================================================" << std::endl;

    // 1. 实例化主反应堆（只负责管理 Acceptor 的事件）
    EventLoop main_loop;

    // 2. 实例化高性能总服务器类
    // 参数含义：绑定的主 loop 指针、监听端口 8888、并发 SubReactor 子线程数量设为 3 个
    TcpServer server(&main_loop, 8888, 3);

    // 3. 启动大管家
    server.Start();

    // 4. 主线程开启永不停歇的事件轮盘
    main_loop.Loop();

    return 0;
}