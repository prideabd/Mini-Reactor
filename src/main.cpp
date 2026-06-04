#include "EventLoop.h"
#include "TcpServer.h"
#include "Logger.h"
#include "AsyncLogging.h"
#include <iostream>
#include <signal.h> // 🌟 引入信号处理头文件
#include <thread>
#include <vector>
#include <string>

AsyncLogging* g_asyncLog = nullptr;

// 🌟 改进后的回调桥接：增加安全性与降级逻辑
void AsyncOutputProxy(const char* msg, int len) {
    if (g_asyncLog) {
        g_asyncLog->Append(msg, len);
    } else {
        // 如果异步日志还没初始化或已提前析构，直接打到控制台，防止日志丢失
        ::fwrite(msg, 1, len, stdout);
    }
}

// 🌟 信号全局处理函数：屏蔽致命的 SIGPIPE 信号
void IgnoreSigPipe() {
    struct sigaction sa;
    sa.sa_handler = SIG_IGN; // 设置为忽略信号
    ::sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (::sigaction(SIGPIPE, &sa, nullptr) < 0) {
        LOG_ERROR << "屏蔽 SIGPIPE 信号失败！";
    } else {
        LOG_INFO << "🛡️ [系统防护]: 成功屏蔽 SIGPIPE 信号，防止对端断开导致服务器暴毙！";
    }
}

int main() {
    LOG_INFO << "=========================================================";
    LOG_INFO << "🎮 [系统初始化]: 开启工业级 Multi-Reactor 多反应堆网络服务...";
    LOG_INFO << "=========================================================";

    // 🌟 工业级标配：第一步先对进程装甲进行加固，忽略内核管道破裂信号
    IgnoreSigPipe();

    // --- 1. 日志系统初始化 ---
    // 实例化异步日志对象 (文件名: reactor.log, 3秒强制刷新)
    AsyncLogging log("reactor.log", 3);
    g_asyncLog = &log;
    // 设置 Logger 全局输出回调到异步落盘逻辑
    Logger::SetOutput(AsyncOutputProxy);
    // 开启后端日志写入线程
    log.Start();

    {
        // --- 2. 服务器网络引擎初始化 ---
        // 实例化主反应堆（只负责管理 Acceptor 的连接接收事件）
        EventLoop main_loop;
        // 实例化高性能总服务器类 (监听 8888 端口, 3个 Sub-Reactor 子线程)
        TcpServer server(&main_loop, 8888, 3);
        // 启动服务器
        server.Start();

        LOG_INFO << "✅ 服务器启动成功，开始监听端口: 8888";
        LOG_INFO << "📝 异步日志后端已就绪，记录文件: reactor.log";

        // --- 4. 开启事件循环 ---
        // 这里会阻塞主线程，直到程序退出
        main_loop.Loop();

        // --- 5. 优雅退出清理 ---
        LOG_INFO << "🚨 服务器准备物理关闭，开始冲刷残余日志...";
    } // 关键：server 和 main_loop 在此处析构。它们的析构日志会进入 AsyncLogging 的待写队列。

    log.Stop();                // 执行最后的“一滴血”冲刷，确保 server 的析构日志被物理写入文件
    g_asyncLog = nullptr;      // 切断全局关联
    Logger::SetOutput(nullptr); // 重置输出回调到 stdout，保护后续可能存在的极晚期日志

    return 0;
}