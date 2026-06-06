#include <iostream>
#include <signal.h>
#include <thread>
#include <vector>
#include <string>

#include "reactor/net/EventLoop.h"
#include "reactor/log/Logger.h"
#include "reactor/log/AsyncLogging.h"

#include "reactor/http/HttpServer.h"
#include "reactor/http/HttpCodec.h"
#include "HttpHandler.h"

using namespace reactor::net;
using namespace reactor::http;
using namespace reactor::log;

reactor::log::AsyncLogging* g_asyncLog = nullptr;

// 改进后的回调桥接：增加安全性与降级逻辑
void AsyncOutputProxy(const char* msg, int len) {
    if (g_asyncLog) {
        g_asyncLog->Append(msg, len);
    } else {
        // 如果异步日志还没初始化或已提前析构，直接打到控制台，防止日志丢失
        ::fwrite(msg, 1, len, stdout);
    }
}

void IgnoreSigPipe() {
    struct sigaction sa;
    sa.sa_handler = SIG_IGN; // 设置为忽略信号
    ::sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (::sigaction(SIGPIPE, &sa, nullptr) < 0) {
        // 降级使用标准库输出，此时日志可能还未初始化
        ::fprintf(stderr, "⚠️ [系统防护]: 屏蔽 SIGPIPE 信号失败！\n");
    } else {
        ::fprintf(stdout, "🛡️ [系统防护]: 成功屏蔽 SIGPIPE 信号，防止对端断开导致进程暴毙。\n");
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
        // 实例化高性能总服务器类 (监听 8080 端口, 3个 Sub-Reactor 子线程)
        HttpServer server(&main_loop, 8080, 3);

        server.SetHttpCallback(app::HandleHttpRequest);

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