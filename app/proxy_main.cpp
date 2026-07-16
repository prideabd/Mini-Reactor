#include <iostream>
#include <signal.h>
#include <string>
#include <any>
#include <memory>
#include <cstdlib>

#include "reactor/net/EventLoop.h"
#include "reactor/net/TcpServer.h"
#include "reactor/net/TcpConnection.h"
#include "reactor/proxy/ProxySession.h"

#include "reactor/log/Logger.h"
#include "reactor/log/AsyncLogging.h"

using namespace reactor::net;
using namespace reactor::log;

reactor::log::AsyncLogging* g_asyncLog = nullptr;
reactor::net::EventLoop* g_main_loop = nullptr;

void AsyncOutputProxy(const char* msg, int len) {
    if (g_asyncLog) {
        g_asyncLog->Append(msg, len);
    } else {
        ::fwrite(msg, 1, len, stdout);
    }
}

void GracefulShutdownHandler(int signum) {
    if (g_main_loop) {
        g_main_loop->QuitFromSignal();
    }
}

void RegisterSignals() {
    struct sigaction sa_ign;
    sa_ign.sa_handler = SIG_IGN;
    ::sigemptyset(&sa_ign.sa_mask);
    sa_ign.sa_flags = 0;

    if (::sigaction(SIGPIPE, &sa_ign, nullptr) < 0) {
        ::fprintf(stderr, "⚠️ [系统防护]: 屏蔽 SIGPIPE 信号失败！\n");
    }

    struct sigaction sa_shutdown;
    sa_shutdown.sa_handler = GracefulShutdownHandler;
    ::sigemptyset(&sa_shutdown.sa_mask);
    sa_shutdown.sa_flags = SA_RESTART;

    ::sigaction(SIGINT, &sa_shutdown, nullptr);
    ::sigaction(SIGTERM, &sa_shutdown, nullptr);
}

int main() {

    AsyncLogging log("reactor_proxy.log", 3);
    g_asyncLog = &log;
    Logger::SetOutput(AsyncOutputProxy);
    log.Start();

    RegisterSignals();

    {
        const int listen_port = 8080;              // 代理监听端口
        const std::string upstream_ip = "127.0.0.1";
        const int upstream_port = 9000;            // 上游源站端口
        const size_t thread_count = 3;

        EventLoop main_loop;
        g_main_loop = &main_loop;

        TcpServer proxy_server(&main_loop, listen_port, thread_count);

        proxy_server.SetConnectionCallback(
            [upstream_ip, upstream_port](const std::shared_ptr<TcpConnection>& conn) {
                if (conn->GetState() == TcpConnection::kConnected) {
                    LOG_INFO << "🎉 [Proxy] 下游连接建立 FD [" << conn->GetFd()
                             << "]，连接上游 " << upstream_ip << ":" << upstream_port;

                    auto session = std::make_shared<ProxySession>(
                        conn->GetLoop(),
                        upstream_ip,
                        upstream_port,
                        conn
                    );

                    conn->SetContext(session);
                    session->Start();
                } else {
                    LOG_INFO << "👋 [Proxy] 下游连接断开 FD [" << conn->GetFd()
                             << "]，清理代理会话";

                    std::any* ctx = conn->GetMutableContext();

                    if (ctx && ctx->has_value()) {
                        try {
                            auto session =
                                std::any_cast<std::shared_ptr<ProxySession>>(*ctx);

                            if (session) {
                                session->Teardown();
                            }

                            conn->SetContext(std::any{});
                        } catch (const std::bad_any_cast& e) {
                            LOG_ERROR << "❌ [Proxy] context 类型错误: " << e.what();
                            conn->SetContext(std::any{});
                        }
                    }
                }
            }
        );

        proxy_server.SetMessageCallback(
            [](const std::shared_ptr<TcpConnection>& conn, Buffer* buf) {
                std::any* ctx = conn->GetMutableContext();

                if (!ctx || !ctx->has_value()) {
                    LOG_WARN << "⚠️ [Proxy] 收到数据但无 ProxySession，丢弃 FD ["
                             << conn->GetFd() << "]";
                    buf->RetrieveAll();
                    return;
                }

                try {
                    auto session =
                        std::any_cast<std::shared_ptr<ProxySession>>(*ctx);

                    if (session) {
                        session->OnDownstreamData(buf);
                    } else {
                        buf->RetrieveAll();
                    }
                } catch (const std::bad_any_cast& e) {
                    LOG_ERROR << "❌ [Proxy] context 类型错误: " << e.what();
                    buf->RetrieveAll();
                }
            }
        );

        proxy_server.Start();

        LOG_INFO << "✅ [Proxy] L4 透明转发代理启动成功";
        LOG_INFO << "🌍 [Proxy] listen: " << listen_port;
        LOG_INFO << "🎯 [Proxy] upstream: " << upstream_ip << ":" << upstream_port;

        main_loop.Loop();

        LOG_INFO << "🚨 [Proxy] 主事件循环退出";
    }

    log.Stop();
    g_asyncLog = nullptr;
    Logger::SetOutput(nullptr);

    return 0;
}