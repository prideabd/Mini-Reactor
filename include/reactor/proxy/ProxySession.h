#ifndef PROXY_SESSION_H
#define PROXY_SESSION_H

/**
 * @file ProxySession.h
 * @brief ProxySession 类，反向代理阶段一核心：桥接一对「下游 conn + 上游 conn」，
 *        实现 TCP 层透明双向转发（down -> up / up -> down）。
 *
 * 生命周期模型：
 *   - ProxySession 必须由 std::shared_ptr 管理：std::make_shared<ProxySession>(...)。
 *   - 上层在下游连接建立时创建 ProxySession，并通过 down_conn->SetContext(session)
 *     挂到下游连接 context 中。
 *   - ProxySession 内部持有 std::shared_ptr<TcpClient>，TcpClient 也通过 TcpClient::Create()
 *     创建，避免异步回调访问已析构对象。
 *   - Start() 中使用 weak_from_this() 捕获会话，避免回调强引用环。
 *   - 下游断开时必须调用 Teardown()，用于停止上游、清理状态、打破引用关系。
 */

#include <memory>
#include <string>

#include "reactor/net/Buffer.h"
#include "reactor/net/TcpClient.h"

namespace reactor::net {

class EventLoop;
class TcpConnection;

class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
    using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

    ProxySession(EventLoop* loop, const std::string& up_ip, int up_port,
                 const TcpConnectionPtr& down_conn);
    ~ProxySession();

    ProxySession(const ProxySession&) = delete;
    ProxySession& operator=(const ProxySession&) = delete;

    void Start();                        // 发起上游连接
    void Teardown();                     // 下游断开 -> 停止上游、清理会话
    void OnDownstreamData(Buffer* buf);  // 下游 -> 上游

private:
    void OnUpstreamConnection(const TcpConnectionPtr& up_conn);         // 上游连上/断开
    void OnUpstreamData(const TcpConnectionPtr& up_conn, Buffer* buf);  // 上游 -> 下游
    void OnUpstreamError();                                             // 上游连接失败/重试通知

    // ---- 流控（背压）----
    // 给下游连接装高水位 / 写完成回调（下游写积压时暂停读上游）
    void InstallDownstreamFlowControl();
    // 给上游连接装高水位 / 写完成回调（上游写积压时暂停读下游）
    void InstallUpstreamFlowControl();

    EventLoop* loop_;

    // 新版 TcpClient 必须由 TcpClient::Create() 创建，并以 shared_ptr 持有。
    TcpClient::Ptr client_;

    TcpConnectionPtr down_conn_;    // 下游连接
    TcpConnectionPtr up_conn_;      // 上游连接（上游连接成功后填充）

    std::string pending_up_;        // 上游握手完成前暂存下游数据
    bool up_ready_ = false;         // 上游是否已连接成功
    bool tearing_down_ = false;     // 防止 Teardown 重入

    // ---- 流控状态 ----
    // 下游读因“上游写积压”被暂停（down -> up 方向背压）
    bool down_read_paused_ = false;
    // 上游读因“下游写积压”被暂停（up -> down 方向背压）
    bool up_read_paused_ = false;
    // 代理高水位阈值（1 MiB）：某端输出积压跨越它就暂停读另一端
    static constexpr size_t kProxyHighWaterMark = 1 * 1024 * 1024;
};

} // namespace reactor::net

#endif // PROXY_SESSION_H

