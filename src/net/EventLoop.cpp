#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include "reactor/net/EventLoop.h"
#include "reactor/net/Channel.h"
#include "reactor/log/Logger.h"

static constexpr int kInitEventListSize = 16;

EventLoop::EventLoop()
    : epoll_fd_(-1),
      wakeup_fd_(-1),
      quit_(false),
      events_(kInitEventListSize) 
{
    pthread_mutex_init(&mutex_, nullptr); // 初始化互斥锁
    // 1. 创建 epoll实例
    // 使用 EPOLL_CLOEXEC 防止 fork 子线程时泄露文件描述符
    // :: 代表全局作用域限定符，告诉编译器调用的是Linux系统自带的接口函数
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_< 0) {
        LOG_FATAL << "EventLoop: epoll_create1 失败, errno = " << errno;
    }

    // 2. 创建用于跨线程唤醒的 wakeup_fd_ (主要就是唤醒主线程)
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
        LOG_FATAL << "EventLoop: eventfd 创建失败, errno = " << errno;
    }

    // 3. 将 wakeup_fd_  注册到 epoll 树上进行监听
    wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
    wakeup_channel_->SetReadCallback(std::bind(&EventLoop::HandleRead, this));
    wakeup_channel_->EnableReading();
    // struct epoll_event ev;
    // ::memset(&ev, 0, sizeof(ev));
    // ev.events = EPOLLIN | EPOLLET; // 可读事件和边缘触发
    // ev.data.fd = wakeup_fd_;
    // if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev) < 0) {
    //     std::cerr << "EventLoop: epoll_ctl 绑定 wakeup_fd_ 失败, errno = " << errno << std::endl;
    // }
}

EventLoop::~EventLoop() {
    // 析构函数中安全关闭所有系统资源
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
    pthread_mutex_destroy(&mutex_); // 销毁互斥锁
}

void EventLoop::Loop() {
    quit_ = false;
    LOG_INFO << "EventLoop 启动，正在等待内核事件...";
    while (!quit_) {
        // &*events_.begin(): 
        // events_.begin() 是迭代器，* 解引用得到元素对象，& 取地址得到连续物理内存的首地址。
        // 这行指针黑魔法可以完美欺骗 Linux 内核，将其作为普通的 C 风格数组传递。
        int nfds = ::epoll_wait(epoll_fd_,
                                &*events_.begin(),
                                static_cast<int>(events_.size()),
                                -1); // -1 代表永久阻塞等待，直到有事件发生
        if (nfds < 0) {
            // 边缘情况处理：如果由于收到 Linux 信号中断导致 epoll 被打断，属于正常现象，直接重试
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR << "EventLoop: epoll_wait 严重错误, errno = " << errno;
            break;
        }

        // 处理 epoll 树上被内核触发的信号事件
        for (int i = 0; i < nfds; ++i) {
            Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
            channel->SetRevents(events_[i].events); // 设定实际 revents_
            channel->HandleEvent(); // 根据实际 revents_ 调用对应函数
        }

        // 顺手把别的线程（比如主线程）委托给我的跨线程任务全部一口气干完！
        // 比如：真正执行新连接挂树、真正执行退出（Quit）
        DoPendingFunctors();

        // ⭐【自适应动态扩容机制】
        // 如果本次活跃的事件数量（nfds）正好把整个 vector 给填满了
        // 意味着当前并发洪峰极高，现有池子太小了，果断将容量翻倍，降低下一轮内核调用的延迟
        if (static_cast<size_t>(nfds) == events_.size()) {
            events_.resize(events_.size() * 2);
        }
    }
}

/**
 * @brief 异步跨线程任务投递接口（Reactor 控制反转的核心纽带）
 * @note  任意线程都可调用此函数，将控制命令（Lambda）安全运送到当前 Loop 专属线程内执行
 * @param cb 待执行的跨线程异步回调任务
 */
void EventLoop::QueueInLoop(Functor cb) {
    {
        pthread_mutex_lock(&mutex_);
        pending_functors_.emplace_back(std::move(cb));
        pthread_mutex_unlock(&mutex_);
    }
    // 告知主线程，执行 DoPendingFunctors
    Wakeup();
}

void EventLoop::DoPendingFunctors() {
    std::vector<Functor> functors;
    // 绝不穿锁去一个个执行 functor ！因为 functor 内部业务长短不可控。
    // 如果穿锁执行，会导致外部子线程在调用 QueueInLoop 时被锁长时间卡死。
    // 用一个空的局部 vector，在锁保护下与全局变量进行秒级的地址置换（swap），随即解锁！
    pthread_mutex_lock(&mutex_);
    functors.swap(pending_functors_);
    pthread_mutex_unlock(&mutex_);
    for (const auto& functor : functors) {
        if (functor) {
            functor();
        }
    }
}

// 将 channel 更新到 epoll 树上，方便监听
void EventLoop::UpdateChannel(Channel* channel) {
    struct epoll_event ev;
    ::memset(&ev, 0, sizeof(ev));
    ev.events = channel->GetEvents();
    ev.data.ptr = channel; // data 是一个联合体，传指针比 fd 效果更好（不用去内存搜索位置）
    int fd = channel->GetFd();
    int index = channel->GetIndex(); // 获取当前状态

    if (index == -1 || index == 2) {
        // -1表示全新， 2表示被注销，那么就表示都不在树上，调用EPOLL_CTL_ADD
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0) {
            channel->SetIndex(1); // 修改状态为 1
        } else {
            LOG_ERROR << "epoll_ctl ADD 失败, fd: " << fd;
        }
    } else if (index == 1) {
        // 1: 已经在树上，调用EPOLL_CTL_MOD
        if (channel->IsNoneEvent()) {
            // 如果上层调用了 DisableAll() 导致 events_ 变为 0，说明它对任何事件都不感兴趣了
            // 我们可以选择直接 DEL，或者用 MOD 把它挂空，并把状态改写为“已被注销(2)”
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == 0) {
                channel->SetIndex(2); 
            }
        } else {
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
                LOG_ERROR << "epoll_ctl MOD 失败, fd: " << fd;
            }
        }
    }
}

void EventLoop::RemoveChannel(Channel* channel) {
    int fd = channel->GetFd();
    int index = channel->GetIndex();
    if (index == 1 || index == 2) {
        // 只有在树上或者被挂空的连接，才执行真正的内核 DEL 拔树动作
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }
    channel->SetIndex(-1); // 修改状态为 -1
}

void EventLoop::Wakeup() {
    uint64_t one = 1;
    // 跨线程:向 wakeup_fd_ 写入 8 个字节, 这样就能告知 epoll 有变动，快去唤醒主线程
    // 此时内核中的计数器累加，立刻会触发该 wakeup_fd_ 的可读事件，从而把主线程从 epoll_wait 中踢醒
    ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR << "EventLoop::Wakeup() 写入失败";
    }
}

void EventLoop::HandleRead() {
    uint64_t one = 1;
    // 重点：因为 wakeup_fd_ 注册的是 ET (边缘触发) 模式
    // 必须通过 read 把这 8 个字节读出来（让内核计数器清零），否则下一次它将不会再次触发可读
    ssize_t n = ::read(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR << "EventLoop::HandleRead() 读取失败";
    }
    // ====================================================
    // 💡 留给你的 Mini-Reactor 扩展：
    // 被唤醒后，主线程通常需要去执行被其他工作线程塞进来的异步任务队列
    // 例如：this->DoPendingFunctors();
    // ====================================================
    LOG_DEBUG << "Loop 成功被跨线程【唤醒】，正在处理队列任务...";
}