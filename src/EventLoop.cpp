#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
// #include <errno.h>
#include <iostream>
#include <cstdlib>
#include <cerrno>

#include "EventLoop.h"

static constexpr int kInitEventListSize = 16;

EventLoop::EventLoop()
    : epoll_fd_(-1),
      wakeup_fd_(-1),
      quit_(false),
      events_(kInitEventListSize) 
{
    // 1. 创建 epoll实例
    // 使用 EPOLL_CLOEXEC 防止 fork 子线程时泄露文件描述符
    // :: 代表全局作用域限定符，告诉编译器调用的是Linux系统自带的接口函数
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_< 0) {
        std::cerr << "EventLoop: epoll_creat1 失败, errno = " << errno << std::endl;
        // 自杀, 退出码 1 代表非正常退出
        ::exit(EXIT_FAILURE);
    }

    // 2. 创建用于跨线程唤醒的 wakeup_fd_
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
        std::cerr << "EventLoop: eventfd 创建失败, errno = " << errno << std::endl;
    }

    // 3. 将 wakeup_fd_  注册到 epoll 树上进行监听
    struct epoll_event ev;
    ::memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET; // 可读事件和边缘触发
    ev.data.fd = wakeup_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev) < 0) {
        std::cerr << "EventLoop: epoll_ctl 绑定 wakeup_fd_ 失败, errno = " << errno << std::endl;
    }
}

EventLoop::~EventLoop() {
    // 析构函数中安全关闭所有系统资源
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
}

void EventLoop::Loop() {
    quit_ = false;
    std::cout << " 主线程 EventLoop 启动，正在等待内核事件..." << std::endl;
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
            std::cerr << "EventLoop: epoll_wait 严重错误, errno = " << errno << std::endl;
            break;
        }

        // 遍历并分发所有就绪事件
        for (int i = 0; i < nfds; ++i) {
            int fd = events_[i].data.fd;

            // 如果是跨线程唤醒的信号来了
            if (fd == wakeup_fd_) {
                HandleRead(); // 读出 8 字节， 清空事件计数器
            } else {
                // ====================================================
                // 💡 留给你的 Mini-Reactor 扩展：
                // 这里应该通过 fd 找到你封装的 Channel 对象或对应的回调函数
                // 例如：Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
                //       channel->HandleEvent(events_[i].events);
                // ====================================================
                std::cout << "收到普通客户端 FD [" << fd << "] 的网络事件" << std::endl;
            }
        }

        // ⭐【自适应动态扩容机制】
        // 如果本次活跃的事件数量（nfds）正好把整个 vector 给填满了
        // 意味着当前并发洪峰极高，现有池子太小了，果断将容量翻倍，降低下一轮内核调用的延迟
        if (static_cast<size_t>(nfds) == events_.size()) {
            events_.resize(events_.size() * 2);
        }
    }
}

void EventLoop::Wakeup() {
    uint64_t one = 1;
    // 跨线程向 wakeup_fd_ 写入 8 个字节
    // 此时内核中的计数器累加，立刻会触发该 wakeup_fd_ 的可读事件，从而把主线程从 epoll_wait 中踢醒
    ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        std::cerr << "EventLoop::Wakeup() 写入失败" << std::endl;
    }
}

void EventLoop::HandleRead() {
    uint64_t one = 1;
    // 重点：因为 wakeup_fd_ 注册的是 ET (边缘触发) 模式
    // 必须通过 read 把这 8 个字节读出来（让内核计数器清零），否则下一次它将不会再次触发可读
    ssize_t n = ::read(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        std::cerr << "EventLoop::HandleRead() 读取失败" << std::endl;
    }
    // ====================================================
    // 💡 留给你的 Mini-Reactor 扩展：
    // 被唤醒后，主线程通常需要去执行被其他工作线程塞进来的异步任务队列
    // 例如：this->DoPendingFunctors();
    // ====================================================
    std::cout << "主线程成功被跨线程【唤醒】，正在处理队列任务..." << std::endl;
}