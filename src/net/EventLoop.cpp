#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdlib>
#include <cerrno>

#include "reactor/net/EventLoop.h"
#include "reactor/net/Channel.h"
#include "reactor/log/Logger.h"

namespace reactor::net {

static constexpr int kInitEventListSize = 16;

EventLoop::EventLoop()
    : epoll_fd_(-1),
      wakeup_fd_(-1),
      timer_fd_(-1),
      quit_(false),
      events_(kInitEventListSize),
      thread_id_(std::this_thread::get_id())
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

    // 3. 将 wakeup_fd_ 注册到 epoll 树上进行监听
    wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
    wakeup_channel_->SetReadCallback(std::bind(&EventLoop::HandleRead, this));
    wakeup_channel_->EnableReading();
    
    // 4. 创建 timer_fd_
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) {
        LOG_FATAL << "EventLoop: timerfd_create 失败, errno = " << errno;
    }
    timer_channel_ = std::make_unique<Channel>(this, timer_fd_);
    timer_channel_->SetReadCallback(std::bind(&EventLoop::HandleTimerfd, this));
    timer_channel_->EnableReading();
}

EventLoop::~EventLoop() {
    // 析构函数中安全关闭所有系统资源
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
    if (timer_fd_ >= 0) {
        ::close(timer_fd_);
    }
    pthread_mutex_destroy(&mutex_); // 销毁互斥锁
}

void EventLoop::Loop() {
    quit_ = false;
    LOG_INFO << "EventLoop 启动，正在等待内核事件...";
    while (!quit_.load(std::memory_order_acquire)) {
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

void EventLoop::RunInLoop(Functor cb) {
    if (IsInLoopThread()) {
        // 情况 A：发现就是本线程调用的，拒绝一切进队、排队、拿锁，直接“就地同步执行”！
        // 性能开销为 0，极大提高了单线程内的执行速度
        cb();
    } else {
        // 情况 B：发现是来自外部异界线程的跨越，乖乖送去冷宫任务队列排队安全消化
        QueueInLoop(std::move(cb));
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

void EventLoop::Quit() {
    quit_.store(true, std::memory_order_release);
    // 若不是在 loop 线程内调用，必须唤醒 epoll_wait，否则它会一直睡死
    if (!IsInLoopThread()) {
        Wakeup();
    }
}

void EventLoop::QuitFromSignal() {
    quit_.store(true, std::memory_order_release);
    uint64_t one = 1;
    auto n = ::write(wakeup_fd_, &one, sizeof(one)); // 裸 write，忽略返回值，不打日志
    (void)n;
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

// ============ 定时器对外接口：先原子生成 id 再投递回 loop 线程 ============
EventLoop::TimerId EventLoop::RunAfter(double delay_sec, TimerCallback cb) {
    // 定时器 Id
    TimerId id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    // 计算到期时间
    TimePoint when = Clock::now() + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(delay_sec));
    RunInLoop([this, id, when, cb = std::move(cb)] () mutable {
        AddTimerInLoop(id, when, 0.0, std::move(cb));
    });
    return id;
}

EventLoop::TimerId EventLoop::RunEvery(double interval_sec, TimerCallback cb) {
    TimerId id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    TimePoint when = Clock::now() + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(interval_sec));
    RunInLoop([this, id, when, interval_sec, cb = std::move(cb)] () mutable {
        AddTimerInLoop(id, when, interval_sec, std::move(cb));
    });
    return id;
}

void EventLoop::CancelTimer(TimerId timer_id) {
    RunInLoop([this, timer_id] () {
        CancelTimerInLoop(timer_id);
    });
}

// ============ 以下均只在 loop 线程内执行，访问 timers_ 无需加锁 ============
void EventLoop::AddTimerInLoop(TimerId id, TimePoint when, double interval_sec, TimerCallback cb) {
    // 判断是否改变了最先到期时间
    // 如果当前定时器队列是空的(timers_.empty())，或者新加的这个定时器到期时间(when)比目前队列里最先到期的那个还要早(< timers_.begin()->first)
    // 说明整个底层内核定时器(timerfd)的闹钟时间需要重新调整了。
    bool earliest_changed = timers_.empty() || when < timers_.begin()->first;
    timers_.emplace(when, Timer{id, std::move(cb), interval_sec});
    active_[id] = when;
    if (earliest_changed) {
        ResetTimerfd(timers_.begin()->first);
    }
}

void EventLoop::CancelTimerInLoop(TimerId id) {
    auto it = active_.find(id);
    if (it != active_.end()) {
        // 还在表里:同一时间键可能挂多个定时器,按 id 精确匹配后删除
        TimePoint when = it->second; // 记下到期时间
        auto range = timers_.equal_range(it->second);
        for (auto i = range.first; i != range.second; ++i) {
            if (i->second.id == id) {
                timers_.erase(i);
                break;
            }
        }
        active_.erase(it);
        // 若取消的是（并列）最早到期者，重新校准内核闹钟，省掉一次空唤醒
        if (!timers_.empty() && when <= timers_.begin()->first) {
            ResetTimerfd(timers_.begin()->first);
        }
    } else if (calling_expired_timers_) {
        // 正在本轮被触发(已移入快照,active_ 里已无):登记一下,阻止稍后续期
        canceling_timers_.insert(id);
    }
    // else:早已消亡的定时器 —— active_ 找不到,直接忽略,不留任何垃圾
}

void EventLoop::HandleTimerfd() {
    // ET 模式： 必须 read 把到期计数全部读走，否则不再触发
    uint64_t expirations = 0;
    ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));
    if (n != sizeof(expirations)) {
        LOG_ERROR << "EventLoop::HandleTimerfd() 读取失败";
    }

    TimePoint now = Clock::now();
    // 1. 先把所有到期的定时器从有序表里摘出来（快照），再统一执行
    //    回调内部可能继续 Add/Cancel 定时器，先摘出来可避免迭代器失效
    std::vector<Timer> expired;
    auto it = timers_.begin();
    while (it != timers_.end() && it->first <= now) {
        active_.erase(it->second.id);
        expired.push_back(std::move(it->second));
        it = timers_.erase(it);
    }
    // 2. 逐个执行；跳过已取消的；周期定时器若未取消则续期
    calling_expired_timers_ = true;
    canceling_timers_.clear();
    for (auto& t : expired) {
        if (t.cb) t.cb();
        // 未在本轮被取消的周期定时器才续期
        if (t.interval_sec > 0.0 && canceling_timers_.count(t.id) == 0) {
            TimePoint next = now +
                std::chrono::duration_cast<Clock::duration>(
                    std::chrono::duration<double>(t.interval_sec));
            timers_.emplace(next, Timer{t.id, std::move(t.cb), t.interval_sec});
            active_[t.id] = next;
        }
    }
    calling_expired_timers_ = false;

    // 3. 还有剩余定时器则武装到下一个最近到期点
    if (!timers_.empty()) {
        ResetTimerfd(timers_.begin()->first);
    }
}

void EventLoop::ResetTimerfd(TimePoint earliest) {
    struct itimerspec new_value;
    ::memset(&new_value, 0, sizeof(new_value));

    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  earliest - Clock::now()).count();
    if (ns < 1000) ns = 1000; // 至少 1us：it_value 全 0 会「停掉」timerfd，必须避免
    
    new_value.it_value.tv_sec  = ns / 1'000'000'000;
    new_value.it_value.tv_nsec = ns % 1'000'000'000;
    // it_interval 保持 0：我们每次手动按 timers_.begin() 重新武装，不靠内核周期触发
    if (::timerfd_settime(timer_fd_, 0, &new_value, nullptr) < 0) {
        LOG_ERROR << "EventLoop: timerfd_settime 失败, errno = " << errno;
    }
}


} // namespace reactor::net