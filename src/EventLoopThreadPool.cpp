#include "EventLoopThreadPool.h"
#include "EventLoop.h"
#include "Logger.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* main_loop, size_t thread_count)
    : main_loop_(main_loop),
      thread_count_(thread_count),
      next_(0)
{}

EventLoopThreadPool::~EventLoopThreadPool() {
    // 线程声明周期交给各自 Loop 内部销毁
}

void EventLoopThreadPool::Start() {
    if (thread_count_ == 0) {
        return;
    }
    threads_.resize(thread_count_);
    loops_.resize(thread_count_);
    for (size_t i = 0; i < thread_count_; ++i) {
        ThreadData data {
            this,
            nullptr,
            PTHREAD_MUTEX_INITIALIZER,
            PTHREAD_COND_INITIALIZER,
            false
        };
        // 强制捕获返回值（pthread 系列函数成功返回 0，失败返回错误码）
        // 创建成功后，子线程会立刻执行 ThreadFunc
        int ret = pthread_create(&threads_[i], nullptr, &EventLoopThreadPool::ThreadFunc, &data);
        if (ret != 0 ) {
            // 销毁锁，防止资源泄漏
            pthread_mutex_destroy(&data.mutex);
            pthread_cond_destroy(&data.cond);
            LOG_FATAL << "pthread_create 失败! 错误码 = " << ret;
        }
        // 创建成功后，主线程拿锁开始等待
        pthread_mutex_lock(&data.mutex);
        // 多线程系统中，存在 "虚假唤醒", 没有任何信号，主线程自动醒来
        // 所以这里是 while 非 if
        while (!data.ready) {
            pthread_cond_wait(&data.cond, &data.mutex);
        }
        loops_[i] = data.loop;
        pthread_mutex_unlock(&data.mutex);

        // 销毁锁，防止资源泄漏
        pthread_mutex_destroy(&data.mutex);
        pthread_cond_destroy(&data.cond);
    }
    LOG_INFO << "🚀 [SubReactors]: 成功初始化 " << thread_count_ << " 个独立的 SubReactor 线程池！";
}

void EventLoopThreadPool::Stop() {
    // 遍历所有成功运行的 EventLoop
    for (EventLoop* sub_loop : loops_) {
        if (sub_loop != nullptr) {
            // 这个时候子线程可能在等待，不能直接调用Quit，需要有个事件变更
            // QueueInLoop 函数会触发 wakeup_fd_，从而唤醒子线程
            sub_loop->QueueInLoop([sub_loop]() {
                sub_loop->Quit();
            });
        }
    }

    for (size_t i = 0; i < thread_count_; ++i) {
        if (threads_[i] != 0) {
            pthread_join(threads_[i], nullptr);
        }
    }
}

void* EventLoopThreadPool::ThreadFunc(void* args) {
    ThreadData* data = static_cast<ThreadData*>(args);

    // 子线程创建自己的 EventLoop (epoll)
    EventLoop sub_loop;

    pthread_mutex_lock(&data->mutex);
    data->loop = &sub_loop;
    data->ready = true;
    pthread_cond_signal(&data->cond);
    pthread_mutex_unlock(&data->mutex);

    sub_loop.Loop();
    return nullptr;
}

EventLoop* EventLoopThreadPool::GetNextLoop() {
    EventLoop* loop = main_loop_;
    if (!loops_.empty()) {
        loop = loops_[next_];
        next_ = (next_ + 1) % thread_count_;
    }
    return loop;
}
