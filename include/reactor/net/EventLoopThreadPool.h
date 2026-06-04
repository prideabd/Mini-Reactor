#ifndef EVENT_LOOP_THREAD_POOL_H
#define EVENT_LOOP_THREAD_POOL_H

// 每个子线程启动时，都去跑一个EventLoop::Loop()
// 一个子线程 绑定 一个 epoll 树，可以接受多个 client_fd
// 这样，几个子线程就有几个 epoll 树

#include <pthread.h>
#include <vector>

class EventLoop;

class EventLoopThreadPool {
public:
    EventLoopThreadPool(EventLoop* main_loop, size_t thread_count);
    ~EventLoopThreadPool();

    void Start(); // 提供接口，批量创建子线程
    void Stop();  // 提供接口，手动关闭子线程
    EventLoop* GetNextLoop(); // 轮询计算 client_fd 放到哪个 子线程（epoll） 树上

private:
    static void* ThreadFunc(void* args); // 子线程入口静态函数
    EventLoop* main_loop_;
    size_t thread_count_;
    size_t next_; // 下一个 client_fd 存放的 epoll 下标

    struct ThreadData {
        EventLoopThreadPool* pool;
        EventLoop* loop;
        pthread_mutex_t mutex;
        pthread_cond_t cond;
        bool ready;
    };
    std::vector<pthread_t> threads_;
    std::vector<EventLoop*> loops_;  // 存储所有子线程的 EventLoop (即 epoll 树)

};

#endif  // EVENT_LOOP_THREAD_POOL_H