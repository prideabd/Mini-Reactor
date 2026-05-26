#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>
#include <queue>
#include <functional>
#include <vector>
#include <atomic>

// 线程池（linux写法）
// 主线程把任务塞进去，子线程抢占
class ThreadPool {
public:
    using Task = std::function<void()>;
    explicit ThreadPool(size_t thread_count = 4);
    ~ThreadPool();

    void AddTask(Task task);

    void Stop();

private:
    // pthread_create 要求函数指针必须是静态或全局
    static void* Worker(void* args);
    void Run();
    std::vector<pthread_t> threads_; // 线程
    std::queue<Task> task_queue_; // 任务队列（主线程分配给子线程的任务）

    pthread_mutex_t mutex_; // 互斥锁， 保护任务队列
    pthread_cond_t cond_;   // 条件变量
    std::atomic_bool stop_; // 线程池停止标志

};

#endif  