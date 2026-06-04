#include <iostream>
#include "reactor/base/ThreadPool.h"

ThreadPool::ThreadPool(size_t thread_count) : stop_(false) {
    // 在linux下，互斥锁和条件变量无法通过初始化列表（非基础类型）
    pthread_mutex_init(&mutex_, nullptr);
    pthread_cond_init(&cond_, nullptr);

    threads_.resize(thread_count);
    for (size_t i = 0; i < thread_count; ++i) {
        // threads_[]：每个线程的ID
        // nullptr: 线程的属性配置， nullptr表示默认配置
        // Worker: 线程的下一步入口，也就是创建完就去立刻执行该函数
        // this: 专门传给Worker用，会被Worker(void* args)中参数args接收
        if (pthread_create(&threads_[i], nullptr, Worker, this) != 0) {
            std::cerr << "线程创建失败！实际成功创建: " << i << " 个线程。" << std::endl;
            threads_.resize(i);
            break;
        }
    }
}

ThreadPool::~ThreadPool() {
    Stop();
}

void ThreadPool::AddTask(Task task) {
    pthread_mutex_lock(&mutex_);
    if (!stop_) {
        task_queue_.push(std::move(task));
    }
    pthread_mutex_unlock(&mutex_);
    // 唤醒一个等待任务的子线程(解锁 cond_)
    pthread_cond_signal(&cond_);
}

void ThreadPool::Stop() {
    // 因为stop_是一个共享变量，修改前要加锁
    pthread_mutex_lock(&mutex_);
    if (stop_) {
        // 标志位已经是true, 直接返回
        // 确保无论 Stop() 被不小心调用了多少次，真正执行销毁和回收逻辑只跑一次
        pthread_mutex_unlock(&mutex_);
        return;
    }
    stop_ = true;
    pthread_mutex_unlock(&mutex_);

    // 唤醒所有子线程
    pthread_cond_broadcast(&cond_);
    for (auto& thread : threads_) {
        // 等待子线程执行完
        pthread_join(thread, nullptr);
    }
    // 销毁互斥锁和条件变量
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
}

void* ThreadPool::Worker(void* args) {
    // Worker函数为静态函数，通过 this 指针回调非静态成员函数
    // 创建一个指针变量，指向当前线程池
    ThreadPool* pool = static_cast<ThreadPool*>(args);
    pool->Run();
    return nullptr;
}

void ThreadPool::Run() {
    // 真正的子线程执行函数
    // 开始死循环
    while (true) {
        Task task;
        pthread_mutex_lock(&mutex_);

        // 任务队列为空且_stop == false
        while (task_queue_.empty() && !stop_) {
            pthread_cond_wait(&cond_, &mutex_);
        }

        // 线程拿到mutex锁，往下走
        // 拿任务之前要先判断是否有任务以及线程池是否停止
        // 这样假设主线程通知说要stop了，也可以把任务做完清空
        if (stop_ && task_queue_.empty()) {
            pthread_mutex_unlock(&mutex_);
            break;
        }
        // 拿任务
        task = task_queue_.front();
        task_queue_.pop();
        pthread_mutex_unlock(&mutex_);
        
        // 防止空任务（task本质是 std::function<void()>)
        // 假如正好调用时，传了个空任务， 会导致异常
        if (task) {
            task();
        }
    }
}