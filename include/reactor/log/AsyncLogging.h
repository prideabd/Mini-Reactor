#pragma once
#include "LogStream.h"
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

class AsyncLogging {
public:
    AsyncLogging(const std::string& basename, int flushInternal = 3);
    ~AsyncLogging() {
        if (running_) Stop();
    }
    // 前端调用
    void Append(const char* logline, int len);
    void Start();
    void Stop();

private:
    void ThreadFunc(); // 后端线程函数

    using Buffer = LogBuffer<kLargeBuffer>;
    using BufferPtr = std::unique_ptr<Buffer>;
    using BufferVector = std::vector<BufferPtr>;

    const int flushInterval_;
    std::atomic<bool> running_;
    std::string basename_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;

    BufferPtr currentBuffer_;  // 当前缓冲区
    BufferPtr nextBuffer_;     // 预备缓冲区
    BufferVector buffers_;     // 待写入文件的缓冲区队列
};

