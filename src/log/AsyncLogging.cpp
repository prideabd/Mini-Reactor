#include <cstdio>
#include <chrono>
#include "reactor/log/AsyncLogging.h"

AsyncLogging::AsyncLogging(const std::string& basename, int flushInternal)
    : flushInterval_(flushInternal),
      running_(false),
      basename_(basename),
      currentBuffer_(new Buffer),
      nextBuffer_(new Buffer) {
    currentBuffer_->Bzero();
    nextBuffer_->Bzero();
    buffers_.reserve(16);
}

void AsyncLogging::Start() {
    running_ = true;
    thread_ = std::thread(&AsyncLogging::ThreadFunc, this);
}

void AsyncLogging::Stop() {
    running_ = false;
    cond_.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void AsyncLogging::Append(const char* logline, int len) {
    bool need_notify = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (currentBuffer_->Avail() > len) {
            currentBuffer_->Append(logline, len);
        } else {
            buffers_.push_back(std::move(currentBuffer_));
            if (nextBuffer_) { // 判断 nextBuffer_ 为一个有效指针，指向一个合法地址
                currentBuffer_ = std::move(nextBuffer_);
            } else {
                currentBuffer_.reset(new Buffer);
            }
            // 这里采用 nextBuffer_ 空间后，没有进行 len 判断，是因为在 LogStream 中已经进行一次判断，限制在32字节
            currentBuffer_->Append(logline, len);
            need_notify = true; // 唤醒后端线程
        }
    }
    if (need_notify) {
        cond_.notify_one();
    }
}

void AsyncLogging::ThreadFunc() {
    // 准备两个空缓冲区用于交换
    BufferPtr buffer1(new Buffer);
    BufferPtr buffer2(new Buffer);
    buffer1->Bzero();
    buffer2->Bzero();
    BufferVector buffersToWrite;
    buffersToWrite.reserve(16);

    FILE* fp = ::fopen(basename_.c_str(), "ae");
    if (!fp) {
        fprintf(stderr, "无法打开日志文件: %s\n", basename_.c_str());
        return;
    }

    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (buffers_.empty()) {
                cond_.wait_for(lock, std::chrono::seconds(flushInterval_));
            }
            // 将当前缓冲推入队列
            buffers_.push_back(std::move(currentBuffer_));
            currentBuffer_ = std::move(buffer1); // 归还一个空缓冲
            buffersToWrite.swap(buffers_);
            if (!nextBuffer_) { // nextBuffer_ 为空指针
                nextBuffer_ = std::move(buffer2);
            }
        }
        // 写入文件
        for (const auto& buffer : buffersToWrite) {
            fwrite(buffer->Data(), 1, buffer->Length(), fp);
        }
        // 当空闲时，保持两个 buffer 循环使用
        if (buffersToWrite.size() > 2) {
            buffersToWrite.resize(2);
        }
        if (!buffer1) {
            buffer1 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            buffer1->Reset();
        }
        if (!buffer2) {
            buffer2 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            buffer2->Reset();
        }
        buffersToWrite.clear();
        fflush(fp);
    }

    // 🌟 核心改进：执行最后一次“收尾”清扫
    // 即使 running_ 变为 false，也要把内存中剩下的数据写完
    {
        std::unique_lock<std::mutex> lock(mutex_);
        buffers_.push_back(std::move(currentBuffer_));
        buffersToWrite.swap(buffers_);
    }

    for (const auto& buffer : buffersToWrite) {
        ::fwrite(buffer->Data(), 1, buffer->Length(), fp);
    }
    ::fflush(fp);
    ::fclose(fp);
}