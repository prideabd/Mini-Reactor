/**
 * @file timer_test.cpp
 * @brief Mini-Reactor 定时器功能与跨线程安全性极限压测
 * * 🚀【编译与运行指南】（推荐：在项目根目录下操作）
 * -------------------------------------------------------------------------
 * 1. 首先确保终端路径处于项目的根目录：
 * cd ~/Mini-Reactor
 * * 2. 复制并执行以下一整段编译命令（已包含所有依赖的 net 和 log 源码）：
 * g++ -std=c++17 tests/timer_test.cpp \
 * src/net/EventLoop.cpp src/net/Channel.cpp \
 * src/log/Logger.cpp src/log/LogStream.cpp src/log/AsyncLogging.cpp \
 * -o tests/timer_test -Iinclude -lpthread
 * * 3. 运行测试程序：
 * ./tests/timer_test
 * -------------------------------------------------------------------------
 */

#include "reactor/net/EventLoop.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <string>

using namespace reactor::net;

// 辅助函数：打印带毫秒时间戳的日志，方便观察定时器精度
void TestLog(const std::string& msg) {
    static auto start_time = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
    std::cout << "[" << elapsed << " ms] " << msg << std::endl;
}

int main() {
    TestLog("🚀 Mini-Reactor 定时器极限压测启动...");
    EventLoop loop;

    // =================================================================
    // 场景一：测试 RunAfter (最基本的单次精确定时)
    // =================================================================
    loop.RunAfter(2.0, []() {
        TestLog("【验证成功】★ RunAfter ★ 2.0 秒单次定时器准时触发！");
    });

    // =================================================================
    // 场景二：测试 RunEvery 周期触发 + 回调内部「自我取消」
    // =================================================================
    // 注意：由于 Lambda 内部要捕获 every_id，我们用 static 变量避开生命周期和捕获环
    static int every_count = 0;
    static EventLoop::TimerId every_id = 0;
    
    every_id = loop.RunEvery(1.0, [&loop]() {
        every_count++;
        TestLog("【循环中】★ RunEvery ★ 1.0 秒周期定时器触发，当前计数: " + std::to_string(every_count));
        
        if (every_count >= 3) {
            TestLog("【验证成功】★ CancelTimer ★ 周期定时器触发满 3 次，开始在回调内部自我取消！");
            loop.CancelTimer(every_id); // 触发你代码中 calling_expired_timers_ 的拦截逻辑
        }
    });

    // =================================================================
    // 场景三：测试 CancelTimer 外部提前拦截（击毙未到期的定时器）
    // =================================================================
    EventLoop::TimerId target_id = loop.RunAfter(4.0, []() {
        TestLog("【❌ 严重错误】这个 4.0 秒的定时器明明被取消了，为什么还会触发？！");
    });

    // 在 1.5 秒的时候，手动把 4.0 秒的定时器扼杀在摇篮里
    loop.RunAfter(1.5, [&loop, target_id]() {
        TestLog("【动作】1.5 秒时，外部发起手动取消 4.0 秒的定时器");
        loop.CancelTimer(target_id); // 触发你代码中 active_ 查表直接删除逻辑
    });

    // =================================================================
    // 场景四：跨线程多线程安全投递测试 (RunInLoop)
    // =================================================================
    std::thread foreign_thread([&loop]() {
        // 让异界线程先睡 500 毫秒
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        TestLog("【跨线程】子线程开始跨线程向 EventLoop 异步投递一个 1.0 秒的定时器...");
        
        loop.RunAfter(1.0, []() {
            TestLog("【验证成功】★ 跨线程安全 ★ 由子线程投递的定时器，在 Loop 线程内完美准时触发！");
        });
    });

    // =================================================================
    // 兜底安全保障：5.5 秒后让整个事件循环安全退出，防止主线程死等
    // =================================================================
    loop.RunAfter(5.5, [&loop]() {
        TestLog("【结束】5.5 秒测试时间到，安全关闭 EventLoop 引擎。");
        loop.Quit();
    });

    // 核心起航
    loop.Loop();

    // 回收子线程资产
    foreign_thread.join();
    TestLog("🏁 测试完美结束。");
    return 0;
}