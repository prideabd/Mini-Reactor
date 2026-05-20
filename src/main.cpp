#include "ThreadPool.h"
#include <iostream>
#include <unistd.h>

int main() {
    std::cout << "🚀 [主线程]: 初始化 2 个线程的线程池..." << std::endl;
    ThreadPool pool(2);

    // 1. 投递任务 1 和 2，把 2 个线程全部占满
    pool.AddTask([]() {
        std::cout << " 🛠️ [任务 1]: 开始全力干活..." << std::endl;
        ::sleep(1);
        std::cout << " 🛠️ [任务 1]: 大功告成！" << std::endl;
    });

    pool.AddTask([&pool]() {
        std::cout << " 🔥 [任务 2]: 开始全力干活..." << std::endl;
        ::sleep(1);
        std::cout << " 🔥 [任务 2]: 突然在子线程内部执行 pool.Stop()！" << std::endl;
        
        pool.Stop(); // 💥 宣布线程池不再营业
    });

    // 2. 🔥 主线程故意睡 2 秒，确保任务 2 的 Stop() 已经彻底执行完毕
    std::cout << "🚀 [主线程]: 正在等待子线程掀桌子..." << std::endl;
    ::sleep(2);

    // 3. 💥 此时线程池已经关闭。主线程试图强行塞入【任务 3】
    std::cout << "🚀 [主线程]: 尝试在 Stop 后偷偷投递【任务 3】..." << std::endl;
    pool.AddTask([]() {
        std::cout << " ❌ [任务 3]: 如果我被打印出来，那 AddTask 的拦截就失效了！" << std::endl;
    });

    std::cout << "🚀 [主线程]: 等待 1 秒看任务 3 会不会蹦出来..." << std::endl;
    ::sleep(1);
    
    std::cout << "🚀 [主线程]: 测试结束，安全退出。" << std::endl;
    return 0;
}