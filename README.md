
### 🚀 Mini-Reactor 项目简介

---

### 🧩 核心架构解析

* **⚡ Reactor 底层网络引擎 (`src` / `include/reactor`)**
* **网络核心 (`net`)：** 封装了基于事件驱动的非阻塞网络模型。包含 `EventLoop`（事件循环）、`Channel`（事件分发）、`Acceptor`（连接接收）以及 `TcpServer`。通过 `EventLoopThreadPool` 实现了多线程并发处理，榨干多核 CPU 性能。
* **应用层协议 (`http`)：** 实现了轻量级的 HTTP 状态机解析器 (`HttpCodec` / `HttpContext`)，支持 HTTP 请求的解析与响应组装，提供原生的 `HttpServer` 接口。
* **高性能组件 (`base` / `log`)：** 内置了基于双缓冲（Double Buffering）技术的无锁异步日志系统 (`AsyncLogging`)，确保高并发下的磁盘 I/O 不会阻塞关键网络线程；并维护了自定义的线程池 (`ThreadPool`) 供耗时任务使用。


* **🏢 App 业务逻辑层 (`app`)**
* **路由分发 (`dispatcher`)：** `HttpDispatcher` 承担了网关控制器的角色，负责将不同的 HTTP URI 映射到对应的业务处理逻辑。
* **数据中心 (`repository`)：** 负责数据层的封装与交互（如 `CommentRepository`），实现了数据存储与网络层的彻底剥离。
* **旁路守护 (`daemons`)：** 运行独立的后台常驻线程（如 `SysMetricsDaemon`），实现对服务器物理硬件指标（CPU、内存、QPS）的无锁安全采样与监控。


* **🛠️ 辅助生态与工程化**
* 引入了 **第三方 JSON 库** (`nlohmann/json`) 支持现代化的 RESTful API 交互。
* 支持 **CMake** 一键跨平台构建。
* 配置了 **DevContainers** (`.devcontainer`)，保障了开发环境的高度一致性与开箱即用。
* 配套了丰富的基础前端看板 (`www/index.html`) 与外部监控/AI 控制脚本 (`agent_daemon.py`)。

---

```
Mini-Reactor
├─ .devcontainer
│  ├─ devcontainer-lock.json
│  └─ devcontainer.json
├─ CMakeLists.txt
├─ README.md
├─ agent_daemon.py
├─ app
│  ├─ common
│  │  ├─ AppUtils.cpp
│  │  └─ AppUtils.h
│  ├─ daemons
│  │  ├─ SysMetricsDaemon.cpp
│  │  └─ SysMetricsDaemon.h
│  ├─ dispatcher
│  │  ├─ HttpDispatcher.cpp
│  │  └─ HttpDispatcher.h
│  ├─ main.cpp
│  ├─ repository
│  │  ├─ BlacklistManager.cpp
│  │  ├─ BlacklistManager.h
│  │  ├─ CommentRepository.cpp
│  │  └─ CommentRepository.h
│  └─ single_thread_main.cpp
├─ config
│  └─ blacklist.txt
├─ include
│  └─ reactor
│     ├─ base
│     │  └─ ThreadPool.h
│     ├─ http
│     │  ├─ HttpCodec.h
│     │  ├─ HttpContext.h
│     │  └─ HttpServer.h
│     ├─ log
│     │  ├─ AsyncLogging.h
│     │  ├─ LogStream.h
│     │  └─ Logger.h
│     └─ net
│        ├─ Acceptor.h
│        ├─ Buffer.h
│        ├─ Channel.h
│        ├─ EventLoop.h
│        ├─ EventLoopThreadPool.h
│        ├─ TcpConnection.h
│        └─ TcpServer.h
├─ src
│  ├─ base
│  │  └─ ThreadPool.cpp
│  ├─ http
│  │  ├─ HttpCodec.cpp
│  │  ├─ HttpContext.cpp
│  │  └─ HttpServer.cpp
│  ├─ log
│  │  ├─ AsyncLogging.cpp
│  │  ├─ LogStream.cpp
│  │  └─ Logger.cpp
│  └─ net
│     ├─ Acceptor.cpp
│     ├─ Buffer.cpp
│     ├─ Channel.cpp
│     ├─ EventLoop.cpp
│     ├─ EventLoopThreadPool.cpp
│     ├─ TcpConnection.cpp
│     └─ TcpServer.cpp
├─ tests
│  └─ test_smoke.sh
├─ third_party
│  ├─ nlohmann
│  │  └─ json.hpp
│  └─ sqlite
│     ├─ sqlite3.c
│     └─ sqlite3.h
├─ www
│  └─ index.html
├─ 下一阶段需求
└─ 小结
   ├─ Mini-Reactor需求文档
   ├─ 回调机制.md
   ├─ 学习小卡片.md
   ├─ 整体流程
   ├─ 阶段一小结.md
   ├─ 阶段七小结.md
   ├─ 阶段三小结.md
   ├─ 阶段九小结.md
   ├─ 阶段二小结.md
   ├─ 阶段五小结.md
   ├─ 阶段八小结.md
   ├─ 阶段六小结.md
   └─ 阶段四小结.md

```