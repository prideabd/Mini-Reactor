cmake_minimum_required(VERSION 3.10)
project(MiniReactorWebServer)

# ============================================================================
# 1. 编译器防护与全局优化配置
# ============================================================================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -Wall 开启警告，-O2 生产级优化，-pthread 跨平台多线程支持
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2 -pthread")

# ============================================================================
# 2. 头文件搜索路径配置
# ============================================================================
# 包含网络框架的头文件
include_directories(${PROJECT_SOURCE_DIR}/include)
# 包含 app 目录本身
include_directories(${PROJECT_SOURCE_DIR}/app)

# ============================================================================
# 3. 编译框架层（编译为静态库 libreactor.a）
# ============================================================================
# 物理隔离，这里只搜集 src/ 目录下的框架源文件，杜绝将业务搅和进来
file(GLOB_RECURSE FRAMEWORK_SOURCES "${PROJECT_SOURCE_DIR}/src/*.cpp")

# 排除可能干扰构建的过时单线程测试入口
list(REMOVE_ITEM FRAMEWORK_SOURCES "${PROJECT_SOURCE_DIR}/src/single_thread_main.cpp")

# 将框架部分编译为一个独立的静态库（对 app 业务隐藏底层物理细节）
add_library(reactor STATIC ${FRAMEWORK_SOURCES})

# ============================================================================
# 4. 编译业务应用层（构建最终的可执行 Web 服务器）
# ============================================================================
# 显式罗列各业务分层源码，实现物理资产的精准控制与追踪
set(APP_SOURCES
    ${PROJECT_SOURCE_DIR}/app/main.cpp
    ${PROJECT_SOURCE_DIR}/app/dispatcher/HttpDispatcher.cpp
    ${PROJECT_SOURCE_DIR}/app/daemons/SysMetricsDaemon.cpp
    ${PROJECT_SOURCE_DIR}/app/repository/CommentRepository.cpp
    ${PROJECT_SOURCE_DIR}/app/common/AppUtils.cpp
)

# 定义你的最终交付应用目标（这里我们更名为更有应用标识度的 web_server）
add_executable(web_server ${APP_SOURCES})

# ============================================================================
# 5. 依赖纽带链接
# ============================================================================
# 让业务程序 web_server 强力链接刚刚打好的框架库 reactor 以及操作系统的 pthread 库
target_link_libraries(web_server PRIVATE reactor pthread)

# ============================================================================
# 6. 工程构建调试打印（方便盘查文件搜集是否发生物理错漏）
# ============================================================================
message(STATUS "--------------------------------------------------------")
message(STATUS "📦 [CMake]: 成功搜集到框架层源码 (src/): \n ${FRAMEWORK_SOURCES}")
message(STATUS "🚀 [CMake]: 成功搜集到业务层源码 (app/): \n ${APP_SOURCES}")
message(STATUS "--------------------------------------------------------")

# 构建安装规则（可选，保持你原本的规范）
install(TARGETS web_server DESTINATION bin)