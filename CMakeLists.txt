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
# 包含第三方库
include_directories(${PROJECT_SOURCE_DIR}/third_party)
# 包含 app 目录本身
include_directories(${PROJECT_SOURCE_DIR}/app)

# ============================================================================
# 3. 编译框架层（编译为静态库 libreactor.a）
# ============================================================================
# 物理隔离，这里只搜集 src/ 目录下的框架源文件，杜绝将业务搅和进来
file(GLOB_RECURSE FRAMEWORK_SOURCES "${PROJECT_SOURCE_DIR}/src/*.cpp")

# 排除可能干扰构建的过时单线程测试入口
# 目前路径不对，后续修改
# list(REMOVE_ITEM FRAMEWORK_SOURCES "${PROJECT_SOURCE_DIR}/src/single_thread_main.cpp")

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
    ${PROJECT_SOURCE_DIR}/app/repository/BlacklistManager.cpp
    ${PROJECT_SOURCE_DIR}/app/common/AppUtils.cpp
    ${PROJECT_SOURCE_DIR}/third_party/sqlite/sqlite3.c
)

# 定义你的最终交付应用目标（这里我们更名为更有应用标识度的 web_server）
add_executable(web_server ${APP_SOURCES})

# 每次构建后，自动把 config 与 www 同步到可执行文件所在目录
add_custom_command(TARGET web_server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_SOURCE_DIR}/config $<TARGET_FILE_DIR:web_server>/config
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_SOURCE_DIR}/www $<TARGET_FILE_DIR:web_server>/www
    COMMENT "🔄 同步 config 与 www 资源到可执行文件目录"
)

# ============================================================================
# 5. 依赖纽带链接
# ============================================================================
# 让业务程序 web_server 强力链接刚刚打好的框架库 reactor 以及操作系统的 pthread 库
target_link_libraries(web_server PRIVATE reactor pthread dl)

# ============================================================================
# 5. 构建阶段一 L4 反向代理测试服务：proxy_server
# ============================================================================
set(PROXY_APP_SOURCES
    ${PROJECT_SOURCE_DIR}/app/proxy_main.cpp
)

add_executable(proxy_server ${PROXY_APP_SOURCES})

target_link_libraries(proxy_server PRIVATE
    reactor
    pthread
    dl
)

# 注意：
# proxy_server 阶段一只做 L4 透明转发，不需要 config / www，
# 所以这里不添加 POST_BUILD copy_directory。

# ============================================================================
# 6. 工程构建调试打印
# ============================================================================
message(STATUS "--------------------------------------------------------")
message(STATUS "📦 [CMake]: 成功搜集到框架层源码 (src/): \n ${FRAMEWORK_SOURCES}")
message(STATUS "🚀 [CMake]: Web 服务源码: \n ${APP_SOURCES}")
message(STATUS "🔀 [CMake]: Proxy 服务源码: \n ${PROXY_APP_SOURCES}")
message(STATUS "--------------------------------------------------------")

# ============================================================================
# 7. 安装规则
# ============================================================================
install(TARGETS web_server proxy_server DESTINATION bin)