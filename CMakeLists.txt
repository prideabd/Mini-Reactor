cmake_minimum_required(VERSION 3.10)
project(MiniRector VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 指定头文件搜索路径
include_directories(include)

# 搜索源文件
file(GLOB SRC_FILES "src/*.cpp")

# 寻找 pthread 库
find_package(Threads REQUIRED)

# 生成可执行文件
add_executable(reactor_server ${SRC_FILES})

# 连接 pthread 库
target_link_libraries(reactor_server PRIVATE Threads::Threads)