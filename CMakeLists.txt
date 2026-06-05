cmake_minimum_required(VERSION 3.10)
project(MiniReactor)

# 1. 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2 -pthread")

# 2. 配置头文件搜索路径
# 这样在代码里可以用 #include "Logger.h" 或 #include "reactor/log/LogStream.h"
include_directories(${PROJECT_SOURCE_DIR}/include)

# 3. 递归搜集所有源文件 (包括 src/, src/net/, src/log/)
file(GLOB_RECURSE SOURCES 
    "${PROJECT_SOURCE_DIR}/src/*.cpp"
)

# 4. 排除掉不需要的文件（如果有旧的单线程 main 或者测试文件）
list(REMOVE_ITEM SOURCES "${PROJECT_SOURCE_DIR}/src/single_thread_main.cpp")

# 5. 定义可执行文件目标
add_executable(reactor_server ${SOURCES})

# 6. 链接线程库 (pthread)
target_link_libraries(reactor_server pthread)

# 打印搜集到的源文件，方便调试结构是否正确
message(STATUS "Found sources: ${SOURCES}")

install(TARGETS reactor_server DESTINATION bin)