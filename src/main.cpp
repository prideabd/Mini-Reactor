#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <string.h>
#include <errno.h>

const int MAX_EVENTS = 1024;
const int BUFFER_SIZE = 10;
const int PORT = 8888;

int SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

int main() {
    // 任务1: 封装基础socket，创建、绑定、监听逻辑
    // AF_INET: Address Family - internet(ipv4)
    // 这里其实创建了一个socket结构体，返回了其在内核里的索引
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "创建socket失败: " << strerror(errno) << std::endl;
        return 1;
    }

    // 设置端口复用， 防止服务器重启时报错 "Address already in use"
    int opt = 1;
    // 允许服务器程序在崩溃或重启后，立刻重新绑定原来端口
    // TCP协议：主动关闭连接方在释放连接(崩溃/重启）后，底层端口不会立刻返回，而是进入time_wait状态，大概2-4分钟
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // sockadd_in：自带的ipv4结构体, in代表internet
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    // sin_family设定和socket一致为AF_INET
    server_addr.sin_family = AF_INET;
    // 监听所有网卡
    server_addr.sin_addr.s_addr = INADDR_ANY; 
    // 设置端口号， htons: host to network short
    // 电脑cpu一般采用小端字节序， TCP/IP采用大端字节序
    server_addr.sin_port = htons(PORT);

    // 绑定，其实通过索引listen_fd找到套接字结构体，然后将 server_addr 信息写进去(端口号，协议类型等)
    // 由于 server_addr 定义为 sockaddr_in（专用）, 但bind需要sockaddr类型（通用）
    // 两者更像是集成，前者就是子类，后者是基类
    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "Bind 失败： " << strerror(errno) << std::endl;
        return 1;
    }

    // 默认套接字是主动，用于向别人连接
    // 但服务器需要的是被动
    // listen函数将其强制转化为被动，等待别人连接
    // 128:全连接队列大小, 瞬时最大排队等待数（3次握手成功，但还未被调用）
    if (listen(listen_fd, 128) == -1) {
        std::cerr << "Listen 失败" <<std::endl;
        return 1;
    }

    // 2. 任务2, 把监听 socket 设为非阻塞
    // 防止在主线程通过 accept/recv 等函数取走连接时，
    // 由于客户端崩溃等原因造成直接在 listen_fd 删除连接
    // 这样就会找不到连接，导致阻塞
    SetNonBlocking(listen_fd);
    std::cout << "🚀 Echo Server 成功启动， 正在监听端口：" << PORT << "..." << std::endl;

    // 3. 任务3，实现单线程 epoll 创建
    // 同 socket 一样，创建了一个 struct eventpoll 结构体，返回索引
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        std::cerr << "Epoll 创建失败" << std::endl;
        return 1;
    }

    // 将 listen_fd 注册到 epoll 中, 使用边缘触发 (ET) 模式
    // 水平触发：
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET; // 可读事件和边缘触发
    ev.data.fd = listen_fd;
    // epoll_fd 的底层是一颗红黑树
    // EPOLL_CTL_ADD: 执行添加操作
    // listen_fd：叶子
    // ev：类似配置信息
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        std::cerr << "Epoll 注册 listen_fd 失败" << std::endl;
        return 1;
    }

    struct epoll_event events[MAX_EVENTS];
    // 进入 epoll_wait 核心事件循环
    while (true) {
        // 阻塞等待新事件（listen_fd 有新连接 或者 旧连接有数据传送）
        // events 的多少其实是根据 fd 来判断的，因此新连接无论多少都算一次 event
        // 每个 client_fd 变化都算一次 event
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds == -1) {
            if (errno == EINTR) continue; // 排除信号中断
            std::cerr << "Epoll wait 错误" << std::endl;
            break;
        }
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            // 新连接（未被 accept 之前，使用的都是 listen_fd)
            if (fd == listen_fd) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                // ET 模式下，多个连接进来，需一次取完 (accept)
                while (true) {
                    int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
                    if (client_fd == -1) {
                        // 因为外层是死循环，这里判断就是说accept取完了，但不应该报错，所以跳出去
                        // EAGAIN: error, try again, 取完请稍后再试
                        // EWOULDBLOCK: error, would block, 不拦着，可能被阻塞
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        std::cerr << "Accept 错误" << std::endl;
                        break;
                    }

                    // 现在取出来的 client_fd 也要放入到 epoll 中， 用于数据传送
                    // 那么就需要设置为非阻塞态, 防止 recv 时连接销毁， 被阻塞
                    SetNonBlocking(client_fd);

                    // 配置client信息，准备放入epoll
                    struct epoll_event client_ev;
                    client_ev.events = EPOLLIN | EPOLLET;
                    client_ev.data.fd = client_fd;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev) == -1) {
                        std::cerr << "工业级报警：client_fd " << client_fd << " 挂载红黑树失败！错误码: " << errno << std::endl;
                        
                        // ⚠️ 致命余震：如果挂载树失败了，绝对不能假装无事发生！
                        // 因为如果不挂到树上，epoll 就永远不会盯着它，这个客户端接下来发任何消息，服务器都听不见。
                        // 这就会变成一个永久占用系统资源的“僵尸连接”。
                        
                        close(client_fd); // 必须要果断将其斩杀，释放文件描述符资源！
                        // 继续接客，不要影响其他无辜的连接
                        continue; 
                    }
                    std::cout << "🤝 新客户端连接成功，分配 FD: " << client_fd << std::endl;
                }
            }
            //  旧连接发来数据
            else if (events[i].events & EPOLLIN) {
                char buf[BUFFER_SIZE];
                bool is_closed = false;

                // 循环执行 recv 读取数据，直到EAGAIN
                while (true) {
                    memset(buf, 0, sizeof(buf));
                    // -1 是因为还要存一个'\0'(空字符) (cout输出靠'\0'截止)
                    // ssize_t: signed size type 有符号
                    ssize_t bytes_read = recv(fd, buf, sizeof(buf) - 1, 0);

                    if (bytes_read > 0) {
                        std::cout << "[FD " << fd << " 收到数据]: " << buf << std::endl;
                        // 简单的 echo 回显，收到什么，发回什么
                        // 这里如果数据过大，就会导致问题，
                        send(fd, buf, bytes_read, 0);
                    }
                    else if (bytes_read == 0) {
                        // 客户主动断开连接
                        std::cout << "👋 客户端关闭连接，FD: " << fd << std::endl;
                        is_closed = true;
                        break;
                    }
                    else {
                        // bytes_read == -1
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // 缓冲区数据读完了， 但未断开连接
                            break;
                        }
                        std::cerr << "读取错误，断开连接，FD: " << fd << std::endl;
                        is_closed = true;
                        break;
                    }
                }

                // 如果客户主动关闭或者出错，从 epoll 中移除fd
                if (is_closed == true) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }
        }
    }
    close(listen_fd);
    close(epoll_fd);
    return 0;
}