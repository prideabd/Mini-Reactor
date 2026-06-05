#include "reactor/net/Channel.h"
#include "reactor/net/EventLoop.h"

namespace reactor::net {

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1)
{}

void Channel::HandleEvent() {
    // 1. 对端断开连接异常处理
    // 异常断开不受 events_ 控制，内核强行通知，必须直接处理
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (close_callback_) {
            close_callback_();
        }
    }

    // 2. 错误处理
    // 错误处理同样不受 events_ 控制
    if (revents_ & EPOLLERR) {
        if (error_callback_) {
            error_callback_();
        }
    }

    // 3. 可读事件
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (read_callback_) {
            read_callback_();
        }
    }

    // 4. 可写事件
    if (revents_ & EPOLLOUT) {
        if (write_callback_) {
            write_callback_();
        }
    }
}

void Channel::EnableReading() {
    events_ |= (EPOLLIN | EPOLLET);
    Update();
}

void Channel::DisableReading() {
    events_ &= ~EPOLLIN;
    Update();
}

void Channel::EnableWriting() {
    events_ |= EPOLLOUT;
    Update();
}

void Channel::DisableWriting() {
    events_ &= ~EPOLLOUT;
    Update();
}

void Channel::DisableAll() {
    events_ = 0;
    Update();
}

void Channel::Remove() {
    loop_->RemoveChannel(this);
}

void Channel::Update() {
    loop_->UpdateChannel(this);
}

} // namespace reactor::net