# SpanIO 使用指南

> 适用版本：0.3.0（API 已冻结，见设计文档 6.11）。
> 本指南只讲怎么用；实现原理见 [`SpanIO.md`](SpanIO.md)。
> 指南里标了 `snippet` 的代码块与 `tests/test_guide.cpp` 中编译并运行的代码逐字相同，由 CTest 测试 `guide_snippets_match` 保证。

## 目录

- [0. 简介](#0-简介)
- [1. 快速开始](#1-快速开始)
- [2. 核心概念](#2-核心概念)
- [3. TCP](#3-tcp)
- [4. UDP](#4-udp)
- [5. 高层服务端](#5-高层服务端)
- [6. 定时器](#6-定时器)
- [7. 域名解析](#7-域名解析)
- [8. 多线程与 strand](#8-多线程与-strand)
- [9. 错误码参考](#9-错误码参考)
- [10. 平台说明](#10-平台说明)
- [11. 常见问题](#11-常见问题)
- [附录 A：API 速查](#附录-aapi-速查)
- [附录 B：示例程序](#附录-b示例程序)

---

## 0. 简介

SpanIO 是一个只有头文件的 C++17 网络库，支持 Windows 和 Linux，不依赖任何第三方库。

- **一套接口，两个平台**：Windows 默认用 IOCP，Linux 用 epoll；用户代码完全相同。
- **完成式回调**：发起一个异步操作，操作完成后回调被调用，此时数据已经在你的缓冲区里。
- **从底层到高层**：既有 `tcp::socket`、`udp::socket` 这样的基础对象，也有 `tcp_server`、`udp_server` 这样只需写回调的服务端。

接口风格参照 Asio（`io_context`、`async_read_some`、`strand` 等名字含义相同），但更小：没有 Boost，没有完成令牌之类的模板机制，回调就是普通的函数对象。

要求：C++17 编译器。已验证的环境是 Windows 上的 Visual Studio 2026，以及 Ubuntu 24.04 上的 g++ 13.3。

---

## 1. 快速开始

### 1.1 引入

SpanIO 只有头文件，把 `include/` 加到包含路径即可。

**CMake**：

```cmake
add_subdirectory(SpanIO)                       # 仓库所在目录
target_link_libraries(my_app PRIVATE spanio::spanio)
```

`spanio::spanio` 会带上包含路径、C++17、线程库，以及 Windows 上的 `ws2_32`。

**不用 CMake，单文件编译**：

```sh
# Linux
g++ -std=c++17 -Iinclude my_app.cpp -o my_app -lpthread
# Windows（VS 开发者命令行）
cl /std:c++17 /EHsc /Iinclude my_app.cpp ws2_32.lib
```

Windows 上 `/EHsc` 不能省：没有它，异常抛出时不会析构对象。

代码里包含总入口即可，也可以只包含用到的头文件（编译更快）：

```cpp
#include "spanio/spanio.h"      // 全部公开 API
#include "spanio/tcp_server.h"  // 或只要 tcp_server
```

### 1.2 第一个程序：回显服务

<!-- snippet: quick_start -->
```cpp
// Echo every byte back to the client that sent it
void serve_echo(spanio::tcp_server& server, unsigned threads)
{
    auto on_message = [](const spanio::tcp_connection_ptr& connection, std::string_view data)
    {
        connection->send(data);
    };
    server.on_message(on_message);
    server.run(threads);    // blocks until server.stop()
}
```

再加一个 `main`：

```cpp
int main()
{
    spanio::tcp_server server(8080);    // 监听 0.0.0.0:8080
    serve_echo(server, 4);              // 4 个线程
}
```

编译运行后，用 `nc localhost 8080`（或 `telnet`）连上去，输入什么就会收到什么。仓库里的 `examples/echo_server.cpp` 是完整版本：带命令行参数，并为每个连接打印连接、收到的数据和断开的日志（`--quiet` 关闭）；`examples/tcp_client.cpp` 是对应的客户端。

### 1.3 编译开关

| 开关 | 作用 |
|---|---|
| `SPANIO_ENABLE_LOG`（CMake 选项 `-DSPANIO_ENABLE_LOG=ON`） | 关键路径出错时向 stderr 打印日志，排查问题时打开 |
| `SPANIO_WINDOWS_USE_AFD_REACTOR`（CMake 选项同名） | Windows 上不用原生 IOCP，改用就绪通知模拟（与 Linux 同一套代码路径），一般不需要 |

---

## 2. 核心概念

### 2.1 `io_context` 与 `run()`

`io_context` 是事件循环。所有 I/O 对象构造时都绑定一个 `io_context`，它们的回调都在 `io_context::run()` 里执行。

- `run()` 一直执行回调，直到**没有工作**为止。工作包括：排队的回调、未完成的 I/O 操作、未到期的定时器等待、存活的 `executor_work_guard`。
- 没有工作时 `run()` 立即返回；它返回后，context 处于“已停止”状态。
- 可以多个线程同时调用 `run()`，回调会分散到这些线程上执行（见第 8 节）。
- `stop()` 让所有 `run()` 尽快返回。

如果你希望 `run()` 一直等新任务（例如工作线程），用 `executor_work_guard`：

<!-- snippet: work_guard -->
```cpp
spanio::io_context context;
spanio::executor_work_guard work = spanio::make_work_guard(context);
auto run_context = [&context]()
{
    context.run();      // keeps waiting for handlers while the guard lives
};
std::thread worker(run_context);

context.post(handler);  // any thread may post; the worker runs it

work.reset();           // no more work coming: run() returns when idle
worker.join();
```

`run()` 返回或 `stop()` 之后，要先 `restart()` 才能再次 `run()`：

<!-- snippet: restart -->
```cpp
context.restart();      // a context that ran out of work, or was stopped,
context.post(handler);  // needs restart() before run() executes handlers again
context.run();
```

### 2.2 回调的四条规则

1. **从不在发起函数里执行**。`async_xxx()` 和 `post()` 总是立即返回，回调一定在之后的 `run()` 里执行；即使操作立刻就失败了（比如 socket 没打开），也是如此。所以发起操作之后再设置回调要用的状态，是安全的。
2. **恰好执行一次**。每个发起的操作都会回调，成功、失败、被取消都一样，而且只回调一次。
3. **缓冲区要活到回调执行**。`spanio::buffer(...)` 不拷贝数据，只记住地址；回调执行前，缓冲区不能释放或移动。
4. **每个 socket 同时最多一个读、一个写**。例如上一个 `async_read_some` 的回调还没执行，就不能再发起一个读。这条不做运行时检查，违反的后果是未定义的。

<!-- snippet: never_inline -->
```cpp
bool called = false;
auto handler = [&called]()
{
    called = true;
};
context.post(handler);
// called is still false here: the handler runs inside run()
```

### 2.3 `post`、`dispatch` 与 executor

- `post(h)`：把 `h` 排进队列，永远不在调用处执行。
- `dispatch(h)`：如果当前线程正在执行这个 context 的 `run()`，就立刻执行 `h`；否则同 `post`。

<!-- snippet: post_dispatch -->
```cpp
std::vector<std::string> order;
auto inner = [&order]()
{
    order.push_back("inner");
};
auto outer = [&]()
{
    context.dispatch(inner);    // inside run(): runs here, at once
    context.post(inner);        // queued: runs after outer returns
    order.push_back("outer");
};
context.post(outer);
context.run();
// order is: inner, outer, inner
```

`context.get_executor()` 返回一个可拷贝的轻量句柄，也有 `post` / `dispatch`；`spanio::make_work_guard(context)` 生成上一节用到的 work guard。

### 2.4 错误处理

所有错误都以 `std::error_code` 返回，**库不抛异常**（只有下面列出的几处构造函数和便捷函数例外）。错误码分三类：

| 类别 | 例子 | 怎么判断 |
|---|---|---|
| 系统错误 | 连接被拒绝、对端重置、端口被占用、操作被取消 | `ec == std::errc::connection_refused` |
| `spanio.misc` | 对端正常关闭、数据报被截断、服务端超时 | `ec == spanio::error::misc_errors::eof` |
| `spanio.netdb` | 域名解析失败 | `ec == spanio::error::netdb_errors::host_not_found` |

异步操作的回调第一个参数就是错误码：

<!-- snippet: error_async -->
```cpp
auto on_read = [&](std::error_code ec, std::size_t bytes)
{
    if (ec == spanio::error::misc_errors::eof)
    {
        peer_closed = true;     // the peer closed the connection normally
    }
    else if (ec == std::errc::operation_canceled)
    {
        // we closed or cancelled the socket ourselves
    }
    else if (ec)
    {
        failure = ec;           // a real error, e.g. std::errc::connection_reset
    }
    else
    {
        received += bytes;
    }
};
socket.async_read_some(spanio::buffer(data), on_read);
```

同步函数按下面的约定报错：

- 没有返回值的（`open`、`bind`、`listen`、`shutdown`、`assign`）**返回** `std::error_code`；
- 有返回值的（`local_endpoint`、`remote_endpoint`、`resolve`、`make_address`）通过最后一个 `std::error_code&` 参数报错；
- 构造函数失败时**抛** `std::system_error`：`io_context()`、`tcp::acceptor(ctx, endpoint)`、`tcp_server(...)`、`udp_server(...)`；
- 不带 `ec` 参数的便捷版本失败时抛 `std::system_error`，例如 `make_address(string)`。

<!-- snippet: error_sync -->
```cpp
std::error_code ec;
spanio::ip::address bad = spanio::ip::make_address("not an address", ec);
// ec == std::errc::invalid_argument; nothing was thrown

try
{
    spanio::ip::make_address(std::string("300.1.1.1"));
}
catch (const std::system_error& e)
{
    // the overload without ec throws; e.code() == std::errc::invalid_argument
    thrown = e.code() == std::errc::invalid_argument;
}
```

各种情况具体对应哪个错误码，见第 9 节。

---

## 3. TCP

### 3.1 地址与端点

`ip::address` 是 IPv4 或 IPv6 地址，`tcp::endpoint`（与 `udp::endpoint` 同为 `ip::endpoint`）是地址加端口。`make_address` 只解析数字形式的地址，主机名要用第 7 节的解析器。

<!-- snippet: addresses -->
```cpp
spanio::ip::address v4 = spanio::ip::make_address("192.168.1.10");
spanio::ip::address v6 = spanio::ip::make_address("fe80::1");
spanio::tcp::endpoint local(spanio::ipv4::loopback(), 8080);    // 127.0.0.1:8080
spanio::tcp::endpoint all_v6(spanio::ipv6::any(), 8080);        // [::]:8080
std::string text = local.address().to_string();                 // "127.0.0.1"
std::uint16_t port = local.port();                               // 8080
```

### 3.2 客户端

一个 TCP 客户端通常是：连接 → 发送 → 读取 → 关闭。

```cpp
spanio::tcp::socket socket(context);
socket.async_connect(endpoint, on_connected);          // 未打开时自动 open
socket.async_write(spanio::buffer(request), on_written);
socket.async_read(spanio::buffer(reply), on_read);     // 读满 reply 才回调
socket.shutdown(spanio::tcp::shutdown_type::send);     // 告诉对端：我发完了
socket.close();                                        // 挂起的操作以 operation_canceled 结束
```

每一步在上一步的回调里发起，形成一条链。完整、可运行的例子是 `examples/tcp_client.cpp`：解析主机名、逐个地址尝试连接、发消息、读回显，整个过程有 5 秒时限。

### 3.3 服务端：accept 循环

`tcp::acceptor` 监听端口，`async_accept` 每次接受一个连接，回调里拿到新的 `tcp::socket`。常见写法是：每个连接一个会话对象（用 `shared_ptr` 管理生命周期），接受完一个就立刻发起下一个 `async_accept`。

<!-- snippet: accept_loop -->
```cpp
// One client: echoes what it reads until the client closes
class session : public std::enable_shared_from_this<session>
{
public:
    explicit session(spanio::tcp::socket socket)
        : m_socket(std::move(socket))
    {
    }

    void start()
    {
        read();
    }

private:
    void read()
    {
        auto self = shared_from_this();     // keeps the session alive while waiting
        auto on_read = [self](std::error_code ec, std::size_t bytes)
        {
            if (!ec)
            {
                self->write(bytes);
            }
        };
        m_socket.async_read_some(spanio::buffer(m_data), on_read);
    }

    void write(std::size_t bytes)
    {
        auto self = shared_from_this();
        auto on_written = [self](std::error_code ec, std::size_t)
        {
            if (!ec)
            {
                self->read();
            }
        };
        m_socket.async_write(spanio::buffer(m_data, bytes), on_written);
    }

    // The connection
    spanio::tcp::socket m_socket;
    // Bytes read, then written back
    char m_data[4096];
};

// Accept clients one after another, each into its own session
void accept_next(spanio::tcp::acceptor& acceptor)
{
    auto on_accept = [&acceptor](std::error_code ec, spanio::tcp::socket peer)
    {
        if (ec)
        {
            return;     // e.g. operation_canceled after acceptor.close()
        }
        std::make_shared<session>(std::move(peer))->start();
        accept_next(acceptor);
    };
    acceptor.async_accept(on_accept);
}
```

启动方式：

```cpp
spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::any(), 8080));
accept_next(acceptor);
context.run();
```

自己写会话要处理很多细节（发送队列、关闭、超时、多线程）。如果只是想写一个服务，直接用第 5 节的 `tcp_server`。

### 3.4 读写函数怎么选

| 函数 | 什么时候回调 | 用途 |
|---|---|---|
| `async_read_some(buf, h)` | 读到**任意**字节数就回调 | 不知道对方会发多少，例如回显、流式数据 |
| `async_read(buf, h)` | **读满** `buf` 才回调；对端先关闭报 `eof` 和已读字节数 | 已知长度，例如先读 4 字节长度头，再读消息体 |
| `async_write_some(buf, h)` | 写出**任意**字节数就回调 | 很少直接用 |
| `async_write(buf, h)` | **全部写完**才回调 | 一般都用它 |

回调签名都是 `void(std::error_code ec, std::size_t bytes)`。非空缓冲区读到 0 字节表示对端正常关闭，报 `error::misc_errors::eof`。

### 3.5 完整示例

`examples/tcp_client.cpp`（`tcp_client <主机> <端口> <消息>`）把本节的内容串了起来，再配合第 6 节的定时器做超时控制。核心是一个会话类：

- `start()`：启动 5 秒定时器，异步解析主机名；
- 解析完成：`tcp::async_connect` 按地址列表逐个连接；
- 连上后 `async_write` 发消息，再 `async_read` 读回同样长度；
- 任何一步失败、或定时器先到期，都走到 `finish()`，记录结果并关闭 socket，`run()` 随即返回。

```sh
echo_server 8080
tcp_client localhost 8080 hello      # 输出 hello
```

---

## 4. UDP

`udp::socket` 的一次操作就是一个数据报：

```cpp
spanio::udp::socket socket(context);
socket.open(AF_INET);
socket.bind(spanio::udp::endpoint(spanio::ipv4::any(), 9000));   // 服务端才需要 bind
socket.async_receive_from(spanio::buffer(data), on_received);     // h(ec, bytes, sender)
socket.async_send_to(spanio::buffer(reply), peer, on_sent);       // 未打开时自动 open
```

要点：

- **截断**：数据报比缓冲区大时，回调收到 `error::misc_errors::datagram_truncated`，能放下的部分已在缓冲区里，其余丢失。缓冲区开到 `spanio::kMaxUdpPayload`（65507）就不会截断。
- **空数据报**：成功，0 字节。UDP 没有“对端关闭”，也就没有 `eof`。
- **丢包没有错误**：发出去的数据报没人回，接收就会一直等。客户端必须自己加超时（第 6 节）。

完整示例是 `examples/udp_client.cpp`（`udp_client <主机> <端口> <消息>`）：同步解析主机名（优先 IPv4）、发一个数据报、2 秒内等回复。服务端可以用 `examples/udp_echo_server.cpp`。

---

## 5. 高层服务端

### 5.1 `tcp_server`

`tcp_server` 帮你管理 accept 循环、连接的生命周期、发送队列、分帧和线程，你只写回调：

<!-- snippet: tcp_server -->
```cpp
spanio::tcp_server_options options;
options.m_max_connections = 1000;
options.m_idle_timeout = std::chrono::seconds(60);    // close silent clients
spanio::tcp_server server(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), port), options);

auto on_connect = [](const spanio::tcp_connection_ptr& connection)
{
    connection->user_data() = 0;    // per-connection state: lines seen
    connection->send("welcome\n");
};
auto on_message = [](const spanio::tcp_connection_ptr& connection, std::string_view line)
{
    ++std::any_cast<int&>(connection->user_data());
    if (line == "quit")
    {
        connection->close();        // sends what is queued, then closes
        return;
    }
    connection->send(std::string(line) + "\n");
};
auto on_close = [&](const spanio::tcp_connection_ptr& connection, std::error_code reason)
{
    messages = std::any_cast<int>(connection->user_data());
    close_reason = reason;          // empty after close(); eof if the client left
};
server.on_connect(on_connect);
server.on_message(on_message);
server.on_close(on_close);
server.set_framer(spanio::line_framer());   // one on_message per line
server.start();     // the caller runs the context; server.run(n) runs its own
```

**三个回调的保证**：

- `on_connect` 在该连接的第一次读取之前返回。
- 同一个连接的回调**从不同时执行**；不同连接的回调可能在不同线程上同时执行，所以连接之间共享的数据要加锁（或者 `run(1)` 单线程运行）。
- `on_close` 每个连接恰好一次，并且是该连接的最后一个回调。
- `on_message` 里的 `std::string_view` 只在本次调用期间有效，要保存就拷贝。
- `on_connect` / `on_message` 抛异常会以 `handler_failed` 关闭该连接，服务继续运行。

**连接对象 `tcp_connection`**（回调里拿到的是 `tcp_connection_ptr`，即 `shared_ptr`）：

| 方法 | 说明 |
|---|---|
| `send(data)` | 排队发送，可在任意线程调用；队列超过上限会以 `send_queue_full` 关闭连接并返回 `false` |
| `close()` | 发完已排队的数据再关闭（最多等 `m_close_timeout`） |
| `abort()` | 立即关闭，丢弃未发出的数据 |
| `user_data()` | `std::any&`，挂每个连接自己的业务数据 |
| `remote_endpoint()` / `is_open()` | 对端地址 / 是否还能发送 |

**选项 `tcp_server_options`**：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `m_max_connections` | 10000 | 同时服务的连接数，超出的连接被接受后立即关闭；0 为不限 |
| `m_max_message_size` | 1 MiB | 一条帧的最大长度（只在设置了 framer 时生效） |
| `m_max_send_queue_bytes` | 16 MiB | 一个连接排队待发的最大字节数，防止不读数据的客户端耗尽内存 |
| `m_read_buffer_size` | 64 KiB | 每次读取的缓冲区大小 |
| `m_idle_timeout` | 0（关闭） | 既没收到数据、发送也没有进展超过这么久，就以 `idle_timeout` 关闭 |
| `m_close_timeout` | 30 秒 | `close()` 等待发送完成的最长时间，超时以 `close_timeout` 关闭；0 为一直等 |

**运行与停止**：

- `tcp_server server(port)` 自己拥有一个 `io_context`，用 `server.run(n)` 在 n 个线程上运行（阻塞，直到 `stop()`）。
- `tcp_server server(context, endpoint)` 用你的 `io_context`，调 `server.start()` 后由你运行 context。
- `stop()`：立即关闭所有连接，未发出的数据丢弃。
- `stop_gracefully(grace)`：停止接受新连接，对每个连接 `close()`；超过 `grace` 还没关的强制关闭（`close_timeout`）。
- 一个 `tcp_server` 只能启动一次。

`on_close` 收到的原因：本端 `close()` / `abort()` / `stop()` 为空；客户端正常断开为 `eof`；其他见第 9 节。

### 5.2 分帧

TCP 是字节流，一次读到的数据可能是半条消息，也可能是好几条。设置 framer 后，`on_message` 每次收到的就是一条完整的消息。

- `spanio::line_framer()`：按行切分（`\n` 或 `\r\n`，不含行尾）。
- `spanio::length_prefix_framer()`：每条消息前面是 4 字节大端长度；发送时用 `spanio::encode_length_prefixed(payload)` 加上长度头。

<!-- snippet: line_framer -->
```cpp
spanio::framer lines = spanio::line_framer();
spanio::frame_result first = lines("abc\r\ndef");
// first.m_payload == "abc" (the line ending is removed), first.m_consumed == 5
```

也可以自己写：framer 是一个函数，参数是还没被消费的输入，返回 `frame_result`。`m_consumed == 0` 表示数据还不够；`m_error = true` 表示数据格式错误，连接会以 `framing_error` 关闭。

<!-- snippet: custom_framer -->
```cpp
// Every message is a record of exactly 8 bytes
constexpr std::size_t kRecordSize = 8;
auto fixed_records = [](std::string_view input)
{
    spanio::frame_result result;            // m_consumed == 0: need more data
    if (input.size() >= kRecordSize)
    {
        result.m_payload = input.substr(0, kRecordSize);
        result.m_consumed = kRecordSize;
    }
    return result;
};
// server.set_framer(fixed_records);
```

### 5.3 `udp_server`

<!-- snippet: udp_server -->
```cpp
spanio::udp_server server(context, spanio::udp::endpoint(spanio::ipv4::loopback(), port));
spanio::udp_server* self = &server;
auto on_datagram = [self](const spanio::udp::endpoint& from, std::string_view data)
{
    self->send_to(from, data);      // queue a reply to the sender
};
server.on_datagram(on_datagram);
server.start();
```

- `on_datagram` 的调用从不重叠，不管有几个线程在运行。
- `send_to(peer, data)` 可在任意线程调用；队列满时拒绝这个数据报并返回 `false`。
- 单个数据报出错不会停止服务，只计入统计：

<!-- snippet: udp_stats -->
```cpp
const spanio::udp_server_stats stats = server.stats();
// stats.m_received, m_sent, m_queue_dropped, m_send_failed, m_truncated
```

选项 `udp_server_options`：`m_max_datagram_size`（接收缓冲区，默认 65507，超长的数据报丢弃并计入 `m_truncated`）、`m_max_send_queue_bytes`（默认 16 MiB）。运行与停止的方式同 `tcp_server`；`stop_gracefully(grace)` 停止接收，但会把已排队的数据发完。

---

## 6. 定时器

`steady_timer` 用单调时钟，不受系统改时间的影响。

- `expires_after(d)` / `expires_at(t)` 设置到期时间（会取消正在等待的 `async_wait`）。
- `async_wait(h)`：到期时 `h(ec)` 被调用，`ec` 为空；被取消时 `ec == operation_canceled`。
- `cancel()` 取消等待，返回取消的个数。定时器已经到期、回调已在排队时，`cancel()` 返回 0，回调仍收到成功。
- 未到期的等待算作工作，`run()` 会等它。定时器不能拷贝或移动。

**周期定时器**：在回调里重新设置到期时间并再次等待。从上次的到期时间累加，而不是从“现在”，就不会越跑越慢。

<!-- snippet: periodic_timer -->
```cpp
const std::chrono::milliseconds kPeriod(20);
spanio::steady_timer timer(context);
int ticks = 0;
std::function<void(std::error_code)> on_tick;
on_tick = [&](std::error_code ec)
{
    if (ec || ++ticks == 3)
    {
        return;             // cancelled, or done: no new wait, run() can return
    }
    timer.expires_at(timer.expiry() + kPeriod);     // from the last expiry: no drift
    timer.async_wait(on_tick);
};
timer.expires_after(kPeriod);
timer.async_wait(on_tick);
context.run();
```

**给操作加超时**：同时启动定时器和操作，谁先完成就取消另一个。

<!-- snippet: read_timeout -->
```cpp
spanio::steady_timer deadline(context, std::chrono::milliseconds(100));
auto on_deadline = [&socket](std::error_code ec)
{
    if (!ec)
    {
        socket.cancel();    // too late: the read ends with operation_canceled
    }
};
auto on_read = [&](std::error_code ec, std::size_t)
{
    deadline.cancel();      // the read ended first: stop the timer
    read_result = ec;
};
deadline.async_wait(on_deadline);
socket.async_read_some(spanio::buffer(data), on_read);
context.run();
```

---

## 7. 域名解析

`tcp::resolver` / `udp::resolver` 把主机名和端口（或服务名）解析成地址列表。每个解析器有一个后台线程执行系统的 `getaddrinfo`，同一个解析器的请求依次处理。

**同步**（会阻塞调用线程，适合程序启动时）：

<!-- snippet: resolve_sync -->
```cpp
spanio::tcp::resolver resolver(context);
std::error_code ec;
std::vector<spanio::tcp::endpoint> endpoints = resolver.resolve("localhost", "8080", ec);
// on success: e.g. [::1]:8080 and 127.0.0.1:8080, in the order to try them
```

**异步**：

<!-- snippet: resolve_async -->
```cpp
auto on_resolved = [&found](std::error_code ec, std::vector<spanio::tcp::endpoint> endpoints)
{
    if (!ec)
    {
        found = endpoints.size();
    }
};
spanio::resolve_options options;
options.m_family = spanio::resolve_family::v4;  // IPv4 addresses only
options.m_numeric_host = true;                  // no DNS: the host is an address
resolver.async_resolve("127.0.0.1", "80", on_resolved, options);
context.run();
```

`resolve_options`：

| 字段 | 说明 |
|---|---|
| `m_family` | `resolve_family::any`（默认）/ `v4` / `v6` |
| `m_numeric_host` | 主机必须是数字地址，不做 DNS 查询 |
| `m_numeric_service` | 端口必须是数字，不查服务名 |

- `cancel()`：挂起的请求立即以 `operation_canceled` 回调。正在执行的系统查询无法中断，结果会被丢弃；解析器析构时要等它结束。
- 失败时的错误码在 `spanio.netdb` 类别：`host_not_found`、`host_not_found_try_again`、`no_data`、`no_recovery`、`service_not_found`。

**按地址列表连接**：解析结果可能有多个地址（例如 `localhost` 先给 `::1`、再给 `127.0.0.1`），`tcp::async_connect` 逐个尝试，直到连上：

```cpp
spanio::tcp::async_connect(socket, endpoints, on_connected);   // h(ec, const tcp::endpoint& connected)
```

全部失败时报最后一个地址的错误；列表为空报 `host_not_found`；连接过程中 `close()` 或 `cancel()` 了 socket，立即以 `operation_canceled` 结束。完整用法见 `examples/tcp_client.cpp`。

---

## 8. 多线程与 strand

多个线程同时 `run()` 时，回调会在这些线程上并行执行。如果几个回调要访问同一份数据，有两种办法：加锁，或者把这些回调放到同一个 **strand** 上。

绑定到同一个 strand 的回调**不会同时执行**，并且按排队的顺序执行，所以它们共享的数据不需要锁；不同 strand 之间照常并行。

- `spanio::make_strand(context)`：新建一个 strand；拷贝出来的 strand 是同一个串行域。
- `strand.post(h)` / `strand.dispatch(h)`：把普通任务放到 strand 上。
- `spanio::bind_executor(strand, h)`：把异步操作的回调放到 strand 上，用法是把它当作回调传给 `async_xxx`。

<!-- snippet: strand -->
```cpp
constexpr int kIncrements = 10000;
constexpr int kThreads = 4;
spanio::io_context context;
auto counter_strand = spanio::make_strand(context);
long counter = 0;               // shared, but only touched on the strand
auto increment = [&counter]()
{
    ++counter;
};
for (int i = 0; i < kIncrements; ++i)
{
    counter_strand.post(increment);
}

// Completion handlers join the strand through bind_executor
spanio::steady_timer timer(context, std::chrono::milliseconds(10));
auto on_timer = [&counter](std::error_code)
{
    ++counter;
};
timer.async_wait(spanio::bind_executor(counter_strand, on_timer));

auto run_context = [&context]()
{
    context.run();
};
std::vector<std::thread> threads;
for (int i = 0; i < kThreads; ++i)
{
    threads.emplace_back(run_context);
}
for (std::thread& thread : threads)
{
    thread.join();
}
// counter == kIncrements + 1, with no lock
```

`tcp_server` 已经保证同一个连接的回调不重叠，用它时一般不需要 strand；strand 用在你自己管理的、被多个异步操作共享的状态上。

---

## 9. 错误码参考

| 情况 | 错误码 |
|---|---|
| 取消：`close()`、`cancel()`、定时器 `cancel()`、解析器 `cancel()` | `std::errc::operation_canceled` |
| 在未打开的 socket 上发起操作 | `std::errc::bad_file_descriptor` |
| 对已打开的 socket 再 `open()` / `assign()` | `std::errc::invalid_argument` |
| TCP 对端正常关闭（读到 0 字节） | `error::misc_errors::eof` |
| 连接被拒绝 | `std::errc::connection_refused` |
| 对端重置连接（读，或重置后第一次失败的写） | `std::errc::connection_reset` |
| 本端 `shutdown(send)` 后再写 | `std::errc::broken_pipe` |
| 地址已被占用 | `std::errc::address_in_use` |
| 数据报比接收缓冲区大 | `error::misc_errors::datagram_truncated` |
| `tcp_server`：消息过长 / 发送队列满 / 帧格式错误 / 回调抛异常 | `error::misc_errors::message_too_large` / `send_queue_full` / `framing_error` / `handler_failed` |
| `tcp_server`：空闲超时 / 关闭超时 | `error::misc_errors::idle_timeout` / `close_timeout` |
| 域名解析失败 | `error::netdb_errors::host_not_found` 等 |
| `tcp::async_connect` 收到空地址列表 | `error::netdb_errors::host_not_found` |

这些在 Windows 和 Linux 上相同。Windows 上 C++ 标准库认不出的几个系统错误码，SpanIO 已经转换成对应的 `std::errc`，所以总是用 `ec == std::errc::xxx` 判断，不要比较 `ec.value()`。

已知的一处平台差异：对端重置连接后，第一次写失败两边都报 `connection_reset`；如果收到错误后还继续写，Linux 报 `broken_pipe`，Windows 仍报 `connection_reset`。收到任何错误后都应关闭连接，就不会遇到这个差异。

推荐的回调写法：先判断 `operation_canceled`（多半是你自己关闭的，直接返回），再判断 `eof`（正常结束），最后才是真正的错误。

---

## 10. 平台说明

| 平台 | 默认后端 | 说明 |
|---|---|---|
| Windows | 原生 IOCP（`AcceptEx` / `ConnectEx` / `WSARecv` / `WSASend`） | 定义 `SPANIO_WINDOWS_USE_AFD_REACTOR` 可改用就绪通知模拟 |
| Linux | epoll（水平触发）+ 就绪后由库完成读写 | io_uring 后端计划在 M5 提供 |

`context.backend_name()` 返回当前后端的名字，可以打到启动日志里。

- **日志**：定义 `SPANIO_ENABLE_LOG` 后，关键路径的错误打印到 stderr。
- **Windows**：需要链接 `ws2_32.lib`（CMake 目标会自动带上），编译时要 `/EHsc`。Winsock 的初始化由库自动完成。
- **Linux**：需要 `-lpthread`（较新的 glibc 可省）。

---

## 11. 常见问题

**`run()` 为什么立刻返回？**
没有工作了。确认调用 `run()` 之前已经发起了异步操作；如果要让 `run()` 一直等新任务，用 `executor_work_guard`（2.1）。`run()` 返回过一次后，要先 `restart()` 才能再 `run()`。

**回调为什么没执行？**
回调只在 `run()` 里执行。检查：有没有线程在调用 `run()`；context 是否已经 `stop()`；操作是否真的发起了。

**能在回调里关闭 socket 吗？**
可以。`close()` 会让这个 socket 上挂起的其他操作以 `operation_canceled` 回调。

**能在别的线程调用 `send` 吗？**
`tcp_connection::send()` 和 `udp_server::send_to()` 可以在任意线程调用。底层的 `tcp::socket` / `udp::socket` 不是线程安全的：同一个 socket 只应在一个线程（或一个 strand）上使用。

**程序怎么优雅退出？**
`tcp_server` / `udp_server` 用 `stop_gracefully(grace)`。自己管理的对象：关闭 acceptor 和所有 socket、取消定时器，没有工作后 `run()` 自然返回；或者直接 `context.stop()`。

**缓冲区可以用局部变量吗？**
只要它活到回调执行。最常见的错误是在函数里声明一个局部数组，发起 `async_read_some` 后函数就返回了。把缓冲区放进会话对象里（3.3）。

---

## 附录 A：API 速查

| 头文件 | 主要内容 |
|---|---|
| `spanio/spanio.h` | 包含下面全部 |
| `spanio/version.h` | `SPANIO_VERSION_MAJOR/MINOR/PATCH`、`SPANIO_VERSION`、`SPANIO_VERSION_STRING` |
| `spanio/io_context.h` | `io_context`（`run/run_one/poll/poll_one/stop/stopped/restart/post/dispatch/get_executor/backend_name`）、`executor_work_guard`、`make_work_guard` |
| `spanio/strand.h` | `strand<Executor>`、`make_strand`、`bind_executor`、`executor_binder` |
| `spanio/timer.h` | `steady_timer`（`expires_after/expires_at/expiry/async_wait/cancel`） |
| `spanio/ip.h` | `ip::address`、`ip::make_address`、`ip::endpoint`、`ipv4::any/loopback`、`ipv6::any/loopback` |
| `spanio/buffer.h` | `mutable_buffer`、`const_buffer`、`spanio::buffer(...)`（指针+长度、数组、`std::string`、`std::vector<char>`、`std::string_view`） |
| `spanio/error.h` | `error::misc_errors`、`error::netdb_errors` 及其类别 |
| `spanio/tcp.h` | `tcp::endpoint`、`tcp::socket`（`open/assign/close/cancel/shutdown/async_connect/async_read_some/async_read/async_write_some/async_write`）、`tcp::acceptor`（`open/bind/listen/async_accept`）、`tcp::shutdown_type` |
| `spanio/udp.h` | `udp::endpoint`、`udp::socket`（`open/bind/close/cancel/async_receive_from/async_send_to`） |
| `spanio/resolver.h` | `tcp::resolver`、`udp::resolver`（`resolve/async_resolve/cancel`）、`resolve_options`、`resolve_family`、`tcp::async_connect(socket, endpoints, h)` |
| `spanio/tcp_server.h` | `tcp_server`、`tcp_server_options`、`tcp_connection`、`tcp_connection_ptr` |
| `spanio/udp_server.h` | `udp_server`、`udp_server_options`、`udp_server_stats`、`kMaxUdpPayload` |
| `spanio/framing.h` | `framer`、`frame_result`、`line_framer`、`length_prefix_framer`、`encode_length_prefixed`、`kLengthHeaderBytes` |

`spanio::detail` 命名空间里的内容是内部实现，不在冻结范围内，不要直接使用。

---

## 附录 B：示例程序

示例都在 `examples/`，每个是一个独立的 `.cpp`，CMake 默认一起编译，也可以按文件头的命令单独编译。

| 程序 | 用法 | 演示内容 |
|---|---|---|
| `echo_server` | `echo_server [端口] [线程数] [--quiet]` | `tcp_server` 的回显服务；打印每个连接的建立（客户端 IP:端口、在线数）、收到的数据和断开原因，`--quiet` 关闭输出 |
| `chat_server` | `chat_server [端口] [线程数]` | 多用户聊天：行分帧、`user_data` 存昵称、广播 |
| `udp_echo_server` | `udp_echo_server [端口]` | `udp_server` |
| `timer_demo` | `timer_demo` | `steady_timer` 在回调里重新等待，每半秒打印一次，三次后退出 |
| `tcp_client` | `tcp_client <主机> <端口> <消息>` | 异步解析、按地址列表连接、`async_write` / `async_read`、整体超时 |
| `udp_client` | `udp_client <主机> <端口> <消息>` | 同步解析、`async_send_to` / `async_receive_from`、等待超时 |

试一试：

```sh
echo_server 8080 &          # 打印 [connect] / [recv] / [close]
tcp_client localhost 8080 hello
udp_echo_server 9000 &
udp_client localhost 9000 hello
```
