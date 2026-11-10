# SpanIO 跨平台网络框架设计方案

> 版本：v0.19（设计草案，按 M1 实现修订；库版本 0.3.0）
> 定位：Linux / Windows 双平台通用网络框架，融合 Proactor 与 Reactor 两种 IO 模型，统一接口、轻量易用。
>
> **v0.19 修订记录（2026-11-10）**：使用指南，M3 完成。
> - 新增 docs/guide.md：面向使用者的中文指南，19 段代码与 	ests/test_guide.cpp 逐字相同，由 CTest 测试 guide_snippets_match（	ests/check_guide_snippets.cmake）保证。
> - 新增示例 examples/tcp_client.cpp、examples/udp_client.cpp 及其测试 	ests/test_client_examples.cpp。
> - 十 / 十二：同步工程结构；M3 标为完成。
>
> **v0.18 修订记录（2026-11-10）**：冻结 API，库版本 0.3.0。
> - 新增 6.11：冻结范围、修改规则、同步接口的错误报告方式；新增 `include/spanio/spanio.h` 与 `include/spanio/version.h`。
> - 枚举统一为 `enum class`（`tcp::shutdown_type::send`、`error::misc_errors::eof`、`error::netdb_errors::host_not_found`）；`tcp_connection::context()` 改名 `user_data()`；`io_context` 内部接口私有化；新增 `native_handle_type`；内部常量移入 `detail`。
> - 6.2：新增 `tcp::socket::async_read`（读满缓冲区）。
> - 十 / 十二：同步工程结构和 M3 进度。
>
> **v0.17 修订记录（2026-11-09）**：统一错误码。
> - 新增 6.10：错误码一览；所有系统错误经 `detail::normalize_error()` 统一，Windows 的 `WSAESHUTDOWN / WSAENOTSOCK / ERROR_NETNAME_DELETED / ERROR_CONNECTION_REFUSED` 转为对应的 `std::errc`。
> - 6.9：修复 `tcp::async_connect` 在取消或关闭 socket 后仍重新打开 socket 尝试下一个地址的问题。
> - 十 / 十二：同步工程结构和 M3 进度。
>
> **v0.16 修订记录（2026-11-08）**：实现 strand，M3 的功能全部完成。
> - 6.4：补全 `strand`、`make_strand`、`bind_executor` 的接口与语义；新增 6.4.1 测试说明。
> - 十 / 十二：同步工程结构和 M3 进度（剩 API 冻结与文档收尾）。
>
> **v0.15 修订记录（2026-11-08）**：实现域名解析。
> - 新增 6.9：`tcp::resolver / udp::resolver`、`resolve_options` 和 `tcp::async_connect`；新增错误类别 `spanio.netdb`。
> - 十 / 十二：同步工程结构和 M3 进度。
>
> **v0.14 修订记录（2026-11-07）**：服务端超时与优雅关闭。
> - 6.5：`tcp_server_options` 增加 `m_idle_timeout`（默认关闭）和 `m_close_timeout`（默认 30 秒），新增 `stop_gracefully(grace)`；新增错误码 `error::misc_errors::idle_timeout / close_timeout`。
> - 6.5：逗留关闭：数据发完后先等对端关闭（最多 2 秒）再关闭 socket。修复了优雅关闭期间对端仍在发数据时，本端关闭触发 RST、对端丢失数据的问题（Linux 上稳定复现）。
> - 6.7：`udp_server` 新增 `stop_gracefully(grace)`。
> - 十 / 十二：同步工程结构和 M3 进度。
>
> **v0.13 修订记录（2026-11-07）**：完成 Linux 验证，M1 与 M2 在两个平台上完成。
> - 新增 11.3：Linux 上的编译、测试、Sanitizer 与单文件编译结果。
> - 修复：CMake 的 IDE 目标引用了不在仓库中的 `CLAUDE.md`，导致全新克隆时配置失败。
> - 6.6 / 11.2 / 十二：去掉“待 Linux 验证”的标注。
>
> **v0.12 修订记录（2026-11-07）**：实现定时器 `steady_timer`，开始 M3。
> - 新增 6.8：`steady_timer` 的接口、约定和实现。
> - 5.3：“工作”增加挂起的定时器等待。
> - 11.1：示例改为可编译的写法；十 / 十二：同步工程结构和 M3 进度。
>
> **v0.11 修订记录（2026-11-06）**：增加高层服务端封装 `udp_server`，M2 在 Windows 上完成。
> - 新增 6.7：`udp_server`、`udp_server_options`、`udp_server_stats` 的接口、约定和限制；示例 `examples/udp_echo_server.cpp`。
> - 十 / 十二：同步工程结构；M2 标注为“Windows 已完成，Linux 待验证”。
>
> **v0.10 修订记录（2026-11-06）**：实现 `udp::socket`。
> - 新增 6.6：`udp::socket` 的接口、约定和平台差异。
> - 4.4：`io_proactor` 增加数据报操作 `kRecvFrom / kSendTo`（`prepare_receive_from / prepare_send_to`）。
> - `endpoint` 移到 `ip.h`，新增错误码 `error::misc_errors::datagram_truncated`。
> - 十 / 十二：同步工程结构和 M2 进度。
>
> **v0.9 修订记录（2026-11-06）**：`examples/echo_server.cpp` 改为基于 `tcp_server` 实现。
> - 7.2：说明该示例用低层接口，一般服务建议用 `tcp_server`。
> - 行为变化：`stop()` 现在也会断开已连接的客户端；客户端只发不读时，积压的回显超过 16 MiB 后连接被断开（原实现读完一段写完才读下一段，靠 TCP 流控限速）。
>
> **v0.8 修订记录（2026-11-06）**：实现 M2 示例 `examples/chat_server.cpp`。
> - 6.5：增加基于 `tcp_server` 的 `chat_server` 示例说明。
> - 十 / 十二：同步工程结构，M2 进度更新。
>
> **v0.7 修订记录（2026-11-05）**：增加高层服务端封装 `tcp_server`。
> - 新增 6.5：`tcp_server`、`tcp_connection`、`tcp_server_options` 与 `framing.h` 的接口、约定和限制。
> - 新增错误码 `error::misc_errors::message_too_large / send_queue_full / framing_error / handler_failed`。
> - 十二：M2 的 `chat_server` 改为基于 `tcp_server` 实现；`udp_server` 在 `udp::socket` 之后实现。
> - 十：同步工程结构。
>
> **v0.6 修订记录（2026-11-05）**：实现 M1 验收示例 `examples/echo_server.cpp`。
> - 11.2：Windows 单文件编译命令补上 `/EHsc`（实测不加会报 C4530），记录验证情况；Linux 尚未验证。
> - 十二：M1 标注为“Windows 已完成，Linux 待验证”。
> - 十：同步工程结构（示例与测试）。
>
> **v0.5 修订记录（2026-11-04）**：实现 TCP 接口。
> - 6.2：按实现重写 `tcp::socket / tcp::acceptor` 接口与约定；`async_read` 改名为 `async_read_some`，新增组合操作 `async_write`；新增 `ip.h`、`buffer.h`、`error.h`。
> - 7.2：回显示例改为可编译的写法，修正 v0.4 草图中“发起读操作后移走 socket”的缺陷。
> - 4.4：原生 IOCP 后端直接识别 `STATUS_CANCELLED`，修复“注销后立即关闭 socket 时取消被报告为 `WSAENOTSOCK`”的问题。
> - 十：同步工程结构。
>
> **v0.4 修订记录（2026-11-04）**：实现执行引擎 `io_context`。
> - 5.3：按实现重写 `io_context` 接口与语义（工作计数、`restart`、多线程 `run()` 的单轮询者模型、异常与析构），`io_context` 不再自建线程，去掉 `concurrency_hint`。
> - 十：同步工程结构（`io_context.h`、`detail/scheduler_op.h`）。
>
> **v0.3 修订记录（2026-11-04）**：增加原生 IOCP Proactor 后端。
> - 新增 4.4：异步操作接口 `io_proactor`，有两个实现：原生 `iocp_proactor`，以及建立在任意 `io_reactor` 之上的 `reactor_proactor` 适配器。
> - 5.2：Windows **默认改回原生 IOCP Proactor**；定义 `SPANIO_WINDOWS_USE_AFD_REACTOR` 时改用 IOCP + AFD_POLL 就绪模拟。
> - 8.4 / 十三：适配器在 socket 没有挂起操作时把它**从 reactor 注销**，而不是用 `mod()` 清空关注。
>
> **v0.2 修订记录（2026-11-03）**：根据 M1 平台适配层的实现同步本文档。
> - 4.2：`io_reactor` 接口加入错误码和 `interrupt()`，并明确线程与触发约定。
> - 4.2 / 5.2：Windows 后端改为“IOCP + AFD_POLL 获取就绪通知”，与 Linux 走同一条“Reactor 模拟 Proactor”路径。
> - 8.4：epoll 由边沿触发（ET）改为**水平触发（LT）**。
> - 十 / 十三：同步工程结构，补充新的风险项。

---

## 一、项目概述

### 1.1 命名含义

- **Span**：跨越、横跨，呼应“跨平台”的核心定位；同时隐含“跨度”“区间”之意，契合网络数据在连接两端的传递。
- **IO**：直接点明框架的技术领域，即输入输出 / 网络 IO。
- 组合为 **SpanIO**，音节短、无歧义、便于作为头文件命名空间与开源项目标识（`namespace spanio`）。

### 1.2 一句话定位

> SpanIO 是一套以头文件为主的 C++17 跨平台网络框架：在 Linux 与 Windows 之上抽象出**统一的异步 IO 接口**，底层自动选择平台最优 IO 模型（epoll / io_uring / IOCP），对上层**融合 Proactor 与 Reactor 的编程语义**，让开发者用一套代码、一种思维模型写完双平台的网络服务。

### 1.3 设计哲学

| 原则   | 含义                                          |
| ---- | ------------------------------------------- |
| 接口统一 | 上层代码对平台**零感知**，切换编译目标不修改一行业务逻辑              |
| 模型最优 | 不强行用一种模型套所有平台，而是**在每个平台用最合适的原生机制**，再由框架抹平差异 |
| 轻量易用 | 摒弃重型模板继承体系，几行代码即可启动一个 TCP 服务                |
| 零依赖  | 仅依赖 C++17 标准库与各平台系统 API，不绑定 Boost 等三方生态     |

---

## 二、设计目标与非目标

### 2.1 设计目标

1. **跨平台一致性**：同一份源码在 Linux（glibc / musl）与 Windows（MSVC / MinGW）上编译运行，行为一致。
2. **模型融合**：对上层暴露统一的“完成回调”语义（Proactor 风格），同时保留对底层“就绪事件”的可控能力（Reactor 风格），让高手可下沉、新手可上手。
3. **轻量**：核心以 header-only 形式提供，可裁剪、可内联，最小二进制增量控制在设计目标数百 KB 级。
4. **易用**：常见场景（TCP Echo、HTTP 服务端、定时器）无需理解执行器、上下文等抽象概念即可完成。
5. **高性能**：面向高并发长连接场景，目标是单机支撑十万级连接，并降低内存分配与锁竞争开销。

### 2.2 非目标（明确不做）

- 不做完整的 HTTP/2、WebSocket、QUIC 等应用层协议栈（预留扩展点，由用户或插件实现）。
- 不追求“一套模型吃遍所有平台”的教条统一，优先保证各平台性能。
- 不替代操作系统原生能力，仅在其上做抽象与整合。
- 初期不以 C++20 协程为强依赖，协程作为**可选增强**而非必需。

---

## 三、总体架构

采用**四层结构**，自下而上逐层收敛差异：

```
+---------------------------------------------------+
|  应用层  Application / Business Logic              |
+---------------------------------------------------+
|  统一接口层  Unified API Layer                     |
|  tcp::acceptor / tcp::socket / timer / resolver   |
|  回调范式 | Future 范式 | 协程范式（可选）           |
+---------------------------------------------------+
|  执行引擎层  Execution Engine                      |
|  io_context / executor / strand / 任务队列         |
+---------------------------------------------------+
|  平台适配层  Platform Adapter                      |
|  +---------------+  +---------------+  +--------+ |
|  | epoll_reactor |  | io_uring_poll |  | iocp   | |
|  |  (Linux)      |  |  (Linux 可选) |  |(Win)   | |
|  +---------------+  +---------------+  +--------+ |
|         Proactor 语义模拟 / 原生 Proactor          |
+---------------------------------------------------+
|  系统调用层  OS Native API                         |
|  epoll / io_uring / IOCP / Winsock / POSIX socket |
+---------------------------------------------------+
```

设计要点：

- **平台适配层**是差异的唯一收敛点，向上只暴露抽象事件源接口。
- **执行引擎层**提供事件循环、任务调度、串行化（strand）能力，是跨平台语义统一的关键。
- **统一接口层**是用户直接接触的部分，力求 API 数量最少、语义最直白。

---

## 四、跨平台底层抽象层设计

### 4.1 Socket 生命周期统一

屏蔽 POSIX 与 Winsock 的差异：

| 操作    | Linux               | Windows                   | SpanIO 统一封装                    |
| ----- | ------------------- | ------------------------- | ------------------------------ |
| 关闭套接字 | `close(fd)`         | `closesocket(s)`          | `detail::close_socket(handle)` |
| 错误码获取 | `errno`             | `WSAGetLastError()`       | `detail::last_error()`         |
| 库初始化  | 无需                  | `WSAStartup / WSACleanup` | 进程内单例自动初始化（RAII）               |
| 套接字类型 | `int`               | `SOCKET`                  | `native_handle_t` 平台别名统一       |
| 非阻塞设置 | `fcntl(O_NONBLOCK)` | `ioctlsocket(FIONBIO)`    | `detail::set_nonblocking()`    |

- Winsock 的 `WSAStartup` 通过一个 `static` 单例的构造函数完成，避免用户手动管理生命周期，也避免多线程重复初始化的竞争。
- 错误码在适配层统一转换为 `std::error_code`，上层用一套错误分类判断，无需区分 `errno` 与 Winsock 错误。

### 4.2 IO 多路复用抽象

定义统一的**就绪通知**事件源接口（实现见 `include/spanio/detail/io_reactor.h`）：

```cpp
namespace spanio { namespace detail {

// 就绪事件位
enum event_bits : event_mask
{
    kEventNone   = 0,       // 作为关注集合时表示“只报告错误与挂断”
    kEventRead   = 1u << 0, // 可读：有数据、监听 socket 有新连接、对端关闭（recv 返回 0）
    kEventWrite  = 1u << 1, // 可写：发送缓冲有空间，或非阻塞 connect 已结束
    kEventError  = 1u << 2, // 有待处理的错误（如 connect 失败），总是报告，不可作为关注项
    kEventHangup = 1u << 3  // 连接被中止或双向关闭，总是报告，不可作为关注项
};

// 平台无关的事件源抽象
class io_reactor
{
public:
    virtual ~io_reactor() = default;
    // 注册 / 修改 / 注销关心的就绪事件，失败返回错误码
    virtual std::error_code add(descriptor_t fd, event_mask interest) = 0;
    virtual std::error_code mod(descriptor_t fd, event_mask interest) = 0;
    virtual std::error_code del(descriptor_t fd) = 0;
    // 等待事件：timeout 毫秒（kInfiniteTimeout 表示无限等待），结果写入 out
    virtual std::size_t poll(event_table& out, int timeout_ms, std::error_code& ec) = 0;
    // 从任意线程唤醒阻塞中的 poll()，供 io_context::post / stop 使用
    virtual void interrupt() noexcept = 0;
    // 后端名称，用于日志与测试
    virtual const char* name() const noexcept = 0;
};

// 按编译平台创建默认后端（platform_reactor.h）
io_reactor_ptr make_platform_reactor(std::error_code& ec);

}}
```

所有后端共同遵守的约定：

| 约定    | 内容                                                                                     |
| ----- | -------------------------------------------------------------------------------------- |
| 触发方式  | **水平触发（LT）**：只要 socket 仍就绪且在关注集合内，每次 `poll()` 都会再次报告（取舍原因见 8.4）                       |
| 错误与挂断 | `kEventError` / `kEventHangup` 总是报告，无需关注（与 epoll 一致）                                  |
| 错误码   | `add` 重复注册返回 `errc::file_exists`；`mod/del` 未注册返回 `errc::no_such_file_or_directory`；非法关注位返回 `errc::invalid_argument` |
| 线程安全  | `add/mod/del/interrupt` 可在任意线程调用，包括另一线程正阻塞在 `poll()` 时，修改对进行中的 `poll()` 立即生效；同一时刻只允许一个线程调用 `poll()` |
| 生命周期  | 必须先 `del()` 再关闭 socket                                                                  |
| 提前返回  | `poll()` 可能在超时前返回 0 个事件（被 `interrupt()` 唤醒，或 Linux 上被信号打断），调用方循环即可                       |

平台实现：

- **`epoll_reactor`（Linux 默认，已实现）**：基于 `epoll_create1 / epoll_ctl / epoll_wait`，水平触发；`interrupt()` 通过一个注册在 epoll 上的 `eventfd` 实现。

- **`iocp_reactor`（Windows，已实现）**：IOCP 本身是“完成通知”模型，不提供“句柄已就绪”事件。为满足上面的就绪接口，采用与 wepoll / libuv / mio 相同的做法：
  - 打开 `\Device\Afd` 设备句柄并绑定到一个完成端口；
  - 为每个关注的 socket（先通过 `SIO_BASE_HANDLE` 取得底层基础 socket）投递一个 `IOCTL_AFD_POLL` 请求，socket 就绪时该请求完成，完成包经 `GetQueuedCompletionStatusEx` 取回并转换为就绪事件；
  - AFD 请求是“一次性”的，水平触发通过“上报过事件的 socket 在下一次 `poll()` 开始时重新投递请求”来模拟；
  - `interrupt()` 通过 `PostQueuedCompletionStatus` 投递唤醒包实现。
  - `IOCTL_AFD_POLL` 属于**未公开的 Windows 内部接口**，风险见第十三节。

- **`io_uring_reactor`（Linux 可选增强，计划 M5）**：基于 `io_uring` 的提交队列（SQ）与完成队列（CQ）双环结构，实现真正的异步提交—完成语义，减少系统调用次数。

- **`select_reactor`（Windows & Linux，计划中）**：基于两个平台都提供的 POSIX 标准接口 `select`。

### 4.4 异步操作抽象 io_proactor（v0.3 新增）

执行引擎面对的是“提交操作、取回完成结果”的接口，而不是就绪接口（实现见 `include/spanio/detail/io_proactor.h`）：

```cpp
namespace spanio { namespace detail {

// 一个异步操作：由调用方持有，用 prepare_accept / prepare_connect /
// prepare_recv / prepare_send / prepare_receive_from / prepare_send_to / prepare_post 填写；
// 结果写回 m_ec、m_bytes、m_accepted，数据报接收的发送者写回 m_address
struct io_operation;

class io_proactor
{
public:
    virtual ~io_proactor() = default;
    virtual std::error_code register_handle(native_handle_t handle) = 0;   // 使用前注册 socket
    virtual std::error_code unregister_handle(native_handle_t handle) = 0; // 关闭前注销，挂起操作以取消返回
    virtual void start(io_operation* op) = 0;                              // 发起 accept / connect / 流式或数据报收发
    virtual void cancel(native_handle_t handle) = 0;                       // 取消该 socket 上的全部挂起操作
    virtual void post(io_operation* op) = 0;                               // 投递一个无 IO 的任务
    virtual std::size_t poll(completion_list& out, int timeout_ms, std::error_code& ec) = 0;
    virtual void interrupt() noexcept = 0;
    virtual const char* name() const noexcept = 0;
};

io_proactor_ptr make_platform_proactor(std::error_code& ec); // 按平台与配置选择后端

}}
```

所有后端共同遵守的约定：

| 约定      | 内容                                                                                               |
| ------- | ------------------------------------------------------------------------------------------------ |
| 恰好一次    | 每个发起或投递的操作都会被 `poll()` **恰好返回一次**，包括失败与取消；`start()` 从不同步完成操作（避免回调重入）                                  |
| 内存归属    | `io_operation` 由调用方持有，在被 `poll()` 返回前必须保持有效且不被改动                                                   |
| 并发限制    | 每个 socket 同时最多一个读侧操作（accept / recv）和一个写侧操作（connect / send）                                          |
| 读写语义    | recv 返回 0 字节表示对端关闭；send 可能只发送部分数据，调用方继续发送剩余部分                                                    |
| 错误码     | 取消统一报告 `errc::operation_canceled`；原生后端通过 `WSAGetOverlappedResult` 转换为 Winsock 错误码，与适配器路径一致（如 `WSAECONNRESET`）。原生后端直接识别 `STATUS_CANCELLED` 判定取消：常见时序是注销（`CancelIoEx`）后立即 `closesocket`，处理完成包时句柄可能已关闭，此时 `WSAGetOverlappedResult` 会返回 `WSAENOTSOCK`（v0.5 修复） |
| 线程安全    | `register/unregister/start/cancel/post/interrupt` 可在任意线程调用；同一时刻只允许一个线程调用 `poll()`                        |
| 生命周期    | 关闭 socket 前确保其上没有挂起操作并 `unregister_handle()`；销毁 proactor 前所有操作必须已被取回                                   |

两个实现：

- **`iocp_proactor`（Windows 默认）**：原生 Proactor，直接使用 `AcceptEx / ConnectEx / WSARecv / WSASend` 与完成端口，**只用公开 API**，没有就绪通知往返。
  - `AcceptEx` 需要预先创建的接收 socket，完成后设置 `SO_UPDATE_ACCEPT_CONTEXT`。
  - `ConnectEx` 要求 socket 已绑定，未绑定时自动绑定到通配地址，完成后设置 `SO_UPDATE_CONNECT_CONTEXT`。
  - 发起时就失败的请求不会产生完成包，后端手动投递一个，以兑现“恰好返回一次”。

- **`reactor_proactor`（Linux 默认，Windows 可选）**：在任意 `io_reactor` 之上“用 Reactor 模拟 Proactor”（见 5.2）。
  - 发起操作时先**试探性地立即执行一次**非阻塞 IO，只有会阻塞时才把操作挂到 socket 上并向 reactor 申请就绪通知。
  - socket 有挂起操作时才加入 reactor，没有时立即注销（原因见 8.4）。

### 4.3 线程与同步原语

- 统一使用 C++17 标准库的 `std::thread`、`std::mutex`、`std::condition_variable`、`std::atomic`，由标准库负责适配底层（Linux futex / Windows 内核对象）。
- 对高频路径（如任务队列、引用计数）使用 `std::atomic` 自旋优化，避免进入内核态。
- 提供 `spanio::thread_pool` 作为执行引擎的后台工作线程载体，内部实现基于条件变量与任务队列，可选无锁队列。

---

## 五、Proactor 与 Reactor 融合模型

### 5.1 两种模型的本质差异

| 维度   | Reactor（就绪通知）               | Proactor（完成通知）                |
| ---- | --------------------------- | ----------------------------- |
| 通知时机 | 内核告知“IO 就绪”                 | 内核告知“IO 已完成”                  |
| 数据处理 | 用户回调内**自行**调用 read/write    | 回调触发时数据**已就绪**在用户缓冲区          |
| 系统调用 | 通知 + 读写，至少 2 次              | 提交 1 次，完成后再通知                 |
| 典型实现 | epoll、kqueue、libevent、muduo | Windows IOCP、Boost.Asio（对外语义） |

**结论**：Proactor 对开发者更友好（回调时数据已就绪），但 Linux 缺乏稳定高效的原生 Proactor；Reactor 在 Linux 上更普适，但回调内仍需手动读写、逻辑与事件循环耦合。SpanIO 的方案是**各取所长、上层统一为 Proactor 语义**。

### 5.2 平台适配策略

| 平台        | 底层机制     | 对外语义     | 实现方式                                                                             |
| --------- | -------- | -------- | -------------------------------------------------------------------------------- |
| Windows（默认） | IOCP     | Proactor | **原生 Proactor**（`iocp_proactor`）：直接投递 `AcceptEx / ConnectEx / WSARecv / WSASend`，完成包经 IOCP 取回 |
| Windows（可选） | IOCP + AFD_POLL | Proactor | **以 Reactor 模拟 Proactor**（`reactor_proactor` + `iocp_reactor`）：AFD_POLL 请求经 IOCP 报告就绪 → 框架内部发起 `recv/send` → 封装为完成事件。定义 `SPANIO_WINDOWS_USE_AFD_REACTOR`（CMake 同名选项）启用 |
| Linux（默认） | epoll    | Proactor | **以 Reactor 模拟 Proactor**（`reactor_proactor` + `epoll_reactor`）：epoll 等待就绪 → 框架内部发起 `read/write` → 封装为完成事件 |
| Linux（可选） | io_uring | Proactor | **原生异步**：直接向 SQ 提交读写请求，CQ 收割完成结果，无模拟层开销                                          |

> v0.3 变更：v0.2 曾让 Windows 也只走就绪模拟路径。v0.3 增加了 `io_proactor` 抽象层（4.4），执行引擎只面对这一个接口，因此原生 IOCP 可以作为 Windows 默认后端，而不必维护第二套引擎代码。默认后端只用公开 API，也少一次就绪通知往返；AFD 就绪模拟保留为可选项，用于需要和 Linux 行为最一致的场景。

**用就绪通知模拟 Proactor 的关键流程**（`reactor_proactor`，下面以 epoll 为例）：

```
用户调用 async_read(socket, buffer, handler)
        │
        ▼
框架在 reactor 上关注 kEventRead（epoll: EPOLLIN；Windows: AFD_POLL_RECEIVE）
        │
        ▼
reactor.poll() 报告该 socket 可读（就绪通知）
        │
        ▼
框架内部线程（或 IO 线程）调用 ::read 把数据读入用户 buffer
        │
        ▼
数据读完 → 构造完成事件(completion) → 投递到 io_context 任务队列
        │
        ▼
执行用户的 handler（此时 buffer 中数据已就绪，符合 Proactor 语义）
```

这一流程与 Boost.Asio 在 Linux 上的处理路径一致：**调度线程负责等待与触发，实际 IO 由框架内部完成**，从而对上呈现“完成通知”。

### 5.3 统一事件分发器

上层只与一个分发器 `io_context` 交互，不感知底层模型（实现见 `include/spanio/io_context.h`，语义参照 Asio）：

```cpp
namespace spanio {

class io_context
{
public:
    class executor_type;                       // 轻量句柄：post / dispatch / running_in_this_thread / context()

    io_context();                              // 使用 make_platform_proactor()，失败抛 std::system_error
    explicit io_context(detail::io_proactor_ptr proactor); // 指定后端（测试覆盖全部后端时使用）

    std::size_t run();                         // 运行到没有工作或 stop()；返回执行的 handler 数
    std::size_t run_one();                     // 阻塞直到执行一个 handler
    std::size_t poll();                        // 只执行当前已就绪的 handler，不阻塞
    std::size_t poll_one();
    void stop();                               // 任意线程可调用，唤醒所有 run() 中的线程
    bool stopped() const;
    void restart();                            // 停止或工作耗尽后，再次运行前调用

    template <typename Fn> void post(Fn&& fn);     // 只入队，从不在调用处执行
    template <typename Fn> void dispatch(Fn&& fn); // 当前线程在本 context 的 run() 内则直接执行，否则 post
    executor_type get_executor() noexcept;

private:
    detail::io_proactor_ptr m_proactor;        // 平台后端（见 4.4）
    detail::op_queue        m_queue;           // 就绪的完成 handler / 用户任务
    // ...互斥量、条件变量、工作计数等
};

class executor_work_guard;                     // make_work_guard(ctx)：工作耗尽时 run() 也不返回

} // namespace spanio
```

v0.4 确定的语义：

| 约定        | 内容                                                                                                             |
| --------- | -------------------------------------------------------------------------------------------------------------- |
| 线程来源      | 由用户线程调用 `run()`，`io_context` 自身**不创建线程**（取消 v0.3 草图中的 `concurrency_hint` 和 `threads_`）                            |
| 工作计数      | “工作” = 队列中的 handler + 挂起的定时器等待 + 已发起未完成的 IO 操作 + 存活的 `executor_work_guard`。降为 0 或调用 `stop()` 时 `run()` 返回，此后 `stopped()` 为真，需 `restart()` 才能再次运行 |
| 多线程 run() | proactor 同一时刻只允许一个线程 `poll()`：同一时刻只有一个线程充当轮询者，把完成的 IO 操作转换成 handler 入队；其余线程在条件变量上等待并从队列取 handler。任意线程 `post` 时唤醒一个等待线程，没有等待线程时 `interrupt()` 轮询者 |
| 异常        | handler 抛出的异常从 `run()` 传出，context 状态保持一致，再次调用 `run()` 继续执行剩余 handler                                          |
| 析构        | 未执行的 handler 只销毁不调用；先从 proactor 取回所有已发起的 IO 操作（调用方应已取消或注销其 socket）再销毁。最多等待 5 秒，超时则记录日志并泄漏，而不是释放内核仍可能写入的内存 |

任务节点（`detail/scheduler_op.h`）：

- `scheduler_op` 是侵入式链表节点，用函数指针分发（无虚函数），run 或 destroy 恰好一次，两者都释放节点。
- IO 操作节点 `io_op<Handler>` 同时继承 `io_operation` 与 `scheduler_op`：同一个堆对象先交给 proactor，完成后由 `static_cast` 转回并放入任务队列，**不需要额外分配**，`io_operation` 的定义也不用改。
- 被丢弃的 accept 操作会关闭已接受的 socket，避免泄漏。
- 供 IO 对象（`tcp::socket` 等）使用的内部接口：`register_handle / unregister_handle / cancel_handle / start_io(io_op_base*) / schedule_timer / cancel_timer`。v0.18 起它们是 `io_context` 的私有成员，只能经 `detail::context_access` 调用，不属于公开 API。

用户回调由 `io_context` 在完成事件到达时调用；此时数据已在用户缓冲区（Proactor 语义）。

---

## 六、统一接口设计

### 6.1 对象模型

```
io_context ──┬── tcp::acceptor  （监听 + accept）
             ├── tcp::socket    （连接读写）
             ├── udp::socket    （数据报）
             ├── timer         （定时器）
             └── resolver      （域名解析）
```

所有异步对象均遵循：**构造时绑定 io_context → 发起异步操作并指定 handler → handler 中拿到结果与错误码**。

### 6.2 核心 API（v0.5 已实现）

实现见 `include/spanio/tcp.h`，辅助类型在 `ip.h`（`ip::address`、`ip::make_address`、`ipv4::any/loopback`、`ipv6::any/loopback`）、`buffer.h`（`mutable_buffer / const_buffer` 与 `spanio::buffer(...)`）、`error.h`（`spanio::error::misc_errors::eof`）。

```cpp
namespace spanio { namespace tcp {

class endpoint;                                   // IP 地址 + 端口，可与 sockaddr 互转

class socket                                      // 只可移动
{
public:
    explicit socket(io_context& ctx);
    std::error_code open(int family);             // async_connect 在未打开时会自动 open
    using native_handle_type = ...;               // Windows 为 SOCKET，Linux 为 int
    std::error_code assign(native_handle_type handle);
    bool is_open() const;
    native_handle_type native_handle() const;
    endpoint local_endpoint(std::error_code& ec) const;
    endpoint remote_endpoint(std::error_code& ec) const;
    std::error_code shutdown(shutdown_type what); // shutdown_type::receive / send / both（enum class）
    void cancel();                                // 挂起操作以 operation_canceled 完成
    void close();                                 // 注销 → 取消 → 关闭；handler 仍恰好执行一次

    template <typename H> void async_connect(const endpoint& peer, H&& h);       // h(std::error_code)
    template <typename H> void async_read_some(mutable_buffer buf, H&& h);       // h(ec, n)，可能只读到一部分
    template <typename H> void async_read(mutable_buffer buf, H&& h);            // h(ec, n)，读满整个缓冲区才回调；对端先关闭报 eof 和已读字节数（v0.18）
    template <typename H> void async_write_some(const_buffer buf, H&& h);        // h(ec, n)，可能只写出一部分
    template <typename H> void async_write(const_buffer buf, H&& h);             // h(ec, n)，写完全部或出错才回调
};

class acceptor                                    // 只可移动
{
public:
    explicit acceptor(io_context& ctx);
    acceptor(io_context& ctx, const endpoint& local); // open + bind + listen，失败抛 std::system_error
    std::error_code open(int family);
    std::error_code bind(const endpoint& local);
    std::error_code listen(int backlog = SOMAXCONN);
    endpoint local_endpoint(std::error_code& ec) const;
    void cancel();
    void close();
    template <typename H> void async_accept(H&& h);                              // h(ec, tcp::socket)
};

}}
```

约定：

| 约定        | 内容                                                                                              |
| --------- | ----------------------------------------------------------------------------------------------- |
| 命名        | 沿用 Asio：`async_read_some / async_write_some` 允许部分传输；`async_read / async_write` 是读满 / 写完整个缓冲区的组合操作（`async_read` 在 v0.18 加入） |
| 回调时机      | handler 一律在 `io_context::run()` 中执行，**从不在发起调用里内联执行**，包括立即失败（如 socket 未打开报 `bad_file_descriptor`）和空缓冲区（成功、0 字节） |
| 恰好一次      | 每个发起的操作恰好回调一次；`close()`、`cancel()` 与析构使挂起操作以 `errc::operation_canceled` 完成                          |
| 并发限制      | 每个 socket 同时最多一个读侧操作（read_some / read / accept）和一个写侧操作（connect / write_some / write），属于前置条件，不做运行时检查                  |
| EOF       | 非空缓冲区读到 0 字节报告 `spanio::error::misc_errors::eof`                                                         |
| 地址复用      | `acceptor::open()`：Linux 设置 `SO_REUSEADDR`（重启服务时可绑定处于 TIME_WAIT 的端口）；Windows 设置 `SO_EXCLUSIVEADDRUSE`。实测 Windows 的 `SO_REUSEADDR` 允许第二个 socket 绑定**正在监听**的端口，有端口劫持风险 |
| 缓冲区生命周期   | `buffer` 不拥有内存，必须保持有效直到 handler 执行                                                       |

handler 写法（遵循 `code_rule.md`：lambda 先声明为变量再传入）：

```cpp
auto on_read = [&](std::error_code ec, std::size_t n)
{
    if (ec)
    {
        return; // spanio::error::misc_errors::eof 表示对端已正常关闭
    }
    // 此时 buf 前 n 字节已是有效数据
};
sock.async_read_some(spanio::buffer(buf), on_read);
```

### 6.3 多范式支持

| 范式                   | 说明                                    | 适用人群      |
| -------------------- | ------------------------------------- | --------- |
| 回调（Callback）         | 最基础，零额外依赖                             | 所有用户      |
| Future / `awaitable` | 通过 `use_future` 等定制标记返回 `std::future` | 偏好同步风格的用户 |
| 协程（可选）               | 检测到 `co_await` 时启用 C++20 协程适配         | 现代 C++ 用户 |

三者共享同一套底层完成事件，**只是完成令牌（CompletionToken）不同**，无需重复实现逻辑。

### 6.4 Strand：用户免加锁的串行化（v0.16 实现）

多线程 `run()` 下，为避免用户对共享状态加锁，提供 `strand`（`include/spanio/strand.h`）：

```cpp
spanio::strand<spanio::io_context::executor_type> s = spanio::make_strand(ctx);
auto on_read = [&](std::error_code ec, std::size_t n)
{
    // 绑定到同一 strand 的 handler 绝不会并发执行，无需加锁
};
sock.async_read_some(spanio::buffer(buf), spanio::bind_executor(s, on_read));
```

strand 保证“逻辑单线程、物理多核利用”，是并发安全的核心工具。

**接口**

| 接口 | 说明 |
|---|---|
| `make_strand(ctx)` / `make_strand(executor)` | 新建一个独立的串行域 |
| `strand::post(h)` | 排入 strand 队列，永不在调用处执行 |
| `strand::dispatch(h)` | 当前线程正在执行本 strand 的回调，或在本 context 的 `run()` 中且 strand 空闲时，就地执行；否则同 `post` |
| `strand::running_in_this_thread()` | 当前线程是否正在执行本 strand 的回调（嵌套时外层 strand 也为 true） |
| `get_inner_executor()` / `context()` / `==` / `!=` | 底层执行器、所属 context；副本之间相等 |
| `bind_executor(ex, h)` | 返回 `executor_binder`：操作完成时保存参数（decay 拷贝或移动），再 `ex.dispatch()` 调用 `h`；参数以右值传入，可接收 `tcp::socket` 这类只能移动的值 |

**语义**

- `strand` 是可拷贝的轻量对象，内部用 `shared_ptr` 指向同一个 `detail::strand_impl`（互斥锁 + 回调队列 + “被占用”标志）。副本共享串行域，每次 `make_strand` 是新的域。
- 同一 strand 的回调同一时刻至多一个在执行，按入队顺序执行；回调之间经互斥锁同步，前一个回调写入的数据对后一个可见。不同 strand 之间并行。
- 执行方式：strand 空闲时，入队的同时向 `io_context` 投递**一个**执行任务（`strand_invoker`）。它执行队首回调后，若队列非空就投递下一个执行任务，否则释放 strand。每个回调都是 `io_context` 的一个独立 handler：计入工作量，`run()` 的返回值也按回调计数；不会长时间独占一个线程。
- 没有排队回调的 strand 不算工作量，不会让 `run()` 不返回。
- 回调抛异常：先把 strand 交给下一个执行任务（或释放），再让异常从 `run()` 抛出；剩余回调保持顺序，下次 `run()` 继续执行。经 `detail::log_error` 记录。
- `io_context` 析构时，未执行的 strand 执行任务被销毁，并带走队列里的回调（销毁、不调用），不等到最后一个 strand 副本析构。strand 副本可以比 context 活得久，但之后不能再使用。
- `io_context`、I/O 对象和两个 server 均无需改动：`bind_executor` 返回的只是一个普通的完成回调。

---

### 6.4.1 Strand 测试（v0.16）

`tests/test_strand.cpp` 共 16 个用例；涉及 TCP 的用例在平台的每个 proactor 后端上各跑一遍。

| 分组 | 用例 |
|---|---|
| 基本语义 | `post_not_inline`、`fifo_order`、`no_concurrency`（4 个线程投递、4 个线程 `run()`、2 万个回调）、`strands_run_in_parallel`、`copies_share_domain` |
| `dispatch` 与 `running_in_this_thread` | `dispatch_inline_inside_strand`、`dispatch_outside_run_queues`、`dispatch_from_context_handler`、`post_inside_strand_runs_after`、`nested_strands` |
| 工作量、异常与销毁 | `work_counting`、`handler_exception`、`destroy_with_pending` |
| `bind_executor` 与 I/O | `bind_timer`（200 个计时器 + 1 个取消）、`bind_tcp_io`（1 MB 回显，读写回调与额外投递的回调都在同一 strand）、`bind_move_only_and_cancel` |

---

### 6.5 高层服务端封装 tcp_server（v0.7）

`tcp::socket / acceptor` 要求使用者自己处理接受循环、连接生命周期、写操作串行和分帧。`tcp_server`（`tcp_server.h`）把这些收拢起来，服务只需提供回调；低层接口继续保留。

```cpp
spanio::tcp_server server(8080);
auto on_message = [](const spanio::tcp_connection_ptr& conn, std::string_view data)
{
    conn->send(data);
};
server.on_message(on_message);
server.run(4);                   // 阻塞到 stop()，共 4 个线程
```

| 组成                     | 内容                                                                                                 |
| ---------------------- | -------------------------------------------------------------------------------------------------- |
| `tcp_server`           | 构造时完成绑定和监听，失败抛异常。`on_connect / on_message / on_close / set_framer` 须在启动前设置。`start()` 不阻塞，`run(n)` 阻塞且只能用于自带 context，`stop()` 可在任意线程调用 |
| `tcp_connection`       | 由 `shared_ptr` 持有。`send()` 可在任意线程调用，内部排队；`close()` 发完再关，`abort()` 立即关；`user_data()` 用 `std::any` 挂业务数据（v0.18 前叫 `context()`，为避免和返回 `io_context&` 的 `context()` 混淆而改名） |
| `tcp_server_options`   | 最大连接数、最大消息长度、发送队列字节上限、读缓冲大小、空闲超时、`close()` 超时                                                                        |
| 分帧（`framing.h`）        | `line_framer()`、`length_prefix_framer()`、`encode_length_prefixed()`，也可传入任意函数对象                         |

约定：

| 约定      | 内容                                                                                                                          |
| ------- | --------------------------------------------------------------------------------------------------------------------------- |
| 回调顺序    | `on_connect` 返回后才开始读；`on_close` 恰好一次且在最后                                                                                     |
| 回调并发    | 同一连接的回调不并发；不同连接之间可能并发，共享数据要自己加锁，`run(1)` 除外                                                                                  |
| 实现方式    | 每条连接有一条持续的读链，**所有回调都从读链发出**，写操作独立进行且不调用用户代码。读链结束且没有写操作在途时才触发 `on_close`                                                     |
| 关闭原因    | `on_close` 的 `ec`：本端 `close()/abort()/stop()` 为空，对端关闭为 `error::misc_errors::eof`，超限或回调异常为 `error::misc_errors::message_too_large / send_queue_full / framing_error / handler_failed`，其余为系统错误码 |
| 半关闭     | 对端只关闭发送方向（EOF）时，已排队的数据仍会发完再关闭，`ec` 为 `error::misc_errors::eof`                                                                           |
| 回调异常    | `on_connect / on_message` 抛异常时记录日志并关闭该连接，服务继续运行；`on_close` 的异常只记录                                                             |
| 超过最大连接数 | 接受后立即关闭，不调用 `on_connect`（暂停接受会让客户端连上却没有响应，更难排查）                                                                               |
| 写操作     | 不用 `async_write`，而是在连接锁内逐次 `async_write_some`：部分发送后继续时不能落到别的线程刚关闭的 socket 上。在途的队首数据等写操作返回后才释放                                  |
| 内存安全    | 注册表持有 `weak_ptr`，连接持有共享状态，避免循环引用；外部 context 上 server 销毁后才运行的 handler 也是安全的                                                     |

超时与关闭（v0.14）：

| 功能                          | 约定                                                                                                  |
| --------------------------- | --------------------------------------------------------------------------------------------------- |
| 空闲超时 `m_idle_timeout`       | 默认关闭。连续这么久没有活动就关闭连接，`ec` 为 `error::misc_errors::idle_timeout`。“活动”指收到数据**或发送有进展**，所以只下载的客户端不会被误断                     |
| `close()` 超时 `m_close_timeout` | 默认 30 秒。队列在时限内没发完就强制断开，`ec` 为 `error::misc_errors::close_timeout`；设为 0 表示一直等                                    |
| `stop_gracefully(grace)`    | 立即停止接受，对每条连接 `close()`：不再投递 `on_message`，已排队数据发完再关；宽限期到了仍未关的强制断开（`close_timeout`），全部关闭后 `run()` 返回 |
| `stop()`                    | 不变，立即关闭；宽限期内调用会立刻结束等待                                                                            |
| 对端半关闭                       | 收到 EOF 时如果还有待发数据，同样受 `m_close_timeout` 限制                                                         |
| 逗留关闭                        | 数据发完后只关闭发送方向，继续读取并丢弃对端发来的数据，等对端关闭后再关闭 socket，最多等 `close` 时限与 2 秒（`detail::kLingerTimeout`）中较小者；到时直接关闭，`ec` 仍为空。原因：接收缓冲区里有未读数据时关闭 socket，系统会发 RST 而不是 FIN，对端可能丢掉刚发出的数据。因此 `close()` 后 `on_close` 要等对端关闭（正常客户端收到 FIN 会立即关闭），最多 2 秒 |

实现：每条连接有一个空闲定时器和一个关闭定时器，访问都在连接锁内。空闲检测靠“最后活动时间”惰性重排：定时器到期时，如果期间有过活动就顺延到“最后活动时间 + 超时”，所以收发路径上只多一次原子写，不必每次重设定时器。连接关闭时取消两个定时器，因为挂起的等待会让连接一直存活，也会算作 `io_context` 的工作。

限制：

- 一个 `tcp_server` 只能启动一次；`tcp_connection_ptr` 不能比 `tcp_server` 活得更久。

示例 `examples/chat_server.cpp`（`chat_server [port] [threads]`，默认端口 9000）演示了在 `tcp_server` 上写一个完整服务：

- **协议：** 文本行，第一行是昵称（非空、最长 32 字节、不与在线用户重复），之后每行以 `昵称: 内容` 广播给其他人；进出时广播 `* 昵称 joined / left`，加入成功回 `* welcome 昵称`，失败回 `error: 原因` 并断开。
- **结构：** 示例只包含聊天逻辑。连接、分帧、发送队列和线程全部由 `tcp_server` 负责，一条超过 1024 字节的行或落后超过 1 MiB 的客户端由 `tcp_server_options` 的上限断开。
- **全局顺序：** `chat_room` 持锁广播，所有用户看到的消息顺序一致。`send()` 只是入队，持锁成本很低。
- **避免循环引用：** 成员表持有 `weak_ptr`，因为回调持有房间，而连接持有服务端状态，强引用会形成环。

### 6.6 UDP 接口 udp::socket（v0.10）

实现见 `include/spanio/udp.h`。`endpoint` 移到 `ip.h` 成为 `ip::endpoint`，`tcp::endpoint` 和 `udp::endpoint` 都是它的别名。

```cpp
namespace spanio { namespace udp {

using endpoint = ip::endpoint;

class socket                                          // 只可移动
{
public:
    explicit socket(io_context& ctx);
    socket(io_context& ctx, const endpoint& local);   // open + bind，失败抛 std::system_error
    std::error_code open(int family);
    std::error_code bind(const endpoint& local);
    endpoint local_endpoint(std::error_code& ec) const;
    void cancel();
    void close();

    template <typename H> void async_receive_from(mutable_buffer buf, H&& h);
    // h(std::error_code, std::size_t bytes, const endpoint& sender)
    template <typename H> void async_send_to(const_buffer buf, const endpoint& peer, H&& h);
    // h(std::error_code, std::size_t bytes)
};

}}
```

| 约定     | 内容                                                                                       |
| ------ | ---------------------------------------------------------------------------------------- |
| 消息边界   | 一次操作对应一个数据报，不会合并，也不会部分发送                                                                 |
| 空数据报   | 0 字节是合法数据报，成功收发，UDP 没有 EOF；空接收缓冲区立即成功且不消耗数据报                                             |
| 截断     | 缓冲区太小时报 `error::misc_errors::datagram_truncated`，`bytes` 为实际存入的字节数，其余丢失                                  |
| 超大数据报  | 超过协议上限时发送报 `errc::message_size`，socket 继续可用                                               |
| 自动打开   | `async_send_to` 在未打开时按目标地址族打开；`async_receive_from` 未打开时报 `bad_file_descriptor`                |
| 发送者地址  | 通过回调参数传递，调用方不用自己保证地址对象的生命周期                                                              |
| 回调与取消  | 与 TCP 相同：从不内联，恰好一次，`close()/cancel()`/析构以 `errc::operation_canceled` 完成                      |

平台差异由 `detail::configure_datagram_socket()` 和后端屏蔽（均已在 Windows 上实测）：

- **端口独占：** Windows 设 `SO_EXCLUSIVEADDRUSE`。实测用 `SO_REUSEADDR` 时第二个 socket 能绑定同一端口。Linux 不设 `SO_REUSEADDR`，端口默认独占（v0.13 已在 Linux 上验证）。
- **`WSAECONNRESET`：** Windows 默认会在向已关闭端口发送后让下一次接收失败，实测属实，因此用 `SIO_UDP_CONNRESET` 关闭。
- **截断：** 原生 IOCP 上 `WSARecvFrom` 返回 `WSA_IO_PENDING`（即使数据报已到达），唯一的完成包状态为 `STATUS_BUFFER_OVERFLOW`，再转成 `WSAEMSGSIZE`，不会重复完成。适配器在 Windows 上 `recvfrom` 直接报 `WSAEMSGSIZE`；Linux 的 `recvfrom` 不报告截断，所以用 `recvmsg` 检查 `MSG_TRUNC`（v0.13 已在 Linux 上验证）。

本期不做 `connect` 模式的 UDP、组播和广播选项，需要时通过 `native_handle()` 设置。

### 6.7 高层服务端封装 udp_server（v0.11）

`udp_server`（`udp_server.h`）对应 6.5 的 `tcp_server`，由它负责接收循环、发送队列和线程，服务只需提供一个回调。低层的 `udp::socket` 继续保留。

```cpp
spanio::udp_server server(9000);
auto on_datagram = [&server](const spanio::udp::endpoint& from, std::string_view data)
{
    server.send_to(from, data);
};
server.on_datagram(on_datagram);
server.run();                    // 阻塞到 stop()
```

| 约定      | 内容                                                                                           |
| ------- | -------------------------------------------------------------------------------------------- |
| 回调并发    | 只有一条接收链，`on_datagram` **始终串行**，处理完一个才收下一个。回调要短，否则内核队列满后会丢包；`run(n)` 的 `n > 1` 对此没有帮助，耗时工作应分发给其他线程 |
| 数据有效期   | `from` 和 `data` 只在回调期间有效；空数据报正常投递                                                             |
| 发送      | `send_to()` 可在任意线程调用，入队后逐个发送，同一线程的顺序不变                                                        |
| 队列满     | 丢弃新数据报，`send_to()` 返回 `false`，计入 `m_queue_dropped`。UDP 没有连接可断，也不影响已入队的数据                      |
| 发送失败    | 记录日志并计入 `m_send_failed`，不提供错误回调                                                               |
| 超长数据报   | 超过 `m_max_datagram_size` 时丢弃并计入 `m_truncated`，不调用回调，避免业务处理残缺数据                                  |
| 错误处理    | 回调异常和接收错误都只记录日志，继续接收，只有 `stop()` 才结束                                                         |
| 生命周期    | 与 `tcp_server` 相同：构造时绑定，回调在启动前设置，只能启动一次，`run()` 只用于自带 context                                 |

关闭：`stop()` 立即停止并丢弃未发送的数据。`stop_gracefully(grace)`（v0.14）立即停止接收、拒绝新的数据报，已排队的数据报继续发送，发完或宽限期到了再关闭；宽限期内调用 `stop()` 会立刻结束等待。

`stats()` 返回 `udp_server_stats`，包含 `m_received / m_sent / m_queue_dropped / m_send_failed / m_truncated`。

示例 `examples/udp_echo_server.cpp`（`udp_echo_server [port]`，默认端口 9000）基于 `udp_server` 实现。

### 6.8 定时器 steady_timer（v0.12）

实现见 `include/spanio/timer.h`。定时器基于单调时钟，等待通过 `io_context` 完成。

```cpp
class steady_timer                                    // 不可复制，也不可移动
{
public:
    using clock = std::chrono::steady_clock;
    using duration = clock::duration;
    using time_point = clock::time_point;

    explicit steady_timer(io_context& ctx);           // 默认已到期
    steady_timer(io_context& ctx, duration delay);
    steady_timer(io_context& ctx, time_point when);

    std::size_t expires_after(duration delay);        // 取消挂起的等待，返回取消数量
    std::size_t expires_at(time_point when);
    time_point expiry() const;

    template <typename H> void async_wait(H&& h);     // h(std::error_code)
    std::size_t cancel();                             // 返回取消数量
};
```

| 约定      | 内容                                                                                  |
| ------- | ----------------------------------------------------------------------------------- |
| 结果      | 到期时 `ec` 为空，取消时为 `errc::operation_canceled`，每个等待恰好回调一次                                |
| 回调时机    | 从不内联，已过期的定时器也通过 `run()` 回调                                                          |
| 精度      | **绝不提前**；等待时长向上取整到毫秒，系统繁忙时可能晚触发                                                     |
| 工作计数    | 挂起的等待算工作，`run()` 等到它结束                                                              |
| 多个等待    | 一个定时器可以有多个等待，到期或取消时全部回调                                                             |
| 取消来不及   | 回调已入队后 `cancel()` 返回 0，回调收到成功，依赖取消结果的代码要看 `ec` 或返回值                                   |
| 销毁      | 销毁定时器以取消方式回调；销毁 `io_context` 时未执行的等待只销毁不调用。`io_context` 必须比定时器活得更久                    |
| `stop()` | 挂起的定时器保留，`restart()` 后继续                                                            |
| 线程安全    | 不同定时器可在不同线程使用，同一个定时器对象不能被多个线程同时操作                                                   |

实现（`detail/timer_queue.h`）：

- `io_context` 持有按到期时间排序的 `timer_queue`，每个定时器只占一项，所以取消是 O(log n)，同一到期时间按发起顺序。
- 轮询者调用 `io_proactor::poll()` 时用最早到期时间算出超时，**proactor 不用改**，所有后端自动支持。新加入的定时器如果成为最早到期的，就唤醒轮询者重新计算。
- 每取一个 handler 前都检查到期，持续有 handler 排队时定时器也不会被饿死，代价是多读一次时钟。
- 到期的等待转成普通 handler 进入任务队列，之后的调度与其他 handler 相同。

### 6.9 域名解析 resolver（v0.15）

实现见 `include/spanio/resolver.h`。

```cpp
namespace spanio {

enum class resolve_family { any, v4, v6 };

struct resolve_options
{
    resolve_family m_family = resolve_family::any;
    bool m_numeric_host = false;      // 主机必须是 IP 字面量，不发 DNS 查询
    bool m_numeric_service = false;   // 服务必须是端口号
};

}

namespace spanio { namespace tcp {     // udp::resolver 相同，只是 SOCK_DGRAM

class resolver
{
public:
    explicit resolver(io_context& ctx);
    std::vector<endpoint> resolve(std::string_view host, std::string_view service,
                                  std::error_code& ec, const resolve_options& options = {});
    template <typename H>
    void async_resolve(std::string_view host, std::string_view service, H&& h,
                       const resolve_options& options = {});   // h(ec, std::vector<endpoint>)
    void cancel();
};

template <typename H>
void async_connect(socket& sock, std::vector<endpoint> endpoints, H&& h);  // h(ec, const endpoint&)

}}
```

| 约定            | 内容                                                                                                       |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| 实现            | **两个平台相同**：每个 `resolver` 有一个工作线程（首次使用时创建），在线程里阻塞调用 `getaddrinfo`，结果投递回 `io_context`。没有用 Windows 的 `GetAddrInfoExW`，因为 Linux 没有对应的异步接口，统一实现才能保证行为一致 |
| 并发            | 同一个 `resolver` 的请求串行处理；需要并行就创建多个 `resolver`                                                                  |
| 回调            | 在 `run()` 中执行，从不内联，恰好一次；挂起的请求算作工作                                                                         |
| 结果            | 保持系统返回的顺序（RFC 6724 排序），去掉重复项；`service` 可以是端口号或服务名                                                        |
| 错误            | 新类别 `spanio.netdb`：`host_not_found / host_not_found_try_again / no_data / no_recovery / service_not_found`，两个平台的 `getaddrinfo` 返回码都映射到这里 |
| 取消            | 立即以 `operation_canceled` 回调。正在进行的系统查询无法中断，结果到达后直接丢弃；因此析构时要等当前查询结束，DNS 慢时可能要几秒                                 |
| `async_connect` | 按顺序逐个尝试，成功即停；每次尝试前重新打开 socket（地址族可能不同）；全部失败报最后一个错误，空列表报 `host_not_found`；取消或关闭 socket 时立即以 `operation_canceled` 结束，不再尝试后面的地址（v0.17 修复）。典型场景：`localhost` 先得到 `::1`，而服务器只监听 IPv4，会自动改连 `127.0.0.1` |

测试通过一个内部构造函数替换解析函数，模拟慢查询，所以取消和析构的用例不依赖真实网络的时序。

### 6.10 错误码一览（v0.17）

所有错误都以 `std::error_code` 交给回调，分三类：

| 类别 | 内容 | 判断方式 |
|---|---|---|
| 系统错误（`std::system_category` / `generic`） | 操作系统报告的错误 | `ec == std::errc::xxx` |
| `spanio.misc` | SpanIO 自己判定的情况 | `ec == spanio::error::misc_errors::eof` 等 |
| `spanio.netdb` | 域名解析失败 | `ec == spanio::error::netdb_errors::host_not_found` 等 |

**平台统一**：所有系统错误都经过 `detail::normalize_error()`（`socket_ops.h`）。Linux 的 `errno` 本来就能与 `std::errc` 比较，原样保留；Windows 上 C++ 标准库认不出的几个码被转换：

| Windows 原始码 | 转换为 | 出现场景 |
|---|---|---|
| `WSA_OPERATION_ABORTED` | `operation_canceled` | 取消（Linux 的 `ECANCELED` 同样转换） |
| `WSAESHUTDOWN` | `broken_pipe` | 本端 `shutdown(发送)` 后再写，与 Linux 的 `EPIPE` 一致 |
| `WSAENOTSOCK` | `bad_file_descriptor` | 句柄已关闭 |
| `ERROR_NETNAME_DELETED` | `connection_reset` | IOCP 原始的对端重置码 |
| `ERROR_CONNECTION_REFUSED` | `connection_refused` | IOCP 原始的拒绝连接码 |

**各情况的错误码**（两个平台、所有后端相同，均有测试）：

| 情况 | 错误码 |
|---|---|
| 取消、关闭 socket、定时器 `cancel()`、resolver `cancel()` | `errc::operation_canceled` |
| 在未打开的 socket 上发起操作 | `errc::bad_file_descriptor` |
| 对已打开的 socket 再 `open()` / `assign()` | `errc::invalid_argument` |
| TCP 对端正常关闭（读到 0 字节） | `error::misc_errors::eof`；UDP 没有 EOF，空数据报是成功、0 字节 |
| 连接被拒绝 | `errc::connection_refused` |
| 对端重置连接：读，或重置后第一次失败的写 | `errc::connection_reset` |
| 本端 `shutdown(发送)` 后再写 | `errc::broken_pipe` |
| 数据报比接收缓冲区大 | `error::misc_errors::datagram_truncated` |
| 地址已被占用 | `errc::address_in_use` |
| `tcp_server` 的限制与回调异常 | `error::misc_errors::message_too_large / send_queue_full / framing_error / handler_failed / idle_timeout / close_timeout`；本端 `close()` / `abort()` / `stop()` 时 `on_close` 收到空错误码 |
| 域名解析 | `spanio.netdb` 的五个值；另有 `EAI_FAMILY / EAI_SOCKTYPE` → `address_family_not_supported`，`EAI_MEMORY` → `not_enough_memory`，`EAI_SYSTEM` → 当时的 `errno` |
| `tcp::async_connect` 收到空地址列表 | `error::netdb_errors::host_not_found` |

**约定**（M3 评审确定）：
- 重复打开用 `errc::invalid_argument`、空地址列表用 `host_not_found`，不另设 Asio 式的 `already_open` / `not_found`。
- 收到任何错误后应关闭该连接。已报告过一次重置的连接上继续写，各平台报的码不同：Linux 报 `broken_pipe`（`EPIPE`），Windows 仍报 `connection_reset`。原因是 Linux 内核只报告一次重置错误；要区分“本端关闭了发送”与“重置已报告过”需要额外记录状态，收益不大，因此只在此说明。
- 发送非空缓冲区却只发出 0 字节（正常不会发生，属防御代码）时，`async_write` 和 `tcp_server` 报 `errc::io_error`，避免死循环。
- 只出现在日志里、不会交给回调的错误码（`strand` 的 `state_not_recoverable`、`io_context` 析构超时的 `timed_out`）不在统一范围内。

测试：`tests/test_errors.cpp`，5 个用例（`connect_sequence_cancel`、`write_after_shutdown`、`write_after_peer_reset` 在每个后端上各跑一遍；`error_categories`；`normalize_platform_codes`）。
### 6.11 API 冻结（v0.18，版本 0.3.0）

M3 结束时冻结公开 API，版本号定为 **0.3.0**（`include/spanio/version.h`：`SPANIO_VERSION_MAJOR / MINOR / PATCH`、`SPANIO_VERSION`、`SPANIO_MAKE_VERSION(a, b, c)`、`SPANIO_VERSION_STRING`）。

| 项目 | 约定 |
|---|---|
| 冻结范围 | `spanio::` 命名空间中（不含 `spanio::detail::`）的类型、函数、回调签名，以及 6.10 的错误码约定 |
| 不冻结 | `spanio::detail::` 的一切；参数为 `detail` 类型的测试用构造函数：`io_context(io_proactor_ptr)`、`basic_resolver(ctx, resolve_function)` |
| 冻结后的修改 | 只做兼容增加（新函数、新选项字段、新重载）；破坏现有代码的修改（改名、删除、改签名）要升主版本号 |
| 二进制兼容 | 只有头文件，不承诺 ABI |
| 入口头文件 | `#include "spanio/spanio.h"`（位于 `include/spanio/spanio.h`）包含全部公开头文件；也可以只包含单个头文件 |

**同步接口的错误报告方式**：

- 没有返回值的操作返回 `std::error_code`：`open`、`bind`、`listen`、`shutdown`、`assign`。
- 有返回值的操作用 `ec` 输出参数：`local_endpoint(ec)`、`remote_endpoint(ec)`、`resolve(..., ec)`、`make_address(text, ec)`。
- 构造函数失败时抛出 `std::system_error`：`acceptor(ctx, ep)`、`tcp_server(port)`、`udp_server(port)`、`io_context()`。
- 不带 `ec` 的便捷版本失败时抛异常：`make_address(string)`。

**冻结前做的调整**（破坏性修改，以后不再发生）：

| 调整 | 原写法 | 新写法 |
|---|---|---|
| 枚举一律用 `enum class` | `tcp::shutdown_send`、`error::eof`、`error::host_not_found` | `tcp::shutdown_type::send`、`error::misc_errors::eof`、`error::netdb_errors::host_not_found` |
| 连接的用户数据改名 | `tcp_connection::context()` | `tcp_connection::user_data()`（`std::any&`） |
| `io_context` 内部接口私有化 | `ctx.register_handle(...)` 等 | 只能经 `detail::context_access` 调用 |
| 原生句柄类型 | `detail::native_handle_t` | `tcp::socket / tcp::acceptor / udp::socket::native_handle_type`；`ip::endpoint::native_size_type` |
| 内部常量移入 `detail` | `tcp::kSocketOptionOn`、`ip::kV4AddressBytes` 等、`spanio::kLingerTimeout`、`kDefault*` | 默认值见各选项结构体的初始值；保留公开的只有 `tcp::kDefaultBacklog`、`kLengthHeaderBytes`、`kMaxUdpPayload` |
| 新增读满 | 无 | `tcp::socket::async_read(buf, h)` |

**冻结后可兼容增加的功能**：常用 socket 选项（`TCP_NODELAY`、keepalive、收发缓冲区）、已连接模式的 UDP 与 `udp::socket::assign`、同步等待接口、future / 协程风格的完成令牌（M5）、`tcp_server` 的广播与连接遍历。

测试：`tests/test_api.cpp`，7 个用例（`umbrella_header`、`internals_not_public`、`native_handle_type`、`async_read_exact`、`async_read_eof`、`async_read_edge_cases`、`user_data`；`async_read` 的三个用例在每个后端上各跑一遍）。多数检查在编译期完成：内部接口重新公开、枚举改回普通枚举、移入 `detail` 的常量名重新出现在公开命名空间，都会使这个文件编译失败。

## 七、轻量易用设计

### 7.1 Header-only 为主

- 核心功能（socket、acceptor、timer、io_context）全部以头文件实现，用户 `#include` 即可集成，无需预编译库。
- 仅少数需要隐藏平台细节或减少编译时间的模块提供可选的静态库构建方式（`SPANIO_SEPARATE_COMPILATION` 宏）。

### 7.2 极简 API

参考 `cpp-httplib` 的易用性思路：**不强制继承任何基类，不强制实现大量虚函数**。

用低层接口 `tcp::socket / acceptor` 写一个最小 TCP 回显服务（按 v0.5 实现的接口）。需要自己管理连接时可以这样写；一般服务建议用 6.5 的 `tcp_server`，`examples/echo_server.cpp` 就是基于它实现的，业务代码只有一个回调：

```cpp
#include <spanio/tcp.h>

#include <array>
#include <memory>

// 一条连接：读到多少就原样写回多少，直到对端关闭
class echo_session : public std::enable_shared_from_this<echo_session>
{
public:
    explicit echo_session(spanio::tcp::socket sock)
        : m_socket(std::move(sock))
    {
    }

    void read()
    {
        auto self = shared_from_this();
        auto on_read = [self](std::error_code ec, std::size_t n)
        {
            if (ec)
            {
                return; // spanio::error::misc_errors::eof：对端关闭；self 释放后 socket 自动关闭
            }
            auto on_written = [self](std::error_code write_ec, std::size_t)
            {
                if (!write_ec)
                {
                    self->read();
                }
            };
            self->m_socket.async_write(spanio::buffer(self->m_buffer.data(), n), on_written);
        };
        m_socket.async_read_some(spanio::buffer(m_buffer.data(), m_buffer.size()), on_read);
    }

private:
    spanio::tcp::socket m_socket;       // 连接
    std::array<char, 1024> m_buffer{};  // 接收缓冲区，也是回写的数据源
};

int main()
{
    spanio::io_context ctx;
    spanio::tcp::acceptor acc(ctx, spanio::tcp::endpoint(spanio::ipv4::any(), 8080));

    std::function<void()> accept_next;
    auto on_accept = [&](std::error_code ec, spanio::tcp::socket sock)
    {
        if (!ec)
        {
            std::make_shared<echo_session>(std::move(sock))->read();
        }
        accept_next();
    };
    accept_next = [&]()
    {
        acc.async_accept(on_accept);
    };
    accept_next();

    ctx.run();  // 启动事件循环
    return 0;
}
```

> v0.4 草图中的写法有缺陷：在 `sock` 上发起读操作后又把 `sock` 移动进同一个 lambda 的捕获，操作所在的对象已被移走。上面的写法让 socket 由会话对象持有，handler 通过 `shared_ptr` 保证会话存活到操作完成。

### 7.3 编译期裁剪

用 CMake option 与宏隔离非核心模块，未使用的功能完全不参与编译：

| 宏                         | 作用                    |
| ------------------------- | --------------------- |
| `SPANIO_ENABLE_SSL`       | 启用 TLS（对接 OpenSSL）    |
| `SPANIO_ENABLE_IO_URING`  | Linux 下启用 io_uring 后端 |
| `SPANIO_ENABLE_COROUTINE` | 启用 C++20 协程适配         |
| `SPANIO_ENABLE_LOG`       | 启用内置轻量日志              |

默认全关，保证最小内核的编译速度与二进制体积。

---

## 八、性能优化设计

> 以下条目为**设计目标**，具体收益需在实现后由基准测试验证，不作为承诺数值。

### 8.1 三级内存池

| 层级      | 用途            | 特点              |
| ------- | ------------- | --------------- |
| 线程本地池   | 单线程高频分配       | 无锁，命中率最高        |
| 全局池     | 跨线程分配         | 带回退机制，处理线程本地池耗尽 |
| 请求级单调缓冲 | 单次请求生命周期内的小对象 | 整体释放，避免逐个 free  |

目标：让小对象分配尽量避开 `malloc`，降低高并发下的分配开销与内存碎片。

### 8.2 无锁发送队列

- 每个连接维护一个发送队列，多线程并发 `async_write` 时采用 MPSC（多生产者单消费者）无锁队列。
- 保证同一连接的写操作串行提交，避免锁竞争与数据交错。

### 8.3 栈上缓冲优化

- 读写默认使用小尺寸栈上缓冲（如 512 字节），绝大多数小包场景无需堆分配。
- 大缓冲或跨异步操作存活的缓冲由用户或框架显式管理（`shared_ptr` 或缓冲池）。

### 8.4 减少系统调用

- **所有后端统一使用水平触发（LT）**（v0.2 修订，原方案为 epoll 边沿触发 ET）。原因：
  - **跨平台一致**：Windows 后端的 AFD_POLL 请求每次只完成一次，用“重新投递”模拟 LT 很自然；但无法可靠地模拟 ET，因为框架不知道用户何时读空了缓冲区。为了让两个平台对上层的行为完全一致，统一采用 LT。
  - **更安全**：在 LT 下，执行引擎即使漏读一部分数据，下一次 `poll()` 也会再次报告；在 ET 下必须读到 `EAGAIN` 为止，否则连接会永久“沉默”。
  - **代价与对策**：对于持续就绪但暂时不处理的 socket，LT 会重复报告。`reactor_proactor` 适配器只在 socket 有挂起操作时把它加入 reactor，没有时立即 `del()` 注销（v0.3 修订：原方案是用 `mod()` 清空关注）。原因是错误与挂断事件总会上报，仅清空关注无法阻止它们；例如 epoll 对尚未连接的 socket 会持续报告 `EPOLLHUP`，在 LT 下会让 `poll()` 空转。若基准测试显示需要，可在后续版本为 epoll 后端增加可选的 ET 模式。
- 可选 io_uring 后端通过批量提交/收割，进一步降低系统调用次数。
- Windows 后端用 `GetQueuedCompletionStatusEx` 一次批量取回多个完成包（每次最多 `kMaxEventsPerPoll` 个）。

---

## 九、与 Asio 的差异化定位

| 对比项      | SpanIO                                      | Boost.Asio                                |
| -------- | ------------------------------------------- | ----------------------------------------- |
| 上手门槛     | 极简 API，几行启动服务，无强制继承体系                       | 执行器、io_context、完成令牌等概念较多，学习曲线陡            |
| 依赖       | 仅 C++17 标准库，零三方依赖                           | 通常强依赖 Boost 生态，或使用 standalone 版本          |
| 平台 IO 后端 | Windows 原生 IOCP（可选 IOCP + AFD_POLL 就绪模拟）；Linux 支持 epoll 与可选 io_uring | Windows IOCP；Linux epoll（io_uring 支持在演进中） |
| 编译体积/速度  | header-only + 编译期裁剪，核心精简                    | 模板实例化较重，编译时间偏长                            |
| 抽象层次     | 保留可下沉到就绪事件的能力，同时默认给完成语义                     | 统一 Proactor 抽象，Reactor 细节对用户较透明           |
| 目标       | 轻量、易用、跨平台一致                                 | 功能全面、生态成熟、事实标准                            |

**SpanIO 的取舍**：不追求功能上全面对齐 Asio，而是聚焦「双平台一致的异步网络能力 + 极低上手成本」，把复杂度留在框架内部，把简单留给使用者。

---

## 十、工程结构

```
spanio/
├── CMakeLists.txt
├── include/
│   └── spanio/
│       ├── spanio.h                # 总入口：包含全部公开头文件（已实现，用法 #include "spanio/spanio.h"）
│       ├── io_context.h            # 执行引擎：事件循环、任务队列、executor、work guard（已实现）
│       ├── executor.h              # 执行器抽象
│       ├── strand.h                # 串行化域 strand、make_strand、bind_executor（已实现）
│       ├── version.h               # 版本号宏 SPANIO_VERSION_*（已实现，0.3.0）
│       ├── error.h                 # SpanIO 自有错误码（error::misc_errors::eof）（已实现）
│       ├── buffer.h                # 缓冲区抽象 mutable_buffer / const_buffer（已实现）
│       ├── ip.h                    # ip::address、ipv4 / ipv6 常用地址（已实现）
│       ├── tcp.h                   # TCP 接口：endpoint、socket、acceptor（已实现）
│       ├── tcp_server.h            # 高层 TCP 服务端：tcp_server、tcp_connection（已实现）
│       ├── framing.h               # 消息分帧：按行、长度前缀（已实现）
│       ├── udp.h                   # UDP 接口：udp::socket（已实现）
│       ├── udp_server.h            # 高层 UDP 服务端：udp_server（已实现）
│       ├── timer.h                 # 定时器 steady_timer（已实现）
│       ├── resolver.h              # 域名解析与 async_connect（已实现）
│       └── detail/
│           ├── platform.h          # 平台宏与类型别名（已实现）
│           ├── socket_ops.h        # socket 生命周期封装（已实现）
│           ├── socket_common.h     # tcp.h 与 udp.h 共用的辅助代码（已实现）
│           ├── run_threads.h       # 在多个线程上运行 io_context（已实现）
│           ├── timer_queue.h       # 按到期时间排序的定时器队列（已实现）
│           ├── io_reactor.h        # 就绪通知抽象接口与事件定义（已实现）
│           ├── platform_reactor.h  # 按平台选择默认后端（已实现）
│           ├── epoll_reactor.h     # Linux epoll 后端，水平触发（已实现）
│           ├── iocp_reactor.h      # Windows IOCP + AFD_POLL 后端（已实现）
│           ├── io_proactor.h       # 异步操作抽象接口与 io_operation（已实现）
│           ├── iocp_proactor.h     # Windows 原生 IOCP Proactor，默认（已实现）
│           ├── reactor_proactor.h  # 在 io_reactor 上模拟 Proactor 的适配器（已实现）
│           ├── platform_proactor.h # 按平台与配置选择默认 proactor（已实现）
│           ├── scheduler_op.h      # io_context 任务节点与侵入式队列（已实现）
│           ├── io_uring_reactor.h  # Linux io_uring 后端
│           └── select_reactor.h    # Linux&Windows 通用后端
│    
├── examples/
│   ├── CMakeLists.txt          # 示例构建（SPANIO_BUILD_EXAMPLES）（已实现）
│   ├── echo_server.cpp         # M1 验收示例：基于 tcp_server 的多线程回显服务（已实现）
│   ├── timer_demo.cpp          # 定时器示例（已实现）
│   ├── chat_server.cpp         # M2 示例：基于 tcp_server 的多用户聊天（已实现）
│   ├── udp_echo_server.cpp     # M2 示例：基于 udp_server 的回显服务（已实现）
│   ├── tcp_client.cpp          # TCP 客户端示例：异步解析、按地址列表连接、async_read、整体超时（已实现）
│   └── udp_client.cpp          # UDP 客户端示例：同步解析、收发数据报、等待超时（已实现）
├── tests/
│   ├── test_io_context.cpp
│   ├── test_tcp.cpp            # TCP 接口测试（已实现）
│   ├── test_echo_server.cpp    # 回显示例测试（已实现）
│   ├── test_tcp_server.cpp     # tcp_server 与分帧测试（已实现）
│   ├── test_chat_server.cpp    # 聊天示例测试（已实现）
│   ├── test_udp.cpp            # udp::socket 测试（已实现）
│   ├── test_udp_server.cpp     # udp_server 与 UDP 回显示例测试（已实现）
│   ├── test_timer.cpp          # steady_timer 测试（已实现）
│   ├── test_server_timeouts.cpp # 服务端超时与优雅关闭测试（已实现）
│   ├── test_resolver.cpp       # 域名解析测试（已实现）
│   ├── test_strand.cpp         # strand 与 bind_executor 测试（已实现）
│   ├── test_errors.cpp         # 统一错误码测试（已实现）
│   ├── test_api.cpp            # API 冻结守护测试与 async_read 测试（已实现）
│   ├── test_client_examples.cpp # 两个客户端示例的测试（已实现）
│   ├── test_guide.cpp          # 使用指南中每段代码的编译与运行（已实现）
│   ├── check_guide_snippets.cmake # 比对指南代码块与 test_guide.cpp（CTest：guide_snippets_match）（已实现）
│   └── test_cross_platform.cpp
├── benchmark/
│   └── bench_throughput.cpp
└── docs/
│     ├── SpanIO.md               # 设计文档（本文）
│     ├── guide.md                # 使用指南（已实现）
│     ├── code_rule.md            # 编码规范
│     └── git_code_rule.md        # 提交规范
└── spanio_demo.sln   
```

构建约束：

- C++17 起（协程模块需 C++20）。
- CMake ≥ 3.15，通过 `if(WIN32)` / `if(UNIX)` 选择后端源文件与链接库。
- Windows 需链接 `ws2_32.lib`；Linux 无需额外链接（io_uring 后端链接 `liburing`，可选）。

---

## 十一、最小可运行示例（骨架）

### 11.1 定时器示例

```cpp
#include <spanio/timer.h>

#include <chrono>
#include <cstdio>

int main()
{
    spanio::io_context ctx;
    spanio::steady_timer timer(ctx, std::chrono::seconds(1));

    auto on_timeout = [](std::error_code ec)
    {
        if (!ec)
        {
            std::puts("1 second elapsed");
        }
    };
    timer.async_wait(on_timeout);

    ctx.run();
    return 0;
}
```

完整示例见 `examples/timer_demo.cpp`：定时器在回调里重新设置自己，连续触发 3 次。

### 11.2 跨平台编译验证

```bash
# Linux
g++ -std=c++17 -Iinclude examples/echo_server.cpp -o echo_server -lpthread

# Windows (MSVC，在 VS 开发者命令行中)
cl /std:c++17 /EHsc /Iinclude examples\echo_server.cpp ws2_32.lib
```

目标：**同一份示例源码，在两个平台以各自原生命令行直接编译通过**。

v0.6 验证情况：

- Windows：上面的 `cl` 命令（另加 `/W4 /WX`）实测编译通过、无警告。`/EHsc` 是必需的：不加时 MSVC 报 C4530，抛出异常时不会正确析构对象。
- Linux：v0.13 已验证。上面的 `g++` 命令直接可用；4 个示例另加 `-Wall -Wextra -Wpedantic -Werror` 也全部编译通过、无警告（见 11.3）。
- CMake 构建时示例作为 `echo_server` 目标编译（`SPANIO_BUILD_EXAMPLES`，顶层项目默认开启），与测试使用相同的严格警告级别。
- 测试 `tests/test_echo_server.cpp` 以 `SPANIO_ECHO_SERVER_NO_MAIN` 包含示例源码，测试的正是发布的这份代码。

### 11.3 Linux 验证（v0.13）

在 `026e250` 上，于 Ubuntu 24.04（内核 6.8，`g++` 13.3，`cmake` 3.28）从 GitHub 全新克隆后验证：

| 项目                                | 结果                                                                     |
| --------------------------------- | ---------------------------------------------------------------------- |
| 编译                                | Debug 与 Release，`-Wall -Wextra -Wpedantic -Werror`，0 错误 0 警告               |
| 测试                                | 11 个程序、150 个用例全部通过（含 1 个仅 Linux 的用例），后端为 `reactor-proactor over epoll` |
| 重复运行                              | Debug 与 Release 各连续 20 次，无失败                                             |
| AddressSanitizer + UBSan + LeakSanitizer | 全部通过，无内存错误、未定义行为或泄漏                                                   |
| ThreadSanitizer                   | 全部通过，无数据竞争                                                             |
| 单文件编译                             | 4 个示例加 `-Werror` 全部通过，文档中的一行命令可直接使用                                      |

几点说明：

- **发现的问题：** 全新克隆时 CMake 配置失败，原因是 IDE 目标 `spanio_docs` 引用了已不在仓库中的 `CLAUDE.md`。v0.13 已修复。
- **之前只能推测的行为：** Linux 上非阻塞 `send` 确实会部分发送。让 `async_write` 只发一次时，用例 8 会因数据不全而卡住，这与 Windows 上“从不部分发送”的实测相反，所以续发路径在 Linux 上是真实会走到的。UDP 截断（`recvmsg` 加 `MSG_TRUNC`）、UDP 端口默认独占、非阻塞 `connect` 也都由对应用例在 Linux 上验证通过。
- **ThreadSanitizer 的环境问题：** 在 `vm.mmap_rnd_bits = 32` 的内核上，GCC 13 的 TSan 启动时会报 `unexpected memory mapping`，这是工具链的已知问题，与代码无关。可以用 `setarch $(uname -m) -R` 对单个进程关闭地址随机化来运行，不必修改系统设置。

---

## 十二、演进路线

| 阶段      | 目标                                              | 关键交付               |
| ------- | ----------------------------------------------- | ------------------ |
| M1 骨架   | 打通 io_context + epoll/iocp 双后端 + tcp::socket 读写 | echo_server 双平台可运行（**已完成**：Windows 与 Linux 上代码与测试全部通过，见 11.3） |
| M2 统一语义 | 完成 Reactor 模拟 Proactor，回调范式贯通；高层封装 tcp_server、udp_server | chat_server 示例（基于 tcp_server）；chat_server、udp_echo_server（**已完成**：Windows 与 Linux 均已验证） |
| M3 易用性  | strand、timer、resolver、错误码统一；tcp_server / udp_server 的优雅关闭和超时 | API 冻结，编写文档（**已完成**：功能齐全，错误码统一，API 冻结于 0.3.0，使用指南 docs/guide.md） |
| M4 性能   | 内存池、无锁发送队列、栈上缓冲                                 | 基准测试报告             |
| M5 增强   | io_uring 后端、TLS、可选协程                            | 生产级特性补齐            |

---

## 十三、风险与取舍

| 风险点               | 说明                        | 应对策略                               |
| ----------------- | ------------------------- | ---------------------------------- |
| 模拟 Proactor 的额外开销 | Linux 默认路径（以及 Windows 可选的 AFD 路径）多一次就绪通知往返      | Linux 提供 io_uring 后端作为高性能选项；Windows 默认已是原生 IOCP Proactor；适配器发起操作时先试探性执行一次 IO，数据已就绪时省去往返 |
| AFD_POLL 为未公开接口   | 可选的 Windows AFD 路径依赖 `IOCTL_AFD_POLL` 等未公开的内核接口，微软未承诺兼容 | v0.3 起 Windows 默认使用只依赖公开 API 的原生 IOCP，AFD 路径仅在显式启用时使用；该接口自 Vista 起保持稳定，并被 libuv、mio、wepoll 长期使用；ntdll 函数在运行期查找，缺失时 `create()` 返回 `function_not_supported` 而不是崩溃 |
| 水平触发的重复通知        | 持续就绪但暂不处理的 socket 每次 `poll()` 都会被报告 | 适配器在 socket 没有挂起操作时把它从 reactor 注销；必要时为 epoll 增加可选 ET 模式 |
| 两个 Windows 后端行为差异 | 原生 IOCP 与 AFD 模拟在边界情况（错误码、部分发送）上可能不同 | 同一套 `test_io_proactor` 用例对两个后端都运行，约定见 4.4 |
| io_uring 兼容性      | 需 Linux 5.1+ 内核与 liburing | 设为编译期可选，默认关闭，运行期探测降级               |
| 协程依赖 C++20        | 提高编译器门槛                   | 协程作为可选模块，不影响核心使用                   |
| 头文件编译膨胀           | header-only 可能拖慢编译        | 提供分离编译模式；模板尽量外提，减少实例化              |
| 与 Asio 生态重叠       | 用户可能倾向成熟方案                | 主打轻量 + 易用 + 零依赖的差异化，避免正面功能对标       |

---

## 附录：术语速查

| 术语         | 含义                              |
| ---------- | ------------------------------- |
| Reactor    | 就绪通知模型：内核通知“可读写”，应用自行完成 IO      |
| Proactor   | 完成通知模型：内核完成 IO 后通知应用，回调时数据已就绪   |
| epoll      | Linux 高效 IO 多路复用，就绪驱动           |
| io_uring   | Linux 5.1+ 真正的异步 IO，SQ/CQ 双环结构  |
| IOCP       | Windows I/O 完成端口，原生 Proactor 实现 |
| AFD_POLL   | Windows 套接字驱动 AFD 的未公开轮询请求，完成时表示 socket 就绪，经 IOCP 取回 |
| LT / ET    | 水平触发 / 边沿触发：LT 只要仍就绪就反复通知，ET 只在状态变化时通知一次 |
| strand     | 串行化执行域，保证绑定其上的回调不并发执行           |
| io_context | 事件循环与任务调度中心，框架运转核心              |

---

*本文档为设计草案，API 与结构将在实现阶段持续迭代。*
