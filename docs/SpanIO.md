# SpanIO 跨平台网络框架设计方案

> 版本：v0.7（设计草案，按 M1 实现修订）
> 定位：Linux / Windows 双平台通用网络框架，融合 Proactor 与 Reactor 两种 IO 模型，统一接口、轻量易用。
>
> **v0.7 修订记录（2026-11-05）**：增加高层服务端封装 `tcp_server`。
> - 新增 6.5：`tcp_server`、`tcp_connection`、`tcp_server_options` 与 `framing.h` 的接口、约定和限制。
> - 新增错误码 `error::message_too_large / send_queue_full / framing_error / handler_failed`。
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
// prepare_recv / prepare_send / prepare_post 填写；结果写回 m_ec、m_bytes、m_accepted
struct io_operation;

class io_proactor
{
public:
    virtual ~io_proactor() = default;
    virtual std::error_code register_handle(native_handle_t handle) = 0;   // 使用前注册 socket
    virtual std::error_code unregister_handle(native_handle_t handle) = 0; // 关闭前注销，挂起操作以取消返回
    virtual void start(io_operation* op) = 0;                              // 发起 accept / connect / recv / send
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
| 工作计数      | “工作” = 队列中的 handler + 已发起未完成的 IO 操作 + 存活的 `executor_work_guard`。降为 0 或调用 `stop()` 时 `run()` 返回，此后 `stopped()` 为真，需 `restart()` 才能再次运行 |
| 多线程 run() | proactor 同一时刻只允许一个线程 `poll()`：同一时刻只有一个线程充当轮询者，把完成的 IO 操作转换成 handler 入队；其余线程在条件变量上等待并从队列取 handler。任意线程 `post` 时唤醒一个等待线程，没有等待线程时 `interrupt()` 轮询者 |
| 异常        | handler 抛出的异常从 `run()` 传出，context 状态保持一致，再次调用 `run()` 继续执行剩余 handler                                          |
| 析构        | 未执行的 handler 只销毁不调用；先从 proactor 取回所有已发起的 IO 操作（调用方应已取消或注销其 socket）再销毁。最多等待 5 秒，超时则记录日志并泄漏，而不是释放内核仍可能写入的内存 |

任务节点（`detail/scheduler_op.h`）：

- `scheduler_op` 是侵入式链表节点，用函数指针分发（无虚函数），run 或 destroy 恰好一次，两者都释放节点。
- IO 操作节点 `io_op<Handler>` 同时继承 `io_operation` 与 `scheduler_op`：同一个堆对象先交给 proactor，完成后由 `static_cast` 转回并放入任务队列，**不需要额外分配**，`io_operation` 的定义也不用改。
- 被丢弃的 accept 操作会关闭已接受的 socket，避免泄漏。
- 供 IO 对象（`tcp::socket` 等）使用的内部接口：`register_handle / unregister_handle / cancel_handle / start_io(io_op_base*)`。

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

实现见 `include/spanio/tcp.h`，辅助类型在 `ip.h`（`ip::address`、`ip::make_address`、`ipv4::any/loopback`、`ipv6::any/loopback`）、`buffer.h`（`mutable_buffer / const_buffer` 与 `spanio::buffer(...)`）、`error.h`（`spanio::error::eof`）。

```cpp
namespace spanio { namespace tcp {

class endpoint;                                   // IP 地址 + 端口，可与 sockaddr 互转

class socket                                      // 只可移动
{
public:
    explicit socket(io_context& ctx);
    std::error_code open(int family);             // async_connect 在未打开时会自动 open
    std::error_code assign(native_handle_t handle);
    bool is_open() const;
    native_handle_t native_handle() const;
    endpoint local_endpoint(std::error_code& ec) const;
    endpoint remote_endpoint(std::error_code& ec) const;
    std::error_code shutdown(shutdown_type what); // shutdown_receive / shutdown_send / shutdown_both
    void cancel();                                // 挂起操作以 operation_canceled 完成
    void close();                                 // 注销 → 取消 → 关闭；handler 仍恰好执行一次

    template <typename H> void async_connect(const endpoint& peer, H&& h);       // h(std::error_code)
    template <typename H> void async_read_some(mutable_buffer buf, H&& h);       // h(ec, n)，可能只读到一部分
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
| 命名        | 沿用 Asio：`async_read_some / async_write_some` 允许部分传输，`async_write` 是写完整个缓冲区的组合操作。v0.4 草图中的 `async_read` 实际是“读一部分”语义，已改名，避免误解为“读满缓冲区” |
| 回调时机      | handler 一律在 `io_context::run()` 中执行，**从不在发起调用里内联执行**，包括立即失败（如 socket 未打开报 `bad_file_descriptor`）和空缓冲区（成功、0 字节） |
| 恰好一次      | 每个发起的操作恰好回调一次；`close()`、`cancel()` 与析构使挂起操作以 `errc::operation_canceled` 完成                          |
| 并发限制      | 每个 socket 同时最多一个读侧操作（read / accept）和一个写侧操作（connect / write），属于前置条件，不做运行时检查                  |
| EOF       | 非空缓冲区读到 0 字节报告 `spanio::error::eof`                                                         |
| 地址复用      | `acceptor::open()`：Linux 设置 `SO_REUSEADDR`（重启服务时可绑定处于 TIME_WAIT 的端口）；Windows 设置 `SO_EXCLUSIVEADDRUSE`。实测 Windows 的 `SO_REUSEADDR` 允许第二个 socket 绑定**正在监听**的端口，有端口劫持风险 |
| 缓冲区生命周期   | `buffer` 不拥有内存，必须保持有效直到 handler 执行                                                       |

handler 写法（遵循 `code_rule.md`：lambda 先声明为变量再传入）：

```cpp
auto on_read = [&](std::error_code ec, std::size_t n)
{
    if (ec)
    {
        return; // spanio::error::eof 表示对端已正常关闭
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

### 6.4 Strand：无锁串行化

多线程 `run()` 下，为避免用户对共享状态加锁，提供 `strand`：

```cpp
spanio::strand<io_context::executor_type> s = spanio::make_strand(ctx);
sock.async_read(buf, spanio::bind_executor(s, [](std::error_code, std::size_t) {
    // 绑定到同一 strand 的 handler 绝不会并发执行，无需加锁
}));
```

strand 保证“逻辑单线程、物理多核利用”，是并发安全的核心工具。

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
| `tcp_connection`       | 由 `shared_ptr` 持有。`send()` 可在任意线程调用，内部排队；`close()` 发完再关，`abort()` 立即关；`context()` 用 `std::any` 挂业务数据 |
| `tcp_server_options`   | 最大连接数、最大消息长度、发送队列字节上限、读缓冲大小                                                                        |
| 分帧（`framing.h`）        | `line_framer()`、`length_prefix_framer()`、`encode_length_prefixed()`，也可传入任意函数对象                         |

约定：

| 约定      | 内容                                                                                                                          |
| ------- | --------------------------------------------------------------------------------------------------------------------------- |
| 回调顺序    | `on_connect` 返回后才开始读；`on_close` 恰好一次且在最后                                                                                     |
| 回调并发    | 同一连接的回调不并发；不同连接之间可能并发，共享数据要自己加锁，`run(1)` 除外                                                                                  |
| 实现方式    | 每条连接有一条持续的读链，**所有回调都从读链发出**，写操作独立进行且不调用用户代码。读链结束且没有写操作在途时才触发 `on_close`                                                     |
| 关闭原因    | `on_close` 的 `ec`：本端 `close()/abort()/stop()` 为空，对端关闭为 `error::eof`，超限或回调异常为 `error::message_too_large / send_queue_full / framing_error / handler_failed`，其余为系统错误码 |
| 半关闭     | 对端只关闭发送方向（EOF）时，已排队的数据仍会发完再关闭，`ec` 为 `error::eof`                                                                           |
| 回调异常    | `on_connect / on_message` 抛异常时记录日志并关闭该连接，服务继续运行；`on_close` 的异常只记录                                                             |
| 超过最大连接数 | 接受后立即关闭，不调用 `on_connect`（暂停接受会让客户端连上却没有响应，更难排查）                                                                               |
| 写操作     | 不用 `async_write`，而是在连接锁内逐次 `async_write_some`：部分发送后继续时不能落到别的线程刚关闭的 socket 上。在途的队首数据等写操作返回后才释放                                  |
| 内存安全    | 注册表持有 `weak_ptr`，连接持有共享状态，避免循环引用；外部 context 上 server 销毁后才运行的 handler 也是安全的                                                     |

限制：

- `stop()` 立即关闭并丢弃未发送的数据。优雅关闭需要超时，要等 M3 的 `timer`；同样的原因，`close()` 遇到不读数据的对端时会一直等待，直到 `abort()` 或 `stop()`。空闲超时也在 M3 之后补充。
- 一个 `tcp_server` 只能启动一次；`tcp_connection_ptr` 不能比 `tcp_server` 活得更久。

## 七、轻量易用设计

### 7.1 Header-only 为主

- 核心功能（socket、acceptor、timer、io_context）全部以头文件实现，用户 `#include` 即可集成，无需预编译库。
- 仅少数需要隐藏平台细节或减少编译时间的模块提供可选的静态库构建方式（`SPANIO_SEPARATE_COMPILATION` 宏）。

### 7.2 极简 API

参考 `cpp-httplib` 的易用性思路：**不强制继承任何基类，不强制实现大量虚函数**。

一个最小 TCP 回显服务示例（按 v0.5 实现的接口，`examples/echo_server.cpp` 将以此为基础）：

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
                return; // spanio::error::eof：对端关闭；self 释放后 socket 自动关闭
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
│       ├── spanio.h                # 总入口
│       ├── io_context.h            # 执行引擎：事件循环、任务队列、executor、work guard（已实现）
│       ├── executor.h              # 执行器抽象
│       ├── strand.h                # 串行化域
│       ├── error.h                 # SpanIO 自有错误码（error::eof）（已实现）
│       ├── buffer.h                # 缓冲区抽象 mutable_buffer / const_buffer（已实现）
│       ├── ip.h                    # ip::address、ipv4 / ipv6 常用地址（已实现）
│       ├── tcp.h                   # TCP 接口：endpoint、socket、acceptor（已实现）
│       ├── tcp_server.h            # 高层 TCP 服务端：tcp_server、tcp_connection（已实现）
│       ├── framing.h               # 消息分帧：按行、长度前缀（已实现）
│       ├── udp.h                   # UDP 接口
│       ├── timer.h                 # 定时器
│       ├── resolver.h              # 域名解析
│       └── detail/
│           ├── platform.h          # 平台宏与类型别名（已实现）
│           ├── socket_ops.h        # socket 生命周期封装（已实现）
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
│   ├── echo_server.cpp         # M1 验收示例：多线程 TCP 回显服务（已实现）
│   ├── timer_demo.cpp
│   └── chat_server.cpp
├── tests/
│   ├── test_io_context.cpp
│   ├── test_tcp.cpp            # TCP 接口测试（已实现）
│   ├── test_echo_server.cpp    # 回显示例测试（已实现）
│   ├── test_tcp_server.cpp     # tcp_server 与分帧测试（已实现）
│   └── test_cross_platform.cpp
├── benchmark/
│   └── bench_throughput.cpp
└── docs/
│     └── design.md
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
#include <spanio/spanio.hpp>
#include <cstdio>

int main() {
    spanio::io_context ctx;
    spanio::steady_timer t(ctx, std::chrono::seconds(1));

    t.async_wait([&](std::error_code ec) {
        if (!ec) std::puts("1 second elapsed");
    });

    ctx.run();
    return 0;
}
```

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
- Linux：尚未验证（开发机没有 Linux 工具链）。
- CMake 构建时示例作为 `echo_server` 目标编译（`SPANIO_BUILD_EXAMPLES`，顶层项目默认开启），与测试使用相同的严格警告级别。
- 测试 `tests/test_echo_server.cpp` 以 `SPANIO_ECHO_SERVER_NO_MAIN` 包含示例源码，测试的正是发布的这份代码。

---

## 十二、演进路线

| 阶段      | 目标                                              | 关键交付               |
| ------- | ----------------------------------------------- | ------------------ |
| M1 骨架   | 打通 io_context + epoll/iocp 双后端 + tcp::socket 读写 | echo_server 双平台可运行（**Windows 已完成**：代码与测试全部通过；**Linux 待验证**：epoll 路径尚未编译运行） |
| M2 统一语义 | 完成 Reactor 模拟 Proactor，回调范式贯通；高层封装 tcp_server、udp_server | chat_server 示例（基于 tcp_server）；tcp_server 已完成，udp_server 待做 |
| M3 易用性  | strand、timer、resolver、错误码统一                     | API 冻结，编写文档        |
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
