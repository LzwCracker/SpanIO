# SpanIO 基准测试报告（0.3.0 基线）

> 本文记录 SpanIO 0.3.0 的性能基线，作为 M4 各项优化的对照。
> 原始数据：[`benchmark/results/0.3.0-windows.csv`](../benchmark/results/0.3.0-windows.csv)、[`benchmark/results/0.3.0-linux.csv`](../benchmark/results/0.3.0-linux.csv)。
> 程序：`benchmark/bench_spanio.cpp`；对比脚本：`benchmark/compare.cmake`。

## 1. 怎么跑

```sh
cmake -S . -B build -DSPANIO_BUILD_BENCHMARKS=ON
cmake --build build --config Release --target bench_spanio
build/benchmark/Release/bench_spanio --csv my.csv                  # 全部场景，约 10～20 分钟
build/benchmark/Release/bench_spanio tcp_latency --conns 1 --runs 3  # 单个场景、单个配置
cmake -DBASE=benchmark/results/0.3.0-linux.csv -DNEW=my.csv -P benchmark/compare.cmake
```

对比脚本逐项比较两份结果，按指标方向（越大越好 / 越小越好）判断，变差超过 10% 的标为 `REGRESSION` 并以失败退出；`-DTHRESHOLD=5` 可改阈值。

## 2. 方法

| 项目 | 做法 |
|---|---|
| 部署 | 客户端与服务端在同一进程，走 127.0.0.1；两侧各用独立的 `io_context`，服务端线程数由 `--threads` 决定（默认 1），客户端 1 个线程 |
| 计时 | 每次运行先预热 1 秒，再测 5 秒窗口；每个配置跑 5 次，报告中位数、最小值、最大值 |
| 稳定性 | 按报告精度（3 位小数）计算，最大值与最小值之差超过中位数的 10% 标为“不稳定” |
| 正确性 | 回显数据逐字节校验（TCP 按流偏移比对模式数据，UDP 按序号比对），任何错误都会在结果中报出 |
| 后端 | 网络场景在平台的每个后端上各跑一遍（Windows：原生 IOCP 与 AFD 模拟；Linux：epoll）；引擎场景只跑默认后端 |
| 编译 | Release（MSVC `/O2`，GCC `-O3`）；Debug 构建运行时会在输出里注明“数字无意义” |

场景说明：

| 场景 | 测什么 |
|---|---|
| `tcp_throughput` | 流水线回显：每个连接连续发送、不等回复，统计回显回来并校验通过的字节数 |
| `tcp_server_throughput` | 同上，服务端换成 `tcp_server`，衡量高层封装的开销 |
| `tcp_latency` | 一问一答：每个连接同一时刻只有一条消息在途，统计往返延迟分位数 |
| `tcp_connect_rate` | 8 个并发槽位反复“连接 → 收发 1 字节 → 关闭”（用 RST 关闭，避免 TIME_WAIT 耗尽端口） |
| `udp_throughput` | 单发送方尽快发送带序号的数据报，统计接收速率与丢包率 |
| `post_rate` / `strand_rate` | 不断重新投递自己的回调链（每线程 4 条），统计每秒执行的回调数 |
| `timer_rate` | 定时器“设置 + 取消”循环速率；以及 1 万个同时到期的定时器的处理速度 |

## 3. 测试环境

| | Windows | Linux |
|---|---|---|
| CPU | Intel Core i7-6700K @ 4.00 GHz，8 个硬件线程（物理机，桌面系统） | Intel Xeon E3-1268L v5 @ 2.40 GHz，**2 个 vCPU**（虚拟机） |
| 系统 | Windows 11 | Ubuntu 24.04，内核 6.8 |
| 编译器 | MSVC 19.51（VS 2026） | GCC 13.3 |
| 后端 | `iocp-proactor`（默认）、`reactor-proactor-afd` | `reactor-proactor-epoll` |

两台机器的 CPU、核数和虚拟化情况都不同，**跨平台的数字不能直接比较**，只能各自作为本平台的基线。

## 4. 基线结果（中位数）

### 4.1 TCP 回显吞吐（`tcp_throughput`，服务端 1 线程）

| 连接数 × 消息大小 | Windows IOCP | Windows AFD | Linux epoll |
|---|---|---|---|
| 1 × 64 B | 145 k msg/s（9.3 MB/s） | 116 k msg/s | 93 k msg/s |
| 1 × 1 KB | 125 k msg/s（128 MB/s） | 108 k msg/s | 89 k msg/s |
| 1 × 16 KB | 1032 MB/s | 872 MB/s | 1034 MB/s |
| 16 × 64 B | 53 k msg/s ⚠ | 66 k msg/s | 130 k msg/s |
| 16 × 1 KB | 58 k msg/s ⚠ | 65 k msg/s | 126 k msg/s |
| 16 × 16 KB | 684 MB/s | 698 MB/s ⚠ | 1384 MB/s |
| 64 × 64 B | 57 k msg/s | 66 k msg/s ⚠ | 132 k msg/s |
| 64 × 1 KB | 51 k msg/s ⚠ | 64 k msg/s | 127 k msg/s |
| 64 × 16 KB | 507 MB/s ⚠ | 698 MB/s | 1183 MB/s |

⚠ 表示该行 5 次运行的波动超过 10%。

### 4.2 `tcp_server` 与裸 socket 对比（16 连接）

| 消息大小 | Windows IOCP 裸 / tcp_server | Windows AFD 裸 / tcp_server | Linux 裸 / tcp_server |
|---|---|---|---|
| 64 B | 53 k / 56 k msg/s | 66 k / 67 k msg/s | 130 k / 128 k msg/s |
| 1 KB | 58 k / 56 k msg/s | 65 k / 66 k msg/s | 126 k / 123 k msg/s |
| 16 KB | 684 / 864 MB/s | 698 / 760 MB/s | 1384 / 1378 MB/s |

### 4.3 延迟（`tcp_latency`，64 B）

| 配置 | Windows IOCP | Windows AFD | Linux epoll |
|---|---|---|---|
| 1 连接 p50 / p99 / p999 | 32 / 62 / 97 µs | 35 / 69 / 111 µs | 43 / 91 / 162 µs |
| 1 连接往返次数 | 28.7 k/s | 26.6 k/s | 19.7 k/s |
| 16 连接 p50 / p99 / p999 | 337 / 542 / 744 µs ⚠ | 264 / 486 / 632 µs | 225 / 349 / 943 µs |
| 16 连接往返次数 | 47.0 k/s ⚠ | 55.1 k/s | 68.8 k/s |

### 4.4 连接、UDP、引擎

| 指标 | Windows（IOCP） | Linux |
|---|---|---|
| 连接速率（8 并发，连接 + 1 字节往返 + 关闭） | 8.6 k/s（AFD 8.2 k/s） | 18.3 k/s |
| UDP 64 B 接收速率 / 丢包 | 71 k/s / 0%（AFD 76 k/s） | 129 k/s / 0% |
| UDP 1 KB 接收速率 / 丢包 | 70 k/s / 0%（AFD 78 k/s） | 125 k/s / 0.06% |
| `post` 回调，1 线程 | 9.48 M/s | 10.97 M/s |
| `post` 回调，4 线程 | **1.78 M/s** | **3.29 M/s** |
| `strand` 回调，4 线程（每条链一个 strand） | 1.53 M/s | 1.94 M/s |
| 定时器设置 + 取消 | 4.59 M/s | 5.18 M/s |
| 定时器批量到期 | 3.32 M/s | 3.19 M/s |

## 5. 分析

### 5.1 主要瓶颈：`io_context` 的任务队列在多线程下互相争用

`post_rate` 从 1 个线程加到 4 个线程，吞吐不升反降：Windows 从 9.48 M/s 降到 1.78 M/s（降 81%），Linux 从 10.97 M/s 降到 3.29 M/s（降 70%，且这台虚拟机只有 2 个核）。

据推测，原因是 `io_context` 只有一把互斥锁保护唯一的任务队列，每个回调出队、入队都要拿这把锁，多个线程在锁上排队，外加条件变量唤醒的开销（`do_run_one` 里每次出队都可能 `notify_one`）。`strand` 在此基础上再多一把锁，因此更慢。这需要用性能分析工具确认（见 6 节）。

影响：所有用多线程 `run()` 的程序（包括 `tcp_server::run(n)`）都受它限制。

### 5.2 小消息的单次操作开销

64 B 消息时，单连接回显约 9～15 万条/秒，每条消息要经过 4 次异步操作（服务端读、写，客户端写、读）。据推测，每次操作的固定开销中，堆分配占比较大：每个异步操作都 `new` 一个 `io_op` 节点，完成后 `delete`；`tcp_server` 每次 `send()` 还会拷贝一份 `std::string` 放进发送队列。M4 规划中的内存池（操作节点复用）正是针对这一点。

### 5.3 Windows 下多连接小消息变慢

Linux 上 16 / 64 个连接比 1 个连接快（流水线更满，epoll 一次取回多个就绪事件），Windows 原生 IOCP 却从 145 k 降到 53 k msg/s，AFD 后端也从 116 k 降到 66 k。据推测与 Windows 下每个完成包单独出队、以及服务端单线程处理大量小完成有关；但这组数据在 Windows 上波动较大（⚠），需要先在更安静的环境下复测，再用分析工具定位。

### 5.4 高层封装开销很小

`tcp_server` 与裸 socket 回显在小消息下差距在 ±5% 以内。16 KB 时 Windows 上 `tcp_server` 反而快 9%～26%；两者的读缓冲同为 64 KB，原因尚未查明（裸 socket 的这几行本身波动也较大）。高层封装目前不是瓶颈。

### 5.5 其他

- 大消息（16 KB）单连接吞吐约 1 GB/s，主要受内存拷贝与内核回环路径限制。
- 定时器的设置、取消、到期都在每秒数百万次量级，不是瓶颈。
- UDP 回环下几乎不丢包，接收速率受单接收链（一次一个 `async_receive_from`）限制。

### 5.6 结果的可重复性

Linux 基线 57 行中只有 4 行不稳定，都是本来就容易波动的指标（p999 / p99 尾延迟，以及接近 0 的丢包率）。Windows 基线 109 行中有 28 行不稳定，集中在多连接吞吐和多连接延迟；据推测是桌面系统上其他程序的干扰。**回归对比以 Linux 为主**；Windows 上对比时建议加大 `--runs`，或在空闲时运行。

## 6. 对 M4 的建议（按收益排序）

1. **任务队列并发**（5.1）：先用分析工具（Windows：VS 性能探查器；Linux：`perf`）确认锁争用，再考虑：每线程本地队列 + 工作窃取，或批量出队以减少加锁次数，或线程本身运行的回调优先投递到本线程队列。目标：`post_rate` 4 线程不低于 1 线程。
2. **操作节点内存池**（5.2）：复用 `io_op` / `handler_op` 节点，消除每次异步操作的 `new` / `delete`。目标：64 B 单连接回显提升 20% 以上。
3. **发送队列**：`tcp_server` 的发送路径避免逐条拷贝和加锁（设计文档 8.2 的无锁发送队列）。
4. **栈上缓冲**（设计文档 8.3）：收益依赖前两项完成后的剖析结果再定。

每项优化完成后，都在同一台机器上重跑基准，并用 `compare.cmake` 与本基线对比。
