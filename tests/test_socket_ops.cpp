// ============================================================================
// File purpose: Unit tests for spanio/detail/socket_ops.h: Winsock start-up,
//               socket creation, closing, non-blocking mode and error codes.
//               All network traffic stays on the loopback interface.
// Usage:        Built as the test_socket_ops executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name to run matching cases only, e.g.
//               `test_socket_ops close_socket`.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#include "spanio/detail/socket_ops.h"
#include "test_framework.h"
#include "test_net_helpers.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace spanio::detail;
using spanio::test::connect_to;
using spanio::test::local_address;
using spanio::test::make_loopback_listener;
using spanio::test::socket_guard;

namespace {

// Number of threads that race to initialize the network library
constexpr int kInitThreadCount = 8;

// Number of initialization calls made by each racing thread
constexpr int kInitCallsPerThread = 100;

// Address family value that no platform defines, used to force a failure
constexpr int kInvalidFamily = -1;

// Socket type value that no platform defines, used to force a failure
constexpr int kInvalidSocketType = -1;

// Receive timeout used to prove that a socket is in blocking mode
constexpr int kRecvTimeoutMs = 200;

// Minimum time a blocking recv must wait before timing out for the test to
// accept it as blocking; half the timeout absorbs timer granularity
constexpr int kMinBlockedMs = kRecvTimeoutMs / 2;

// Milliseconds per second, for converting the timeout to a timeval
constexpr int kMsPerSecond = 1000;

// Microseconds per millisecond, for converting the timeout to a timeval
constexpr int kUsPerMs = 1000;

// Size of the receive buffer used when probing blocking behaviour
constexpr int kProbeBufferSize = 1;

// recv flags: none
constexpr int kNoRecvFlags = 0;

// Make recv on a blocking socket give up after a timeout instead of waiting
// forever
// Parameter handle:     socket to configure
// Parameter timeout_ms: timeout in milliseconds
void set_recv_timeout(native_handle_t handle, int timeout_ms)
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // Winsock takes the timeout as a DWORD in milliseconds
    const DWORD timeout = static_cast<DWORD>(timeout_ms);
#else
    // POSIX takes the timeout as a timeval
    timeval timeout{};
    timeout.tv_sec = timeout_ms / kMsPerSecond;
    timeout.tv_usec = (timeout_ms % kMsPerSecond) * kUsPerMs;
#endif
    SPANIO_REQUIRE(::setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO,
                                reinterpret_cast<const char*>(&timeout),
                                static_cast<socklen_type>(sizeof(timeout)))
                   != kSocketCallFailed);
}

// Try to read one byte, measuring how long the call takes
// Parameter handle:     connected socket with no data waiting
// Parameter elapsed_ms: set to the time spent inside recv, in milliseconds
// Return value: the raw result of ::recv
long long recv_one_byte(native_handle_t handle, long long& elapsed_ms)
{
    char buffer[kProbeBufferSize];
    const auto start = std::chrono::steady_clock::now();
    const long long result = ::recv(handle, buffer, kProbeBufferSize, kNoRecvFlags);
    const auto stop = std::chrono::steady_clock::now();
    elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    return result;
}

#if defined(SPANIO_PLATFORM_LINUX)
// Read the O_NONBLOCK flag straight from the kernel
// Parameter handle: an open socket
// Return value: true if the descriptor is in non-blocking mode
bool has_nonblock_flag(native_handle_t handle)
{
    const int flags = ::fcntl(handle, F_GETFL);
    SPANIO_REQUIRE(flags != kSocketCallFailed);
    return (flags & O_NONBLOCK) != 0;
}
#endif

} // namespace

// Case 1: initialization can be repeated and raced from many threads
SPANIO_TEST(ensure_network_initialized_is_repeatable_and_thread_safe)
{
    SPANIO_CHECK(!ensure_network_initialized());
    SPANIO_CHECK(!ensure_network_initialized());

    // Count failures in the workers; the check macros are not thread-safe
    std::atomic<int> failures(0);
    auto initialize_repeatedly = [&failures]()
    {
        for (int call = 0; call < kInitCallsPerThread; ++call)
        {
            if (ensure_network_initialized())
            {
                ++failures;
            }
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < kInitThreadCount; ++i)
    {
        threads.emplace_back(initialize_repeatedly);
    }
    for (std::thread& t : threads)
    {
        t.join();
    }
    SPANIO_CHECK(failures.load() == 0);
}

// Case 2: IPv4/IPv6 x TCP/UDP sockets can be created and closed
SPANIO_TEST(create_socket_supports_ipv4_ipv6_tcp_udp)
{
    // One socket kind to try
    struct socket_kind
    {
        // Address family passed to create_socket
        int m_family;
        // Socket type passed to create_socket
        int m_type;
        // Protocol passed to create_socket
        int m_protocol;
    };
    const socket_kind kinds[] = {
        {AF_INET, SOCK_STREAM, IPPROTO_TCP},
        {AF_INET, SOCK_DGRAM, IPPROTO_UDP},
        {AF_INET6, SOCK_STREAM, IPPROTO_TCP},
        {AF_INET6, SOCK_DGRAM, IPPROTO_UDP},
    };

    for (const socket_kind& kind : kinds)
    {
        std::error_code ec;
        const native_handle_t handle =
            create_socket(kind.m_family, kind.m_type, kind.m_protocol, ec);
        SPANIO_CHECK(!ec);
        SPANIO_CHECK(handle != kInvalidHandle);
        if (handle != kInvalidHandle)
        {
            SPANIO_CHECK(!close_socket(handle));
        }
    }
}

// Case 3: invalid arguments give kInvalidHandle and a non-empty error
SPANIO_TEST(create_socket_rejects_invalid_arguments)
{
    std::error_code ec;
    native_handle_t handle = create_socket(kInvalidFamily, SOCK_STREAM, IPPROTO_TCP, ec);
    SPANIO_CHECK(handle == kInvalidHandle);
    SPANIO_CHECK(static_cast<bool>(ec));

    ec.clear();
    handle = create_socket(AF_INET, kInvalidSocketType, 0, ec);
    SPANIO_CHECK(handle == kInvalidHandle);
    SPANIO_CHECK(static_cast<bool>(ec));
}

// Case 4a: in non-blocking mode, connect returns at once with "in progress"
// (Linux EINPROGRESS) or "would block" (Winsock WSAEWOULDBLOCK)
SPANIO_TEST(set_nonblocking_true_makes_connect_return_immediately)
{
    socket_guard listener(make_loopback_listener());
    const sockaddr_in addr = local_address(listener.get());

    std::error_code ec;
    socket_guard client(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(!set_nonblocking(client.get(), true));
#if defined(SPANIO_PLATFORM_LINUX)
    SPANIO_CHECK(has_nonblock_flag(client.get()));
#endif

    const int result = connect_to(client.get(), addr);
    const std::error_code connect_ec = last_error();
    SPANIO_CHECK(result == kSocketCallFailed);
#if defined(SPANIO_PLATFORM_WINDOWS)
    SPANIO_CHECK(connect_ec == std::errc::operation_would_block);
#else
    SPANIO_CHECK(connect_ec == std::errc::operation_in_progress);
#endif
}

// Case 4b: switching back to blocking mode makes recv wait again. A blocking
// recv with SO_RCVTIMEO must take about the timeout to fail, whereas a
// non-blocking recv fails at once with "would block".
SPANIO_TEST(set_nonblocking_false_restores_blocking_mode)
{
    socket_guard listener(make_loopback_listener());
    const sockaddr_in addr = local_address(listener.get());

    std::error_code ec;
    socket_guard client(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    // A blocking connect to a listening loopback socket completes at once
    SPANIO_REQUIRE(connect_to(client.get(), addr) != kSocketCallFailed);
    socket_guard server(::accept(listener.get(), nullptr, nullptr));
    SPANIO_REQUIRE(server.get() != kInvalidHandle);

    // Non-blocking first: Winsock leaves a socket in an undefined state after
    // an SO_RCVTIMEO timeout, so the timeout probe must be the last step
    long long elapsed_ms = 0;
    SPANIO_REQUIRE(!set_nonblocking(client.get(), true));
    SPANIO_CHECK(recv_one_byte(client.get(), elapsed_ms) == kSocketCallFailed);
    SPANIO_CHECK(last_error() == std::errc::operation_would_block);
    SPANIO_CHECK(elapsed_ms < kMinBlockedMs);

    SPANIO_REQUIRE(!set_nonblocking(client.get(), false));
#if defined(SPANIO_PLATFORM_LINUX)
    SPANIO_CHECK(!has_nonblock_flag(client.get()));
#endif
    set_recv_timeout(client.get(), kRecvTimeoutMs);
    SPANIO_CHECK(recv_one_byte(client.get(), elapsed_ms) == kSocketCallFailed);
    SPANIO_CHECK(elapsed_ms >= kMinBlockedMs);
}

// Case 5: closing kInvalidHandle is rejected with bad_file_descriptor
SPANIO_TEST(close_socket_rejects_invalid_handle)
{
    SPANIO_CHECK(close_socket(kInvalidHandle) == std::errc::bad_file_descriptor);
}

// Case 6: operations on a handle that was already closed report an error.
// Nothing else opens sockets between the close and the checks, so the OS
// cannot have reused the handle value for a new socket.
SPANIO_TEST(operations_on_closed_handle_fail)
{
    std::error_code ec;
    const native_handle_t handle = create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(!close_socket(handle));

    SPANIO_CHECK(static_cast<bool>(close_socket(handle)));
    SPANIO_CHECK(static_cast<bool>(set_nonblocking(handle, true)));
}

#if defined(SPANIO_PLATFORM_LINUX)
// Case 7: sockets are created with FD_CLOEXEC so exec() does not leak them
SPANIO_TEST(create_socket_sets_cloexec)
{
    std::error_code ec;
    socket_guard handle(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    const int fd_flags = ::fcntl(handle.get(), F_GETFD);
    SPANIO_REQUIRE(fd_flags != kSocketCallFailed);
    SPANIO_CHECK((fd_flags & FD_CLOEXEC) != 0);
}
#endif

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
