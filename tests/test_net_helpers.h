// ============================================================================
// File purpose: Socket helpers shared by the SpanIO unit tests: an owning
//               socket guard, loopback listeners and connected TCP pairs.
//               All traffic stays on 127.0.0.1.
// Usage:        Include after test_framework.h. Helpers that can fail use
//               SPANIO_REQUIRE, so they may only be called inside a test case.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_TESTS_TEST_NET_HELPERS_H
#define SPANIO_TESTS_TEST_NET_HELPERS_H

#include "spanio/detail/socket_ops.h"
#include "test_framework.h"

#include <cstddef>
#include <cstdint>

namespace spanio { namespace test {

// Port number 0 asks the OS to pick a free ephemeral port when binding
constexpr std::uint16_t kAnyPort = 0;

// Backlog for test listeners; each listener serves a single connection
constexpr int kListenBacklog = 1;

// Receive timeout of blocking test clients
constexpr int kClientReceiveTimeoutMs = 5000;

// Milliseconds per second, for the Linux SO_RCVTIMEO timeval
constexpr int kMillisPerSecond = 1000;

// Microseconds per millisecond, for the Linux SO_RCVTIMEO timeval
constexpr int kMicrosPerMilli = 1000;

// recv/send flags of blocking test clients: none
constexpr int kNoSocketFlags = 0;

// recv/send result at or below which the connection is closed or failed
constexpr int kSocketClosedResult = 0;

// Owns a socket handle and closes it when the scope ends, so a failed
// SPANIO_REQUIRE does not leak the handle
class socket_guard
{
public:
    // Take ownership of a handle
    // Parameter handle: handle to own; kInvalidHandle means nothing is owned
    explicit socket_guard(detail::native_handle_t handle = detail::kInvalidHandle)
        : m_handle(handle)
    {
    }

    // Close the owned handle, if any
    ~socket_guard()
    {
        reset();
    }

    socket_guard(const socket_guard&) = delete;
    socket_guard& operator=(const socket_guard&) = delete;

    // Access the owned handle without giving up ownership
    // Return value: the owned handle, or kInvalidHandle
    detail::native_handle_t get() const
    {
        return m_handle;
    }

    // Give up ownership without closing the handle
    // Return value: the previously owned handle
    detail::native_handle_t release()
    {
        const detail::native_handle_t handle = m_handle;
        m_handle = detail::kInvalidHandle;
        return handle;
    }

    // Close the owned handle (if any) and take ownership of another one
    // Parameter handle: new handle to own; kInvalidHandle leaves it empty
    void reset(detail::native_handle_t handle = detail::kInvalidHandle)
    {
        if (m_handle != detail::kInvalidHandle)
        {
            detail::close_socket(m_handle);
        }
        m_handle = handle;
    }

private:
    // Handle closed by the destructor; kInvalidHandle when nothing is owned
    detail::native_handle_t m_handle;
};

// Build an IPv4 loopback address
// Parameter port: port in host byte order
// Return value: 127.0.0.1:port
inline sockaddr_in loopback_address(std::uint16_t port)
{
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    return addr;
}

// Open a TCP socket listening on 127.0.0.1 at an OS-chosen port
// Return value: the listening socket; the caller owns it
inline detail::native_handle_t make_loopback_listener()
{
    std::error_code ec;
    socket_guard listener(detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);

    const sockaddr_in addr = loopback_address(kAnyPort);
    SPANIO_REQUIRE(::bind(listener.get(), reinterpret_cast<const sockaddr*>(&addr),
                          static_cast<detail::socklen_type>(sizeof(addr)))
                   != detail::kSocketCallFailed);
    SPANIO_REQUIRE(::listen(listener.get(), kListenBacklog) != detail::kSocketCallFailed);
    return listener.release();
}

// Look up the local address a socket is bound to
// Parameter handle: a bound socket
// Return value: its IPv4 address and port
inline sockaddr_in local_address(detail::native_handle_t handle)
{
    sockaddr_in addr{};
    detail::socklen_type len = static_cast<detail::socklen_type>(sizeof(addr));
    SPANIO_REQUIRE(::getsockname(handle, reinterpret_cast<sockaddr*>(&addr), &len)
                   != detail::kSocketCallFailed);
    return addr;
}

// Start a TCP connect to a socket address
// Parameter handle: unconnected client socket
// Parameter addr:   address to connect to
// Return value: the raw result of ::connect (0 or kSocketCallFailed)
inline int connect_to(detail::native_handle_t handle, const sockaddr_in& addr)
{
    return ::connect(handle, reinterpret_cast<const sockaddr*>(&addr),
                     static_cast<detail::socklen_type>(sizeof(addr)));
}

// Create a connected TCP pair over loopback. Both sockets are blocking.
// Parameter client: receives the connecting side
// Parameter server: receives the accepted side
inline void make_tcp_pair(socket_guard& client, socket_guard& server)
{
    socket_guard listener(make_loopback_listener());
    const sockaddr_in addr = local_address(listener.get());

    std::error_code ec;
    client.reset(detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    // A blocking connect to a listening loopback socket completes at once
    SPANIO_REQUIRE(connect_to(client.get(), addr) != detail::kSocketCallFailed);
    server.reset(::accept(listener.get(), nullptr, nullptr));
    SPANIO_REQUIRE(server.get() != detail::kInvalidHandle);
}

// Give a blocking socket a receive timeout, so a server bug fails a case
// instead of hanging it
// Parameter handle: socket to configure
// Return value: true on success
inline bool set_receive_timeout(detail::native_handle_t handle)
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    const DWORD timeout = kClientReceiveTimeoutMs;
#else
    timeval timeout{};
    timeout.tv_sec = kClientReceiveTimeoutMs / kMillisPerSecond;
    timeout.tv_usec = (kClientReceiveTimeoutMs % kMillisPerSecond) * kMicrosPerMilli;
#endif
    return ::setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout),
                        static_cast<detail::socklen_type>(sizeof(timeout)))
        != detail::kSocketCallFailed;
}

// Open a blocking client connected to a server on 127.0.0.1
// Parameter port:   server port
// Parameter client: receives the connected socket
// Return value: true on success; it uses no check macros, so it is safe in
//               helper threads
inline bool connect_client(std::uint16_t port, socket_guard& client)
{
    std::error_code ec;
    client.reset(detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    if (ec)
    {
        return false;
    }
    return set_receive_timeout(client.get()) && connect_to(client.get(), loopback_address(port)) == 0;
}

// Send a whole buffer on a blocking socket
// Parameter handle: connected socket
// Parameter data:   bytes to send
// Parameter size:   number of bytes
// Return value: true if every byte was sent
inline bool send_all(detail::native_handle_t handle, const char* data, std::size_t size)
{
    std::size_t sent = 0;
    while (sent < size)
    {
        const int result = ::send(handle, data + sent, static_cast<int>(size - sent), kNoSocketFlags);
        if (result <= kSocketClosedResult)
        {
            return false;
        }
        sent += static_cast<std::size_t>(result);
    }
    return true;
}

// Receive exactly a number of bytes on a blocking socket
// Parameter handle: connected socket
// Parameter data:   destination
// Parameter size:   number of bytes to receive
// Return value: true if all of them arrived before a close, error or timeout
inline bool recv_exact(detail::native_handle_t handle, char* data, std::size_t size)
{
    std::size_t received = 0;
    while (received < size)
    {
        const int result = ::recv(handle, data + received, static_cast<int>(size - received),
                                  kNoSocketFlags);
        if (result <= kSocketClosedResult)
        {
            return false;
        }
        received += static_cast<std::size_t>(result);
    }
    return true;
}

}}

#endif // SPANIO_TESTS_TEST_NET_HELPERS_H
