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

#include <cstdint>

namespace spanio { namespace test {

// Port number 0 asks the OS to pick a free ephemeral port when binding
constexpr std::uint16_t kAnyPort = 0;

// Backlog for test listeners; each listener serves a single connection
constexpr int kListenBacklog = 1;

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

}}

#endif // SPANIO_TESTS_TEST_NET_HELPERS_H
