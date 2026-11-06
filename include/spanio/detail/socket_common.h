// ============================================================================
// File purpose: Pieces shared by the socket classes of tcp.h and udp.h: the
//               errors for sockets in the wrong state, reading a socket's
//               addresses, posting a result known before an operation
//               starts, and the completion of a plain transfer.
// Usage:        Internal to tcp.h and udp.h.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#ifndef SPANIO_DETAIL_SOCKET_COMMON_H
#define SPANIO_DETAIL_SOCKET_COMMON_H

#include "spanio/error.h"
#include "spanio/ip.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/scheduler_op.h"
#include "spanio/detail/socket_ops.h"

#include <cstddef>
#include <system_error>
#include <utility>

namespace spanio { namespace detail {

// Error used when an operation is started on a socket that is not open
// Return value: errc::bad_file_descriptor
inline std::error_code not_open_error() noexcept
{
    return std::make_error_code(std::errc::bad_file_descriptor);
}

// Error used when open() / assign() is called on a socket that is open
// Return value: errc::invalid_argument
inline std::error_code already_open_error() noexcept
{
    return std::make_error_code(std::errc::invalid_argument);
}

// Read the local or remote address of a socket
// Parameter handle: open socket
// Parameter remote: true for the peer (getpeername), false for the local side
// Parameter ec:     set to the failure reason, or cleared
// Return value: the endpoint, or a default endpoint on failure
inline ip::endpoint query_endpoint(native_handle_t handle, bool remote, std::error_code& ec)
{
    ec.clear();
    if (handle == kInvalidHandle)
    {
        ec = not_open_error();
        return ip::endpoint();
    }
    sockaddr_storage storage{};
    socklen_type length = static_cast<socklen_type>(sizeof(storage));
    sockaddr* address = reinterpret_cast<sockaddr*>(&storage);
    const int result = remote ? ::getpeername(handle, address, &length)
                              : ::getsockname(handle, address, &length);
    if (result == kSocketCallFailed)
    {
        ec = last_error();
        return ip::endpoint();
    }
    return ip::endpoint::from_native(address, length);
}

// Task that calls a handler with a stored error; used to report failures
// that are known before an operation starts, without running it inline
template <typename Handler>
class error_binder
{
public:
    // Parameter handler: function object taking (std::error_code)
    // Parameter ec:      error to pass
    template <typename H>
    error_binder(H&& handler, const std::error_code& ec)
        : m_handler(std::forward<H>(handler))
        , m_ec(ec)
    {
    }

    // Call the handler with the stored error
    void operator()()
    {
        m_handler(m_ec);
    }

private:
    // User handler
    Handler m_handler;
    // Error to pass
    std::error_code m_ec;
};

// Task that calls a handler with a stored error and byte count
template <typename Handler>
class transfer_binder
{
public:
    // Parameter handler: function object taking (std::error_code, std::size_t)
    // Parameter ec:      error to pass
    // Parameter bytes:   byte count to pass
    template <typename H>
    transfer_binder(H&& handler, const std::error_code& ec, std::size_t bytes)
        : m_handler(std::forward<H>(handler))
        , m_ec(ec)
        , m_bytes(bytes)
    {
    }

    // Call the handler with the stored values
    void operator()()
    {
        m_handler(m_ec, m_bytes);
    }

private:
    // User handler
    Handler m_handler;
    // Error to pass
    std::error_code m_ec;
    // Byte count to pass
    std::size_t m_bytes;
};

// Completion of a single recv or send: passes the error and byte count.
// For reads, 0 bytes without an error becomes spanio::error::eof.
template <typename Handler>
class transfer_completion
{
public:
    // Parameter handler: function object taking (std::error_code, std::size_t)
    // Parameter is_read: whether 0 bytes means end of file
    template <typename H>
    transfer_completion(H&& handler, bool is_read)
        : m_handler(std::forward<H>(handler))
        , m_is_read(is_read)
    {
    }

    // Parameter result: the finished operation
    void operator()(const io_result& result)
    {
        std::error_code ec = result.m_ec;
        if (!ec && m_is_read && result.m_bytes == 0)
        {
            ec = error::eof;
        }
        m_handler(ec, result.m_bytes);
    }

private:
    // User handler
    Handler m_handler;
    // true for recv, false for send
    bool m_is_read;
};


}}

#endif // SPANIO_DETAIL_SOCKET_COMMON_H
