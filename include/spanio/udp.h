// ============================================================================
// File purpose: UDP networking API on top of io_context: udp::socket sends
//               and receives datagrams with handlers that run inside
//               io_context::run(), and udp::endpoint is an address + port.
// Usage:        spanio::io_context ctx;
//               spanio::udp::socket sock(ctx, spanio::udp::endpoint(spanio::ipv4::any(), 9000));
//               char data[1500];
//               auto on_receive = [&](std::error_code ec, std::size_t bytes,
//                                     const spanio::udp::endpoint& sender)
//               {
//                   ...
//               };
//               sock.async_receive_from(spanio::buffer(data), on_receive);
//               ctx.run();
//
//               Handler signatures:
//               - async_receive_from: void(std::error_code, std::size_t,
//                                          const udp::endpoint& sender)
//               - async_send_to:      void(std::error_code, std::size_t)
//
//               Rules:
//               - Handlers never run inside the call that starts the
//                 operation, and every operation runs its handler once.
//               - One operation is one datagram; datagrams never merge, and
//                 a send is never partial.
//               - An empty datagram is valid: it arrives as success with 0
//                 bytes. UDP has no end of file.
//               - A datagram larger than the receive buffer reports
//                 error::misc_errors::datagram_truncated; the part that fit is in the
//                 buffer and the rest is lost.
//               - A datagram too large for the protocol fails its send with
//                 errc::message_size.
//               - At most one receive and one send may be outstanding per
//                 socket (io_proactor.h). This is not checked.
//               - Buffers must stay valid until the handler has run.
//               - close(), cancel() and the destructor make pending
//                 operations complete with errc::operation_canceled.
//               - A port is exclusive to one socket on every platform.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#ifndef SPANIO_UDP_H
#define SPANIO_UDP_H

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/detail/io_proactor.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/scheduler_op.h"
#include "spanio/detail/socket_common.h"
#include "spanio/detail/socket_ops.h"

#include <cstddef>
#include <system_error>
#include <type_traits>
#include <utility>

namespace spanio { namespace udp {

// An IP address and a port
using endpoint = ip::endpoint;

}}

namespace spanio { namespace detail {

// Task that reports a receive that needs no I/O: the handler gets a stored
// error, 0 bytes and a default sender
template <typename Handler>
class receive_from_binder
{
public:
    // Parameter handler: function object taking (std::error_code,
    //                    std::size_t, const udp::endpoint&)
    // Parameter ec:      error to pass
    template <typename H>
    receive_from_binder(H&& handler, const std::error_code& ec)
        : m_handler(std::forward<H>(handler))
        , m_ec(ec)
    {
    }

    // Call the handler with the stored error
    void operator()()
    {
        m_handler(m_ec, static_cast<std::size_t>(0), udp::endpoint());
    }

private:
    // User handler
    Handler m_handler;
    // Error to pass
    std::error_code m_ec;
};

// Completion of a datagram receive: passes the error, byte count and sender
template <typename Handler>
class receive_from_completion
{
public:
    // Parameter handler: function object taking (std::error_code,
    //                    std::size_t, const udp::endpoint&)
    template <typename H>
    explicit receive_from_completion(H&& handler)
        : m_handler(std::forward<H>(handler))
    {
    }

    // Parameter result: the finished receive
    void operator()(const io_result& result)
    {
        std::error_code ec = result.m_ec;
        const bool truncated = ec == std::errc::message_size;
        if (truncated)
        {
            ec = error::misc_errors::datagram_truncated;
        }
        udp::endpoint sender;
        if (!result.m_ec || truncated)
        {
            sender = udp::endpoint::from_native(reinterpret_cast<const sockaddr*>(&result.m_address),
                                                result.m_address_len);
        }
        m_handler(ec, result.m_bytes, sender);
    }

private:
    // User handler
    Handler m_handler;
};

}}

namespace spanio { namespace udp {

// A UDP socket. Movable, not copyable. Destroying or closing it cancels its
// pending operations; their handlers still run once.
class socket
{
public:
    // Native socket handle type (SOCKET on Windows, int on Linux)
    using native_handle_type = detail::native_handle_t;

    // Create a socket that is not open yet
    // Parameter context: context that runs its handlers; must outlive it
    explicit socket(io_context& context) noexcept
        : m_context(&context)
        , m_handle(detail::kInvalidHandle)
    {
    }

    // Open and bind in one step; throws std::system_error on failure
    // Parameter context: context that runs its handlers; must outlive it
    // Parameter local:   address and port to bind; port 0 picks one
    socket(io_context& context, const endpoint& local)
        : socket(context)
    {
        std::error_code ec = open(local.family());
        if (!ec)
        {
            ec = bind(local);
        }
        if (ec)
        {
            close();
            throw std::system_error(ec, "spanio::udp::socket");
        }
    }

    // Take over another socket
    // Parameter other: socket to move from; it is not open afterwards
    socket(socket&& other) noexcept
        : m_context(other.m_context)
        , m_handle(other.m_handle)
    {
        other.m_handle = detail::kInvalidHandle;
    }

    // Close this socket, then take over another one
    // Parameter other: socket to move from; it is not open afterwards
    // Return value: *this
    socket& operator=(socket&& other) noexcept
    {
        if (this != &other)
        {
            close();
            m_context = other.m_context;
            m_handle = other.m_handle;
            other.m_handle = detail::kInvalidHandle;
        }
        return *this;
    }

    socket(const socket&) = delete;
    socket& operator=(const socket&) = delete;

    // Close the socket, cancelling pending operations
    ~socket()
    {
        close();
    }

    // Context that runs this socket's handlers
    // Return value: the context
    io_context& context() const noexcept
    {
        return *m_context;
    }

    // Create the native socket, apply the platform settings and register it
    // with the context
    // Parameter family: AF_INET or AF_INET6
    // Return value: empty on success; errc::invalid_argument if already
    //               open; otherwise the system error
    std::error_code open(int family)
    {
        if (is_open())
        {
            return detail::already_open_error();
        }
        std::error_code ec;
        const detail::native_handle_t handle = detail::create_socket(family, SOCK_DGRAM, IPPROTO_UDP, ec);
        if (ec)
        {
            detail::log_error("udp::socket::open", ec);
            return ec;
        }
        ec = detail::configure_datagram_socket(handle);
        if (!ec)
        {
            ec = detail::context_access::register_handle(*m_context, handle);
        }
        if (ec)
        {
            detail::log_error("udp::socket::open: setup", ec);
            detail::close_socket(handle);
            return ec;
        }
        m_handle = handle;
        return std::error_code();
    }

    // Bind to a local address
    // Parameter local: address and port; port 0 lets the system pick one
    // Return value: empty on success, otherwise the system error (e.g.
    //               errc::address_in_use)
    std::error_code bind(const endpoint& local)
    {
        if (!is_open())
        {
            return detail::not_open_error();
        }
        if (::bind(m_handle, local.data(), local.size()) == detail::kSocketCallFailed)
        {
            const std::error_code ec = detail::last_error();
            detail::log_error("udp::socket::bind", ec);
            return ec;
        }
        return std::error_code();
    }

    // Whether the socket is open
    // Return value: true when open
    bool is_open() const noexcept
    {
        return m_handle != detail::kInvalidHandle;
    }

    // The native handle, for socket options not wrapped here
    // Return value: the handle, or kInvalidHandle when not open
    native_handle_type native_handle() const noexcept
    {
        return m_handle;
    }

    // Local address, e.g. to learn the port picked for port 0
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint, or a default endpoint on failure
    endpoint local_endpoint(std::error_code& ec) const
    {
        return detail::query_endpoint(m_handle, false, ec);
    }

    // Cancel pending operations; their handlers get errc::operation_canceled
    void cancel()
    {
        if (is_open())
        {
            detail::context_access::cancel_handle(*m_context, m_handle);
        }
    }

    // Unregister and close the socket. Pending operations complete with
    // errc::operation_canceled. Does nothing when not open.
    void close()
    {
        if (!is_open())
        {
            return;
        }
        std::error_code ec = detail::context_access::unregister_handle(*m_context, m_handle);
        if (ec)
        {
            detail::log_error("udp::socket::close: unregister_handle", ec);
        }
        ec = detail::close_socket(m_handle);
        if (ec)
        {
            detail::log_error("udp::socket::close", ec);
        }
        m_handle = detail::kInvalidHandle;
    }

    // Receive one datagram
    // Parameter buffer:  destination; valid until the handler runs. An empty
    //                    buffer succeeds at once with 0 bytes and takes no
    //                    datagram.
    // Parameter handler: called as handler(std::error_code, bytes received,
    //                    const udp::endpoint& sender)
    template <typename Handler>
    void async_receive_from(const mutable_buffer& buffer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open() || buffer.size() == 0)
        {
            const std::error_code ec = is_open() ? std::error_code() : detail::not_open_error();
            m_context->post(detail::receive_from_binder<handler_type>(std::forward<Handler>(handler), ec));
            return;
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::receive_from_completion<handler_type>(std::forward<Handler>(handler)));
        detail::prepare_receive_from(*op, m_handle, static_cast<char*>(buffer.data()), buffer.size());
        detail::context_access::start_io(*m_context, op);
    }

    // Send one datagram, opening the socket first if needed
    // Parameter buffer:  source; valid until the handler runs. An empty
    //                    buffer sends an empty datagram.
    // Parameter peer:    destination
    // Parameter handler: called as handler(std::error_code, bytes sent)
    template <typename Handler>
    void async_send_to(const const_buffer& buffer, const endpoint& peer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open())
        {
            const std::error_code ec = open(peer.family());
            if (ec)
            {
                m_context->post(detail::transfer_binder<handler_type>(std::forward<Handler>(handler), ec, 0));
                return;
            }
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::transfer_completion<handler_type>(std::forward<Handler>(handler), false));
        detail::prepare_send_to(*op, m_handle, static_cast<const char*>(buffer.data()), buffer.size(),
                                peer.data(), peer.size());
        detail::context_access::start_io(*m_context, op);
    }

private:
    // Context that runs the handlers; never null
    io_context* m_context;
    // Native socket, or kInvalidHandle when not open
    detail::native_handle_t m_handle;
};

}}

#endif // SPANIO_UDP_H
