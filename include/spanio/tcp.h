// ============================================================================
// File purpose: TCP networking API on top of io_context:
//               - tcp::endpoint: IP address + port
//               - tcp::socket:   a connection; async connect / read / write
//               - tcp::acceptor: a listening socket; async accept
//               Handlers always run inside io_context::run(), never inline
//               in the call that starts the operation, and every started
//               operation runs its handler exactly once.
// Usage:        spanio::io_context ctx;
//               spanio::tcp::acceptor acc(ctx, spanio::tcp::endpoint(spanio::ipv4::any(), 8080));
//               auto on_accept = [&](std::error_code ec, spanio::tcp::socket peer)
//               {
//                   ...
//               };
//               acc.async_accept(on_accept);
//               ctx.run();
//
//               Handler signatures:
//               - async_connect:                    void(std::error_code)
//               - async_read_some / async_write_some
//                 / async_write:                    void(std::error_code, std::size_t)
//               - async_accept:                     void(std::error_code, tcp::socket)
//
//               Rules shared with the proactor (io_proactor.h):
//               - At most one read (async_read_some / async_accept) and one
//                 write (async_connect / async_write_some / async_write) may
//                 be outstanding per socket. This is not checked.
//               - Buffers must stay valid until the handler has run.
//               - close(), cancel() and the destructor make pending
//                 operations complete with errc::operation_canceled.
//               - A read of a non-empty buffer that gets 0 bytes reports
//                 spanio::error::eof.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_TCP_H
#define SPANIO_TCP_H

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/detail/io_proactor.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/scheduler_op.h"
#include "spanio/detail/socket_ops.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace spanio { namespace tcp {

// Default listen() backlog: the largest the system allows
constexpr int kDefaultBacklog = SOMAXCONN;

// Value that turns a boolean socket option on
constexpr int kSocketOptionOn = 1;

// An IP address and a port. Default-constructed it is 0.0.0.0:0.
class endpoint
{
public:
    // The IPv4 wildcard address with port 0
    endpoint() noexcept
        : endpoint(ip::address(), 0)
    {
    }

    // Parameter address: IPv4 or IPv6 address
    // Parameter port:    port in host byte order; 0 lets bind() pick one
    endpoint(const ip::address& address, std::uint16_t port) noexcept
        : m_data{}
        , m_size(0)
    {
        if (address.is_v4())
        {
            sockaddr_in v4{};
            v4.sin_family = AF_INET;
            v4.sin_port = htons(port);
            v4.sin_addr = address.to_v4();
            std::memcpy(&m_data, &v4, sizeof(v4));
            m_size = static_cast<detail::socklen_type>(sizeof(v4));
        }
        else
        {
            sockaddr_in6 v6{};
            v6.sin6_family = AF_INET6;
            v6.sin6_port = htons(port);
            v6.sin6_addr = address.to_v6();
            v6.sin6_scope_id = address.scope_id();
            std::memcpy(&m_data, &v6, sizeof(v6));
            m_size = static_cast<detail::socklen_type>(sizeof(v6));
        }
    }

    // Build an endpoint from a socket address returned by the system
    // Parameter data: sockaddr_in or sockaddr_in6
    // Parameter size: bytes valid at data
    // Return value: the endpoint
    static endpoint from_native(const sockaddr* data, detail::socklen_type size) noexcept
    {
        endpoint result;
        std::size_t length = static_cast<std::size_t>(size);
        if (length > sizeof(result.m_data))
        {
            length = sizeof(result.m_data);
        }
        std::memset(&result.m_data, 0, sizeof(result.m_data));
        std::memcpy(&result.m_data, data, length);
        result.m_size = static_cast<detail::socklen_type>(length);
        return result;
    }

    // Address family
    // Return value: AF_INET or AF_INET6
    int family() const noexcept
    {
        return m_data.ss_family;
    }

    // The IP address
    // Return value: the address part
    ip::address address() const noexcept
    {
        if (family() == AF_INET6)
        {
            sockaddr_in6 v6{};
            std::memcpy(&v6, &m_data, sizeof(v6));
            return ip::address::from_v6(v6.sin6_addr, v6.sin6_scope_id);
        }
        sockaddr_in v4{};
        std::memcpy(&v4, &m_data, sizeof(v4));
        return ip::address::from_v4(v4.sin_addr);
    }

    // The port
    // Return value: port in host byte order
    std::uint16_t port() const noexcept
    {
        if (family() == AF_INET6)
        {
            sockaddr_in6 v6{};
            std::memcpy(&v6, &m_data, sizeof(v6));
            return ntohs(v6.sin6_port);
        }
        sockaddr_in v4{};
        std::memcpy(&v4, &m_data, sizeof(v4));
        return ntohs(v4.sin_port);
    }

    // The socket address for system calls
    // Return value: pointer to the stored address
    const sockaddr* data() const noexcept
    {
        return reinterpret_cast<const sockaddr*>(&m_data);
    }

    // Number of meaningful bytes at data()
    // Return value: sizeof(sockaddr_in) or sizeof(sockaddr_in6)
    detail::socklen_type size() const noexcept
    {
        return m_size;
    }

    // Two endpoints are equal when address and port match
    // Parameter other: endpoint to compare with
    // Return value: true when equal
    bool operator==(const endpoint& other) const noexcept
    {
        return address() == other.address() && port() == other.port();
    }

    // Negation of operator==
    // Parameter other: endpoint to compare with
    // Return value: true when different
    bool operator!=(const endpoint& other) const noexcept
    {
        return !(*this == other);
    }

private:
    // The socket address (sockaddr_in or sockaddr_in6)
    sockaddr_storage m_data;
    // Number of meaningful bytes in m_data
    detail::socklen_type m_size;
};

// Which direction socket::shutdown() closes
enum shutdown_type
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // Stop receiving: further reads fail
    shutdown_receive = SD_RECEIVE,
    // Stop sending: the peer reads end of file after the pending data
    shutdown_send = SD_SEND,
    // Both directions
    shutdown_both = SD_BOTH
#else
    // Stop receiving: further reads fail
    shutdown_receive = SHUT_RD,
    // Stop sending: the peer reads end of file after the pending data
    shutdown_send = SHUT_WR,
    // Both directions
    shutdown_both = SHUT_RDWR
#endif
};

}}

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
inline tcp::endpoint query_endpoint(native_handle_t handle, bool remote, std::error_code& ec)
{
    ec.clear();
    if (handle == kInvalidHandle)
    {
        ec = not_open_error();
        return tcp::endpoint();
    }
    sockaddr_storage storage{};
    socklen_type length = static_cast<socklen_type>(sizeof(storage));
    sockaddr* address = reinterpret_cast<sockaddr*>(&storage);
    const int result = remote ? ::getpeername(handle, address, &length)
                              : ::getsockname(handle, address, &length);
    if (result == kSocketCallFailed)
    {
        ec = last_error();
        return tcp::endpoint();
    }
    return tcp::endpoint::from_native(address, length);
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

// Completion of a connect: passes only the error
template <typename Handler>
class connect_completion
{
public:
    // Parameter handler: function object taking (std::error_code)
    template <typename H>
    explicit connect_completion(H&& handler)
        : m_handler(std::forward<H>(handler))
    {
    }

    // Parameter result: the finished operation
    void operator()(const io_result& result)
    {
        m_handler(result.m_ec);
    }

private:
    // User handler
    Handler m_handler;
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

// Composed operation behind socket::async_write: keeps sending the rest of
// the buffer until everything is written or an error occurs. The object
// travels from one send to the next as the completion of each send.
template <typename Handler>
class write_all_op
{
public:
    // Parameter context: context the socket belongs to
    // Parameter handle:  open, registered socket
    // Parameter data:    bytes to send; valid until the handler runs
    // Parameter size:    number of bytes; greater than 0
    // Parameter handler: function object taking (std::error_code, std::size_t)
    template <typename H>
    write_all_op(io_context& context, native_handle_t handle, const char* data,
                 std::size_t size, H&& handler)
        : m_context(&context)
        , m_handle(handle)
        , m_data(data)
        , m_size(size)
        , m_written(0)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Start a send of the part not written yet. This object is moved into
    // the operation, so it must not be used afterwards.
    void start()
    {
        io_context& context = *m_context;
        const native_handle_t handle = m_handle;
        const char* rest = m_data + m_written;
        const std::size_t rest_size = m_size - m_written;
        io_op_base* op = make_io_op(std::move(*this));
        prepare_send(*op, handle, rest, rest_size);
        context.start_io(op);
    }

    // Completion of one send: finish, or continue with the rest
    // Parameter result: the finished send
    void operator()(const io_result& result)
    {
        m_written += result.m_bytes;
        std::error_code ec = result.m_ec;
        if (!ec && result.m_bytes == 0)
        {
            // A send of a non-empty buffer never transfers 0 bytes without
            // an error; stop instead of looping forever if it ever does
            ec = std::make_error_code(std::errc::io_error);
            log_error("tcp::socket::async_write: send transferred 0 bytes", ec);
        }
        if (ec || m_written == m_size)
        {
            m_handler(ec, m_written);
            return;
        }
        start();
    }

private:
    // Context the sends run on
    io_context* m_context;
    // Socket being written
    native_handle_t m_handle;
    // Start of the whole buffer
    const char* m_data;
    // Size of the whole buffer
    std::size_t m_size;
    // Bytes sent so far
    std::size_t m_written;
    // User handler, called once at the end
    Handler m_handler;
};

}}

namespace spanio { namespace tcp {

// A TCP connection. Movable, not copyable. Destroying or closing it cancels
// its pending operations; their handlers still run once.
class socket
{
public:
    // Create a socket that is not open yet
    // Parameter context: context that runs its handlers; must outlive it
    explicit socket(io_context& context) noexcept
        : m_context(&context)
        , m_handle(detail::kInvalidHandle)
    {
    }

    // Take over another socket's connection
    // Parameter other: socket to move from; it is not open afterwards
    socket(socket&& other) noexcept
        : m_context(other.m_context)
        , m_handle(other.m_handle)
    {
        other.m_handle = detail::kInvalidHandle;
    }

    // Close this socket, then take over another socket's connection
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

    // Create the native socket and register it with the context
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
        const detail::native_handle_t handle = detail::create_socket(family, SOCK_STREAM, IPPROTO_TCP, ec);
        if (ec)
        {
            detail::log_error("tcp::socket::open", ec);
            return ec;
        }
        ec = m_context->register_handle(handle);
        if (ec)
        {
            detail::log_error("tcp::socket::open: register_handle", ec);
            detail::close_socket(handle);
            return ec;
        }
        m_handle = handle;
        return std::error_code();
    }

    // Adopt an existing connected socket and register it with the context.
    // On failure the handle is left untouched and still owned by the caller.
    // Parameter handle: open TCP socket
    // Return value: empty on success; errc::invalid_argument if already
    //               open; otherwise the registration error
    std::error_code assign(detail::native_handle_t handle)
    {
        if (is_open())
        {
            return detail::already_open_error();
        }
        const std::error_code ec = m_context->register_handle(handle);
        if (ec)
        {
            detail::log_error("tcp::socket::assign: register_handle", ec);
            return ec;
        }
        m_handle = handle;
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
    detail::native_handle_t native_handle() const noexcept
    {
        return m_handle;
    }

    // Local address of the socket
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint, or a default endpoint on failure
    endpoint local_endpoint(std::error_code& ec) const
    {
        return detail::query_endpoint(m_handle, false, ec);
    }

    // Address of the peer
    // Parameter ec: set to the failure reason (e.g. not connected), or cleared
    // Return value: the endpoint, or a default endpoint on failure
    endpoint remote_endpoint(std::error_code& ec) const
    {
        return detail::query_endpoint(m_handle, true, ec);
    }

    // Close one or both directions of the connection
    // Parameter what: direction to close
    // Return value: empty on success, otherwise the system error
    std::error_code shutdown(shutdown_type what)
    {
        if (!is_open())
        {
            return detail::not_open_error();
        }
        if (::shutdown(m_handle, static_cast<int>(what)) == detail::kSocketCallFailed)
        {
            const std::error_code ec = detail::last_error();
            detail::log_error("tcp::socket::shutdown", ec);
            return ec;
        }
        return std::error_code();
    }

    // Cancel pending operations; their handlers get errc::operation_canceled
    void cancel()
    {
        if (is_open())
        {
            m_context->cancel_handle(m_handle);
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
        std::error_code ec = m_context->unregister_handle(m_handle);
        if (ec)
        {
            detail::log_error("tcp::socket::close: unregister_handle", ec);
        }
        ec = detail::close_socket(m_handle);
        if (ec)
        {
            detail::log_error("tcp::socket::close", ec);
        }
        m_handle = detail::kInvalidHandle;
    }

    // Connect to a remote endpoint, opening the socket first if needed
    // Parameter peer:    address to connect to
    // Parameter handler: called as handler(std::error_code)
    template <typename Handler>
    void async_connect(const endpoint& peer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open())
        {
            const std::error_code ec = open(peer.family());
            if (ec)
            {
                m_context->post(detail::error_binder<handler_type>(std::forward<Handler>(handler), ec));
                return;
            }
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::connect_completion<handler_type>(std::forward<Handler>(handler)));
        detail::prepare_connect(*op, m_handle, peer.data(), peer.size());
        m_context->start_io(op);
    }

    // Read whatever is available, at most buffer.size() bytes
    // Parameter buffer:  destination; valid until the handler runs
    // Parameter handler: called as handler(std::error_code, bytes read);
    //                    spanio::error::eof when the peer closed
    template <typename Handler>
    void async_read_some(const mutable_buffer& buffer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open() || buffer.size() == 0)
        {
            post_transfer(std::forward<Handler>(handler));
            return;
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::transfer_completion<handler_type>(std::forward<Handler>(handler), true));
        detail::prepare_recv(*op, m_handle, static_cast<char*>(buffer.data()), buffer.size());
        m_context->start_io(op);
    }

    // Send part or all of a buffer (one send call)
    // Parameter buffer:  source; valid until the handler runs
    // Parameter handler: called as handler(std::error_code, bytes sent)
    template <typename Handler>
    void async_write_some(const const_buffer& buffer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open() || buffer.size() == 0)
        {
            post_transfer(std::forward<Handler>(handler));
            return;
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::transfer_completion<handler_type>(std::forward<Handler>(handler), false));
        detail::prepare_send(*op, m_handle, static_cast<const char*>(buffer.data()), buffer.size());
        m_context->start_io(op);
    }

    // Send the whole buffer, in as many sends as needed
    // Parameter buffer:  source; valid until the handler runs
    // Parameter handler: called once as handler(std::error_code, bytes sent);
    //                    bytes sent equals buffer.size() on success
    template <typename Handler>
    void async_write(const const_buffer& buffer, Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open() || buffer.size() == 0)
        {
            post_transfer(std::forward<Handler>(handler));
            return;
        }
        detail::write_all_op<handler_type> op(*m_context, m_handle,
                                              static_cast<const char*>(buffer.data()),
                                              buffer.size(), std::forward<Handler>(handler));
        op.start();
    }

private:
    // Report a transfer that needs no I/O through the context: a closed
    // socket fails with bad_file_descriptor, an empty buffer succeeds with
    // 0 bytes
    // Parameter handler: user handler
    template <typename Handler>
    void post_transfer(Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        const std::error_code ec = is_open() ? std::error_code() : detail::not_open_error();
        m_context->post(detail::transfer_binder<handler_type>(std::forward<Handler>(handler), ec, 0));
    }

    // Context that runs the handlers; never null
    io_context* m_context;
    // Native socket, or kInvalidHandle when not open
    detail::native_handle_t m_handle;
};

}}

namespace spanio { namespace detail {

// Task that reports a failed accept: the handler gets the error and a
// socket that is not open
template <typename Handler>
class accept_error_binder
{
public:
    // Parameter context: context the new (empty) socket belongs to
    // Parameter handler: function object taking (std::error_code, tcp::socket)
    // Parameter ec:      error to pass
    template <typename H>
    accept_error_binder(io_context& context, H&& handler, const std::error_code& ec)
        : m_context(&context)
        , m_handler(std::forward<H>(handler))
        , m_ec(ec)
    {
    }

    // Call the handler with the error and an empty socket
    void operator()()
    {
        m_handler(m_ec, tcp::socket(*m_context));
    }

private:
    // Context for the empty socket
    io_context* m_context;
    // User handler
    Handler m_handler;
    // Error to pass
    std::error_code m_ec;
};

// Completion of an accept: wraps the accepted handle in a tcp::socket
// registered with the context and hands it to the user
template <typename Handler>
class accept_completion
{
public:
    // Parameter context: context the accepted socket will belong to
    // Parameter handler: function object taking (std::error_code, tcp::socket)
    template <typename H>
    accept_completion(io_context& context, H&& handler)
        : m_context(&context)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Parameter result: the finished accept
    void operator()(const io_result& result)
    {
        tcp::socket peer(*m_context);
        std::error_code ec = result.m_ec;
        if (!ec)
        {
            ec = peer.assign(result.m_accepted);
            if (ec)
            {
                // Nobody owns the handle if it could not be wrapped
                close_socket(result.m_accepted);
            }
        }
        m_handler(ec, std::move(peer));
    }

private:
    // Context the accepted socket belongs to
    io_context* m_context;
    // User handler
    Handler m_handler;
};

}}

namespace spanio { namespace tcp {

// A listening TCP socket. Movable, not copyable. Destroying or closing it
// cancels a pending accept; its handler still runs once.
//
// Address reuse policy, applied by open():
// - Linux:   SO_REUSEADDR, so a restarted server can bind while old
//            connections are in TIME_WAIT
// - Windows: SO_EXCLUSIVEADDRUSE. Windows' SO_REUSEADDR would let another
//            socket bind the same port while this one listens.
class acceptor
{
public:
    // Create an acceptor that is not open yet
    // Parameter context: context that runs its handlers; must outlive it
    explicit acceptor(io_context& context) noexcept
        : m_context(&context)
        , m_handle(detail::kInvalidHandle)
    {
    }

    // Open, bind and listen in one step; throws std::system_error on failure
    // Parameter context: context that runs its handlers; must outlive it
    // Parameter local:   address and port to listen on; port 0 picks one
    acceptor(io_context& context, const endpoint& local)
        : acceptor(context)
    {
        std::error_code ec = open(local.family());
        if (!ec)
        {
            ec = bind(local);
        }
        if (!ec)
        {
            ec = listen();
        }
        if (ec)
        {
            close();
            throw std::system_error(ec, "spanio::tcp::acceptor");
        }
    }

    // Take over another acceptor's socket
    // Parameter other: acceptor to move from; it is not open afterwards
    acceptor(acceptor&& other) noexcept
        : m_context(other.m_context)
        , m_handle(other.m_handle)
    {
        other.m_handle = detail::kInvalidHandle;
    }

    // Close this acceptor, then take over another one's socket
    // Parameter other: acceptor to move from; it is not open afterwards
    // Return value: *this
    acceptor& operator=(acceptor&& other) noexcept
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

    acceptor(const acceptor&) = delete;
    acceptor& operator=(const acceptor&) = delete;

    // Close the acceptor, cancelling a pending accept
    ~acceptor()
    {
        close();
    }

    // Create the native socket, apply the address reuse policy and register
    // it with the context
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
        const detail::native_handle_t handle = detail::create_socket(family, SOCK_STREAM, IPPROTO_TCP, ec);
        if (ec)
        {
            detail::log_error("tcp::acceptor::open", ec);
            return ec;
        }
#if defined(SPANIO_PLATFORM_WINDOWS)
        const int reuse_option = SO_EXCLUSIVEADDRUSE;
#else
        const int reuse_option = SO_REUSEADDR;
#endif
        const int on = kSocketOptionOn;
        if (::setsockopt(handle, SOL_SOCKET, reuse_option, reinterpret_cast<const char*>(&on),
                         static_cast<detail::socklen_type>(sizeof(on))) == detail::kSocketCallFailed)
        {
            ec = detail::last_error();
            detail::log_error("tcp::acceptor::open: address reuse option", ec);
            detail::close_socket(handle);
            return ec;
        }
        ec = m_context->register_handle(handle);
        if (ec)
        {
            detail::log_error("tcp::acceptor::open: register_handle", ec);
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
            detail::log_error("tcp::acceptor::bind", ec);
            return ec;
        }
        return std::error_code();
    }

    // Start listening for connections
    // Parameter backlog: maximum queue of pending connections
    // Return value: empty on success, otherwise the system error
    std::error_code listen(int backlog = kDefaultBacklog)
    {
        if (!is_open())
        {
            return detail::not_open_error();
        }
        if (::listen(m_handle, backlog) == detail::kSocketCallFailed)
        {
            const std::error_code ec = detail::last_error();
            detail::log_error("tcp::acceptor::listen", ec);
            return ec;
        }
        return std::error_code();
    }

    // Whether the acceptor is open
    // Return value: true when open
    bool is_open() const noexcept
    {
        return m_handle != detail::kInvalidHandle;
    }

    // The native handle
    // Return value: the handle, or kInvalidHandle when not open
    detail::native_handle_t native_handle() const noexcept
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

    // Cancel a pending accept; its handler gets errc::operation_canceled
    void cancel()
    {
        if (is_open())
        {
            m_context->cancel_handle(m_handle);
        }
    }

    // Unregister and close the socket. A pending accept completes with
    // errc::operation_canceled. Does nothing when not open.
    void close()
    {
        if (!is_open())
        {
            return;
        }
        std::error_code ec = m_context->unregister_handle(m_handle);
        if (ec)
        {
            detail::log_error("tcp::acceptor::close: unregister_handle", ec);
        }
        ec = detail::close_socket(m_handle);
        if (ec)
        {
            detail::log_error("tcp::acceptor::close", ec);
        }
        m_handle = detail::kInvalidHandle;
    }

    // Accept one connection
    // Parameter handler: called as handler(std::error_code, tcp::socket);
    //                    the socket is open on success and belongs to the
    //                    same context as the acceptor
    template <typename Handler>
    void async_accept(Handler&& handler)
    {
        using handler_type = typename std::decay<Handler>::type;
        if (!is_open())
        {
            m_context->post(detail::accept_error_binder<handler_type>(
                *m_context, std::forward<Handler>(handler), detail::not_open_error()));
            return;
        }
        detail::io_op_base* op = detail::make_io_op(
            detail::accept_completion<handler_type>(*m_context, std::forward<Handler>(handler)));
        detail::prepare_accept(*op, m_handle);
        m_context->start_io(op);
    }

private:
    // Context that runs the handlers; never null
    io_context* m_context;
    // Native listening socket, or kInvalidHandle when not open
    detail::native_handle_t m_handle;
};

}}

#endif // SPANIO_TCP_H
