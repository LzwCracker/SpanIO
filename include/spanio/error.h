// ============================================================================
// File purpose: SpanIO's own error codes that have no std::errc equivalent,
//               plugged into the std::error_code machinery. System errors
//               keep their std::system_category codes; cancellation is always
//               std::errc::operation_canceled (see io_proactor.h).
//               - misc_errors (category "spanio.misc"): eof and the
//                 tcp_server / udp::socket conditions
//               - netdb_errors (category "spanio.netdb"): name resolution
//                 failures reported by resolver.h
// Usage:        sock.async_read_some(buf, handler);
//               ...
//               if (ec == spanio::error::eof)
//               {
//                   // the peer closed the connection in an orderly way
//               }
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_ERROR_H
#define SPANIO_ERROR_H

#include <string>
#include <system_error>
#include <type_traits>

namespace spanio { namespace error {

// Errors reported by SpanIO itself rather than by the operating system.
// Values start at 1 because 0 means "no error" in std::error_code.
enum misc_errors
{
    // The peer closed the connection in an orderly way: a read completed
    // with 0 bytes although the buffer was not empty
    eof = 1,
    // tcp_server: a message, or the input buffered while waiting for one,
    // exceeded tcp_server_options::m_max_message_size
    message_too_large,
    // tcp_server: queued output exceeded
    // tcp_server_options::m_max_send_queue_bytes
    send_queue_full,
    // tcp_server: the framer reported malformed input
    framing_error,
    // tcp_server: a user callback threw an exception
    handler_failed,
    // udp::socket: a datagram was larger than the receive buffer; the part
    // that fit was delivered and the rest is lost
    datagram_truncated,
    // tcp_server: a connection had no activity (no data received and no
    // send progress) for tcp_server_options::m_idle_timeout
    idle_timeout,
    // tcp_server: queued data could not be sent within the close limit
    // (tcp_server_options::m_close_timeout, or the grace period of
    // stop_gracefully), so the connection was closed without it
    close_timeout
};

// Category of misc_errors
class misc_category_impl : public std::error_category
{
public:
    // Name of the category
    // Return value: static string "spanio.misc"
    const char* name() const noexcept override
    {
        return "spanio.misc";
    }

    // Human-readable description of an error value
    // Parameter value: a misc_errors value
    // Return value: description text
    std::string message(int value) const override
    {
        switch (value)
        {
        case eof:
            return "End of file";
        case message_too_large:
            return "Message too large";
        case send_queue_full:
            return "Send queue full";
        case framing_error:
            return "Malformed message framing";
        case datagram_truncated:
            return "Datagram truncated";
        case idle_timeout:
            return "Connection idle for too long";
        case close_timeout:
            return "Close timed out before all data was sent";
        case handler_failed:
            return "User callback failed";
        default:
            return "spanio.misc error";
        }
    }
};

// The single instance of the misc category
// Return value: reference to the category, valid for the whole program
inline const std::error_category& misc_category() noexcept
{
    static const misc_category_impl s_category;
    return s_category;
}

// Make an error_code from a misc_errors value; found by argument-dependent
// lookup, so `std::error_code ec = spanio::error::eof;` works
// Parameter value: the error
// Return value: the error code in the misc category
inline std::error_code make_error_code(misc_errors value) noexcept
{
    return std::error_code(static_cast<int>(value), misc_category());
}

// Name resolution errors (resolver.h), the same on every platform. The
// getaddrinfo() codes of each system are mapped onto these.
enum netdb_errors
{
    // The host name is not known
    host_not_found = 1,
    // The host name could not be resolved now (for example no DNS server
    // answered); trying again later may work
    host_not_found_try_again,
    // The host name is valid but has no address of the requested family
    no_data,
    // A non-recoverable error occurred during resolution
    no_recovery,
    // The service name is not known for this socket type
    service_not_found
};

// Category of netdb_errors
class netdb_category_impl : public std::error_category
{
public:
    // Name of the category
    // Return value: static string "spanio.netdb"
    const char* name() const noexcept override
    {
        return "spanio.netdb";
    }

    // Human-readable description of an error value
    // Parameter value: a netdb_errors value
    // Return value: description text
    std::string message(int value) const override
    {
        switch (value)
        {
        case host_not_found:
            return "Host not found";
        case host_not_found_try_again:
            return "Host not found, try again later";
        case no_data:
            return "Host has no address of the requested type";
        case no_recovery:
            return "Non-recoverable name resolution failure";
        case service_not_found:
            return "Service not found";
        default:
            return "spanio.netdb error";
        }
    }
};

// The single instance of the netdb category
// Return value: reference to the category, valid for the whole program
inline const std::error_category& netdb_category() noexcept
{
    static const netdb_category_impl s_category;
    return s_category;
}

// Make an error_code from a netdb_errors value
// Parameter value: the error
// Return value: the error code in the netdb category
inline std::error_code make_error_code(netdb_errors value) noexcept
{
    return std::error_code(static_cast<int>(value), netdb_category());
}

}}

namespace std {

// Lets misc_errors convert implicitly to std::error_code
template <>
struct is_error_code_enum<spanio::error::misc_errors> : true_type
{
};

// Lets netdb_errors convert implicitly to std::error_code
template <>
struct is_error_code_enum<spanio::error::netdb_errors> : true_type
{
};

}

#endif // SPANIO_ERROR_H
