// ============================================================================
// File purpose: SpanIO's own error codes that have no std::errc equivalent,
//               plugged into the std::error_code machinery. System errors
//               keep their std::system_category codes; cancellation is always
//               std::errc::operation_canceled (see io_proactor.h).
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
    handler_failed
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

}}

namespace std {

// Lets misc_errors convert implicitly to std::error_code
template <>
struct is_error_code_enum<spanio::error::misc_errors> : true_type
{
};

}

#endif // SPANIO_ERROR_H
