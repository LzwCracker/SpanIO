// ============================================================================
// File purpose: Thin, platform-neutral wrappers over the socket lifecycle
//               calls that differ between POSIX and Winsock: library start-up,
//               socket creation, closing, non-blocking mode and error codes.
//               Every function reports failures as std::error_code, so upper
//               layers never touch errno or WSAGetLastError() directly.
// Usage:        Internal header for the reactors and the tcp/udp classes.
//               Call create_socket() to open a socket (it initializes Winsock
//               on first use), then set_nonblocking() and close_socket().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_SOCKET_OPS_H
#define SPANIO_DETAIL_SOCKET_OPS_H

#include "spanio/detail/platform.h"

#include <system_error>

namespace spanio { namespace detail {

// ----------------------------------------------------------------------------
// Error codes
// ----------------------------------------------------------------------------

// Turn a system error into the form every SpanIO caller sees, so the same
// situation compares equal to the same std::errc on every platform:
// - cancellation (WSA_OPERATION_ABORTED / ECANCELED) -> operation_canceled
// - Windows codes the C++ library does not map to any errc:
//     WSAESHUTDOWN (write after a local shutdown)    -> broken_pipe, as EPIPE
//     WSAENOTSOCK (handle already closed)            -> bad_file_descriptor
//     ERROR_NETNAME_DELETED (raw IOCP peer reset)    -> connection_reset
//     ERROR_CONNECTION_REFUSED (raw IOCP refusal)    -> connection_refused
// Linux errno values already compare equal to std::errc and pass through.
// Codes of other categories (spanio.misc, spanio.netdb, generic) are never
// touched.
// Parameter ec: error to translate
// Return value: the translated error, or ec unchanged
inline std::error_code normalize_error(const std::error_code& ec) noexcept
{
    if (!ec || ec.category() != std::system_category())
    {
        return ec;
    }
#if defined(SPANIO_PLATFORM_WINDOWS)
    switch (ec.value())
    {
    // WSA_OPERATION_ABORTED and ERROR_OPERATION_ABORTED share one value
    case WSA_OPERATION_ABORTED:
        return std::make_error_code(std::errc::operation_canceled);
    case WSAESHUTDOWN:
        return std::make_error_code(std::errc::broken_pipe);
    case WSAENOTSOCK:
        return std::make_error_code(std::errc::bad_file_descriptor);
    case ERROR_NETNAME_DELETED:
        return std::make_error_code(std::errc::connection_reset);
    case ERROR_CONNECTION_REFUSED:
        return std::make_error_code(std::errc::connection_refused);
    default:
        return ec;
    }
#else
    if (ec.value() == ECANCELED)
    {
        return std::make_error_code(std::errc::operation_canceled);
    }
    return ec;
#endif
}

// Fetch the error left by the most recent failed socket call on this thread
// Return value: the Winsock error on Windows or errno on Linux, wrapped in
//               std::system_category and passed through normalize_error()
inline std::error_code last_error() noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    return normalize_error(std::error_code(::WSAGetLastError(), std::system_category()));
#else
    return normalize_error(std::error_code(errno, std::system_category()));
#endif
}

// ----------------------------------------------------------------------------
// Winsock start-up
// ----------------------------------------------------------------------------
#if defined(SPANIO_PLATFORM_WINDOWS)

// Major part of the Winsock version requested from WSAStartup (2.2)
constexpr unsigned char kWinsockMajorVersion = 2;

// Minor part of the Winsock version requested from WSAStartup (2.2)
constexpr unsigned char kWinsockMinorVersion = 2;

// Calls WSAStartup when constructed and WSACleanup when destroyed. A single
// process-wide instance lives in ensure_network_initialized(), so users never
// manage Winsock themselves and threads cannot initialize it twice.
class winsock_init
{
public:
    // Start Winsock 2.2 and remember whether it worked
    winsock_init() noexcept
    {
        WSADATA data;
        // WSAStartup returns its error directly instead of through
        // WSAGetLastError(), because the error machinery is not up yet
        const int result = ::WSAStartup(
            MAKEWORD(kWinsockMajorVersion, kWinsockMinorVersion), &data);
        m_result = std::error_code(result, std::system_category());
        if (m_result)
        {
            log_error("WSAStartup", m_result);
        }
    }

    // Release Winsock, but only if the matching WSAStartup succeeded
    ~winsock_init()
    {
        if (!m_result)
        {
            ::WSACleanup();
        }
    }

    winsock_init(const winsock_init&) = delete;
    winsock_init& operator=(const winsock_init&) = delete;

    // Result of the WSAStartup call made by the constructor
    // Return value: empty error code on success, the Winsock error otherwise
    const std::error_code& result() const noexcept
    {
        return m_result;
    }

private:
    // Outcome of WSAStartup; empty means Winsock is usable and must be
    // released with WSACleanup in the destructor
    std::error_code m_result;
};

#endif

// Make sure the platform socket library is ready. This is idempotent and
// thread-safe; calls after the first one only read the cached result.
// Return value: empty error code if sockets can be used, otherwise the error
//               from WSAStartup. Always empty on Linux.
inline std::error_code ensure_network_initialized() noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // Function-local static: C++11 guarantees exactly one thread-safe
    // construction, and an inline function shares it across all translation
    // units. It is destroyed (WSACleanup) during static destruction at exit.
    static winsock_init s_init;
    return s_init.result();
#else
    return std::error_code();
#endif
}

// ----------------------------------------------------------------------------
// Socket lifecycle
// ----------------------------------------------------------------------------

// Open a new socket. The handle is not inherited by child processes, and on
// Windows it is created in overlapped mode so it can be bound to an IOCP.
// Parameter family:   address family, e.g. AF_INET or AF_INET6
// Parameter type:     socket type, e.g. SOCK_STREAM or SOCK_DGRAM
// Parameter protocol: protocol, e.g. IPPROTO_TCP; 0 picks the default
// Parameter ec:       set to the failure reason, or cleared on success
// Return value: the new handle, or kInvalidHandle on failure
inline native_handle_t create_socket(int family, int type, int protocol,
                                     std::error_code& ec) noexcept
{
    ec = ensure_network_initialized();
    if (ec)
    {
        return kInvalidHandle;
    }

#if defined(SPANIO_PLATFORM_WINDOWS)
    // Overlapped I/O is required by IOCP. WSA_FLAG_NO_HANDLE_INHERIT is the
    // Winsock counterpart of SOCK_CLOEXEC (Windows 7 SP1 and later).
    DWORD flags = WSA_FLAG_OVERLAPPED;
#if defined(WSA_FLAG_NO_HANDLE_INHERIT)
    flags |= WSA_FLAG_NO_HANDLE_INHERIT;
#endif
    const native_handle_t handle =
        ::WSASocketW(family, type, protocol, nullptr, 0, flags);
#else
    // SOCK_CLOEXEC closes the descriptor atomically across exec(), avoiding
    // the race of a separate fcntl(FD_CLOEXEC) call in a threaded program
    const native_handle_t handle =
        ::socket(family, type | SOCK_CLOEXEC, protocol);
#endif

    if (handle == kInvalidHandle)
    {
        ec = last_error();
        log_error("create_socket", ec);
        return kInvalidHandle;
    }

    ec.clear();
    return handle;
}

// Close a socket handle. The handle must not be used afterwards, even if an
// error is returned, because the system releases it in every case.
// Parameter handle: socket to close; kInvalidHandle is rejected
// Return value: empty error code on success; bad_file_descriptor for
//               kInvalidHandle; otherwise the error reported by the system
inline std::error_code close_socket(native_handle_t handle) noexcept
{
    if (handle == kInvalidHandle)
    {
        return std::make_error_code(std::errc::bad_file_descriptor);
    }

#if defined(SPANIO_PLATFORM_WINDOWS)
    const int result = ::closesocket(handle);
#else
    const int result = ::close(handle);
#endif

    if (result == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
#if defined(SPANIO_PLATFORM_LINUX)
        // On Linux the descriptor is already released when close() fails
        // with EINTR. Retrying could close a descriptor that another thread
        // has just been given, so EINTR is treated as success.
        if (ec.value() == EINTR)
        {
            return std::error_code();
        }
#endif
        log_error("close_socket", ec);
        return ec;
    }

    return std::error_code();
}

// Switch a socket between blocking and non-blocking mode
// Parameter handle: an open socket
// Parameter enable: true for non-blocking mode, false for blocking mode
// Return value: empty error code on success, the system error otherwise
inline std::error_code set_nonblocking(native_handle_t handle, bool enable) noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // FIONBIO treats any non-zero value as "enable non-blocking mode"
    u_long mode = static_cast<u_long>(enable);
    if (::ioctlsocket(handle, FIONBIO, &mode) == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
        log_error("set_nonblocking", ec);
        return ec;
    }
#else
    const int flags = ::fcntl(handle, F_GETFL);
    if (flags == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
        log_error("set_nonblocking(F_GETFL)", ec);
        return ec;
    }

    const int new_flags = enable ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    // Skip the second system call when the mode is already the one requested
    if (new_flags != flags && ::fcntl(handle, F_SETFL, new_flags) == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
        log_error("set_nonblocking(F_SETFL)", ec);
        return ec;
    }
#endif

    return std::error_code();
}

#if defined(SPANIO_PLATFORM_WINDOWS)
#if !defined(SIO_UDP_CONNRESET)
// WSAIoctl code that controls whether ICMP "port unreachable" fails the next
// receive; declared by newer SDKs in mswsock.h, with this same value
#define SIO_UDP_CONNRESET _WSAIOW(IOC_VENDOR, 12)
#endif
#endif

// Value that turns a boolean option on in configure_datagram_socket
constexpr int kDatagramOptionOn = 1;

// Give a new datagram socket the same behaviour on every platform.
// On Windows:
// - SO_EXCLUSIVEADDRUSE, so no other socket can bind the same port and take
//   its datagrams
// - SIO_UDP_CONNRESET off, so a datagram sent to a closed port does not make
//   a later receive fail with WSAECONNRESET
// Linux needs nothing: without SO_REUSEADDR a port is already exclusive, and
// unconnected UDP sockets do not report ICMP errors.
// Parameter handle: datagram socket, not bound yet
// Return value: empty on success, otherwise the system error
inline std::error_code configure_datagram_socket(native_handle_t handle) noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    const int exclusive = kDatagramOptionOn;
    if (::setsockopt(handle, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&exclusive),
                     static_cast<socklen_type>(sizeof(exclusive))) == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
        log_error("configure_datagram_socket(SO_EXCLUSIVEADDRUSE)", ec);
        return ec;
    }
    BOOL report_connection_reset = FALSE;
    DWORD returned = 0;
    if (::WSAIoctl(handle, SIO_UDP_CONNRESET, &report_connection_reset,
                   static_cast<DWORD>(sizeof(report_connection_reset)), nullptr, 0, &returned, nullptr,
                   nullptr) == kSocketCallFailed)
    {
        const std::error_code ec = last_error();
        log_error("configure_datagram_socket(SIO_UDP_CONNRESET)", ec);
        return ec;
    }
#else
    static_cast<void>(handle);
#endif
    return std::error_code();
}

}}

#endif // SPANIO_DETAIL_SOCKET_OPS_H
