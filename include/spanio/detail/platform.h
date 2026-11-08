// ============================================================================
// File purpose: Platform detection, system header inclusion and platform type
//               aliases for SpanIO. This is the lowest layer of the library;
//               every other SpanIO header includes it, so platform-specific
//               #if branches should be confined here and in other detail/
//               headers instead of leaking into the public API.
// Usage:        Include indirectly through other SpanIO headers. On Windows,
//               do not include <windows.h> before this header unless
//               WIN32_LEAN_AND_MEAN is defined, otherwise the legacy
//               <winsock.h> conflicts with <winsock2.h>.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_PLATFORM_H
#define SPANIO_DETAIL_PLATFORM_H

#include <cstdio>
#include <system_error>

// ----------------------------------------------------------------------------
// Platform detection. Exactly one SPANIO_PLATFORM_* macro is defined; any
// other target is rejected at compile time so missing support is obvious.
// ----------------------------------------------------------------------------
#if defined(_WIN32)
// Defined when building for Windows (MSVC or MinGW, 32 or 64 bit)
#define SPANIO_PLATFORM_WINDOWS 1
#elif defined(__linux__)
// Defined when building for Linux (glibc or musl)
#define SPANIO_PLATFORM_LINUX 1
#else
#error "SpanIO only supports Windows and Linux"
#endif

// ----------------------------------------------------------------------------
// System headers
// ----------------------------------------------------------------------------
#if defined(SPANIO_PLATFORM_WINDOWS)

// Keep <windows.h> from pulling in the legacy <winsock.h>, which conflicts
// with <winsock2.h>, and trim rarely used APIs to speed up compilation
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

// Stop <windows.h> from defining min/max macros that break std::min/std::max
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>

// With MSVC, link the Winsock library automatically so header-only users do
// not have to add ws2_32.lib by hand. MinGW users must pass -lws2_32.
#if defined(_MSC_VER)
#pragma comment(lib, "ws2_32.lib")
#endif

#else // SPANIO_PLATFORM_LINUX

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
// getaddrinfo() and its EAI_ codes, for resolver.h
#include <netdb.h>

#endif

namespace spanio { namespace detail {

// ----------------------------------------------------------------------------
// Platform type aliases and constants
// ----------------------------------------------------------------------------
#if defined(SPANIO_PLATFORM_WINDOWS)

// Native socket handle type: SOCKET (an unsigned pointer-sized integer)
using native_handle_t = SOCKET;

// Length type that the socket address APIs (bind, accept, getsockname...) expect
using socklen_type = int;

// Value of a handle that does not refer to an open socket
constexpr native_handle_t kInvalidHandle = INVALID_SOCKET;

// Return value of socket API calls (bind, listen, ioctlsocket...) on failure
constexpr int kSocketCallFailed = SOCKET_ERROR;

#else // SPANIO_PLATFORM_LINUX

// Native socket handle type: a plain file descriptor
using native_handle_t = int;

// Length type that the socket address APIs (bind, accept, getsockname...) expect
using socklen_type = socklen_t;

// Value of a handle that does not refer to an open socket
constexpr native_handle_t kInvalidHandle = -1;

// Return value of socket API calls (and fcntl/close) on failure; the
// detailed reason is in errno
constexpr int kSocketCallFailed = -1;

#endif

// ----------------------------------------------------------------------------
// Minimal logging hook for critical paths. It compiles to nothing unless
// SPANIO_ENABLE_LOG is defined, so the default build has no logging cost.
// It is meant to be replaced by the built-in logger once that module exists.
// ----------------------------------------------------------------------------

// Report a failed operation on a critical path
// Parameter what: short static description of the operation that failed,
//                 e.g. "WSAStartup" or "set_nonblocking"
// Parameter ec:   error code describing why the operation failed
inline void log_error(const char* what, const std::error_code& ec) noexcept
{
#if defined(SPANIO_ENABLE_LOG)
    std::fprintf(stderr, "[spanio][error] %s failed: %d (%s)\n",
                 what, ec.value(), ec.message().c_str());
#else
    (void)what;
    (void)ec;
#endif
}

}}

#endif // SPANIO_DETAIL_PLATFORM_H
