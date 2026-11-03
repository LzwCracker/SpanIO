// ============================================================================
// File purpose: Selects the default io_reactor backend for the platform being
//               compiled for: epoll on Linux, IOCP + AFD poll on Windows.
// Usage:        std::error_code ec;
//               spanio::detail::io_reactor_ptr reactor =
//                   spanio::detail::make_platform_reactor(ec);
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_PLATFORM_REACTOR_H
#define SPANIO_DETAIL_PLATFORM_REACTOR_H

#include "spanio/detail/io_reactor.h"

#if defined(SPANIO_PLATFORM_WINDOWS)
#include "spanio/detail/iocp_reactor.h"
#else
#include "spanio/detail/epoll_reactor.h"
#endif

namespace spanio { namespace detail {

// Create the default reactor backend for this platform
// Parameter ec: set to the failure reason, or cleared on success
// Return value: the new reactor, or nullptr on failure
inline io_reactor_ptr make_platform_reactor(std::error_code& ec)
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    return iocp_reactor::create(ec);
#else
    return epoll_reactor::create(ec);
#endif
}

}}

#endif // SPANIO_DETAIL_PLATFORM_REACTOR_H
