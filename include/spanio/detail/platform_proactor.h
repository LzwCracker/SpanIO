// ============================================================================
// File purpose: Selects the default io_proactor for the platform being
//               compiled for:
//               - Windows: native iocp_proactor, unless
//                 SPANIO_WINDOWS_USE_AFD_REACTOR is defined, in which case
//                 reactor_proactor over the IOCP + AFD poll reactor is used
//               - Linux:   io_uring_proactor when SPANIO_ENABLE_IO_URING is
//                 defined and the kernel supports it, otherwise (or when
//                 io_uring cannot be set up at run time) reactor_proactor
//                 over epoll
// Usage:        std::error_code ec;
//               spanio::detail::io_proactor_ptr proactor =
//                   spanio::detail::make_platform_proactor(ec);
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04 (io_uring selection added 2026-11-18)
// ============================================================================

#ifndef SPANIO_DETAIL_PLATFORM_PROACTOR_H
#define SPANIO_DETAIL_PLATFORM_PROACTOR_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/io_uring_proactor.h"
#include "spanio/detail/platform_reactor.h"
#include "spanio/detail/reactor_proactor.h"

#if defined(SPANIO_PLATFORM_WINDOWS)
#include "spanio/detail/iocp_proactor.h"
#endif

namespace spanio { namespace detail {

// Create the readiness-emulation proactor over this platform's reactor
// Parameter ec: set to the failure reason, or cleared on success
// Return value: the new proactor, or nullptr on failure
inline io_proactor_ptr make_reactor_proactor(std::error_code& ec)
{
    io_reactor_ptr reactor = make_platform_reactor(ec);
    if (ec)
    {
        return nullptr;
    }
    return reactor_proactor::create(std::move(reactor), ec);
}

// Function that creates a proactor, e.g. make_io_uring_proactor
using proactor_factory = io_proactor_ptr (*)(std::error_code&);

// Create a proactor from a preferred factory, falling back to a second one
// when the first fails (run-time degradation, design doc risk table)
// Parameter preferred: factory tried first
// Parameter fallback:  factory used when preferred fails
// Parameter ec:        set to the fallback's failure reason, or cleared
// Return value: the new proactor, or nullptr if both failed
inline io_proactor_ptr make_preferred_proactor(proactor_factory preferred, proactor_factory fallback,
                                               std::error_code& ec)
{
    io_proactor_ptr proactor = preferred(ec);
    if (!ec && proactor != nullptr)
    {
        return proactor;
    }
    log_error("make_preferred_proactor: preferred backend unavailable, falling back", ec);
    return fallback(ec);
}

// Create the default proactor for this platform (see the file header)
// Parameter ec: set to the failure reason, or cleared on success
// Return value: the new proactor, or nullptr on failure
inline io_proactor_ptr make_platform_proactor(std::error_code& ec)
{
#if defined(SPANIO_PLATFORM_WINDOWS) && !defined(SPANIO_WINDOWS_USE_AFD_REACTOR)
    return iocp_proactor::create(ec);
#elif defined(SPANIO_HAS_IO_URING)
    return make_preferred_proactor(&make_io_uring_proactor, &make_reactor_proactor, ec);
#else
    return make_reactor_proactor(ec);
#endif
}

}}

#endif // SPANIO_DETAIL_PLATFORM_PROACTOR_H
