// ============================================================================
// File purpose: Platform-neutral readiness notification interface (Reactor).
//               Backends (epoll on Linux, IOCP + AFD poll on Windows) report
//               when a socket becomes readable or writable; the execution
//               engine builds Proactor-style completions on top of it.
// Usage:        Create the backend for the current platform with
//               make_platform_reactor() (platform_reactor.h), register sockets
//               with add(), change interest with mod(), unregister with del()
//               BEFORE closing the socket, and call poll() in the event loop.
//
//               Contract shared by every backend:
//               - Level-triggered: as long as a socket stays ready and its
//                 interest includes that event, every poll() reports it again.
//               - kEventError and kEventHangup are always reported and never
//                 need to be requested, like epoll.
//               - add/mod/del/interrupt may be called from any thread, even
//                 while another thread is blocked in poll(). Changes take
//                 effect for the poll() in progress.
//               - Only one thread may call poll() at a time.
//               - poll() may return 0 events before the timeout expires
//                 (interrupt(), or a signal on Linux); callers simply loop.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_IO_REACTOR_H
#define SPANIO_DETAIL_IO_REACTOR_H

#include "spanio/detail/platform.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>
#include <vector>

namespace spanio { namespace detail {

// Handle type a reactor watches; always a socket
using descriptor_t = native_handle_t;

// Bit set of kEvent* values, used both for interest and for reported events
using event_mask = std::uint32_t;

// Readiness event bits
enum event_bits : event_mask
{
    // No event. As an interest it means "only report errors and hang-ups".
    kEventNone = 0,
    // Readable: data is waiting, a listening socket has a pending connection,
    // or the peer closed its side (recv will return 0)
    kEventRead = 1u << 0,
    // Writable: the send buffer has room, or a non-blocking connect finished
    // (check SO_ERROR to learn whether it succeeded)
    kEventWrite = 1u << 1,
    // An error is pending on the socket, e.g. a failed connect. Always
    // reported; not allowed in an interest mask.
    kEventError = 1u << 2,
    // The connection was aborted or both directions are closed. Always
    // reported; not allowed in an interest mask.
    kEventHangup = 1u << 3
};

// Bits that callers may put into an interest mask
constexpr event_mask kInterestMask = kEventRead | kEventWrite;

// Bits that every backend reports without being asked
constexpr event_mask kAlwaysReported = kEventError | kEventHangup;

// poll() timeout value meaning "wait until an event or interrupt()"
constexpr int kInfiniteTimeout = -1;

// Upper bound of events collected by a single poll() call; more ready
// sockets are reported by the following calls
constexpr std::size_t kMaxEventsPerPoll = 128;

// One ready socket reported by poll()
struct ready_event
{
    // Socket that became ready
    descriptor_t m_handle;
    // Events that occurred, already filtered by the socket's interest
    // (plus kAlwaysReported bits)
    event_mask m_events;
};

// Output buffer of poll(); reused between calls to avoid reallocation
using event_table = std::vector<ready_event>;

// Check that an interest mask only holds bits callers are allowed to request
// Parameter interest: mask passed to add() or mod()
// Return value: true if the mask is a subset of kInterestMask
inline bool is_valid_interest(event_mask interest) noexcept
{
    return (interest & ~kInterestMask) == 0;
}

// Abstract readiness notification backend. See the file header for the
// threading and triggering contract all implementations follow.
class io_reactor
{
public:
    virtual ~io_reactor() = default;

    // Start watching a socket
    // Parameter fd:       socket to watch; must not be registered already
    // Parameter interest: events to report, a combination of kEventRead and
    //                     kEventWrite (kEventNone is allowed)
    // Return value: empty on success; errc::file_exists if fd is already
    //               registered; errc::invalid_argument for a bad interest;
    //               otherwise the system error
    virtual std::error_code add(descriptor_t fd, event_mask interest) = 0;

    // Replace the interest of a registered socket
    // Parameter fd:       a socket previously passed to add()
    // Parameter interest: new interest, same rules as add()
    // Return value: empty on success; errc::no_such_file_or_directory if fd
    //               is not registered; errc::invalid_argument for a bad
    //               interest; otherwise the system error
    virtual std::error_code mod(descriptor_t fd, event_mask interest) = 0;

    // Stop watching a socket. Must be called before the socket is closed.
    // Parameter fd: a socket previously passed to add()
    // Return value: empty on success; errc::no_such_file_or_directory if fd
    //               is not registered; otherwise the system error
    virtual std::error_code del(descriptor_t fd) = 0;

    // Wait for ready sockets
    // Parameter out:        cleared, then filled with the ready sockets
    // Parameter timeout_ms: maximum wait in milliseconds; 0 returns at once,
    //                       kInfiniteTimeout waits without limit
    // Parameter ec:         set to the failure reason, or cleared
    // Return value: number of entries written to out
    virtual std::size_t poll(event_table& out, int timeout_ms, std::error_code& ec) = 0;

    // Wake up a poll() that is blocked in another thread. Safe to call from
    // any thread; several calls before the wake-up collapse into one.
    virtual void interrupt() noexcept = 0;

    // Short backend name for logs and tests, e.g. "epoll"
    // Return value: static string, never null
    virtual const char* name() const noexcept = 0;
};

// Owning pointer to a backend of any type
using io_reactor_ptr = std::unique_ptr<io_reactor>;

}}

#endif // SPANIO_DETAIL_IO_REACTOR_H
