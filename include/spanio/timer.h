// ============================================================================
// File purpose: steady_timer, a timer on the monotonic clock whose waits
//               complete through io_context.
// Usage:        spanio::io_context ctx;
//               spanio::steady_timer timer(ctx, std::chrono::seconds(1));
//               auto on_timeout = [](std::error_code ec)
//               {
//                   if (!ec)
//                   {
//                       std::puts("1 second elapsed");
//                   }
//               };
//               timer.async_wait(on_timeout);
//               ctx.run();
//
//               Rules:
//               - A wait completes with an empty error when the timer expires
//                 and with errc::operation_canceled when it is cancelled.
//                 Each wait runs its handler exactly once, and never inside
//                 the call that started it, even if the timer already expired.
//               - A timer never fires early. It may fire late if the system
//                 is busy.
//               - A timer may have several waits; all of them complete
//                 together.
//               - A pending wait counts as work: run() does not return before
//                 it completes.
//               - cancel() can be too late. Once a timer expired its handler
//                 is on its way: cancel() returns 0 and the handler sees
//                 success. Check the handler's error code or cancel()'s
//                 result if that matters.
//               - Destroying a timer cancels its waits. The io_context must
//                 outlive every timer.
//               - stop() leaves timers pending; they continue after restart().
//               - Different timers may be used on different threads, but one
//                 timer object must not be used by two threads at once.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#ifndef SPANIO_TIMER_H
#define SPANIO_TIMER_H

#include "spanio/io_context.h"
#include "spanio/detail/timer_queue.h"

#include <chrono>
#include <cstddef>
#include <utility>

namespace spanio {

// A timer on the monotonic clock. See the file header for the rules. It
// cannot be copied or moved, because its pending waits refer to it; keep it
// in a std::unique_ptr to put it in a container.
class steady_timer
{
public:
    // Clock the timer uses
    using clock = detail::timer_clock;
    // Length of a wait
    using duration = clock::duration;
    // A point in time on the clock
    using time_point = detail::timer_time_point;

    // Create a timer that has already expired
    // Parameter context: context that runs its handlers; must outlive it
    explicit steady_timer(io_context& context)
        : m_context(&context)
        , m_expiry(clock::now())
    {
    }

    // Create a timer that expires after a delay
    // Parameter context: context that runs its handlers; must outlive it
    // Parameter delay:   time from now
    steady_timer(io_context& context, duration delay)
        : m_context(&context)
        , m_expiry(clock::now() + delay)
    {
    }

    // Create a timer that expires at a point in time
    // Parameter context: context that runs its handlers; must outlive it
    // Parameter when:    expiry time
    steady_timer(io_context& context, time_point when)
        : m_context(&context)
        , m_expiry(when)
    {
    }

    steady_timer(const steady_timer&) = delete;
    steady_timer& operator=(const steady_timer&) = delete;

    // Cancel pending waits
    ~steady_timer()
    {
        cancel();
    }

    // Set a new expiry relative to now, cancelling pending waits
    // Parameter delay: time from now
    // Return value: number of waits cancelled
    std::size_t expires_after(duration delay)
    {
        return expires_at(clock::now() + delay);
    }

    // Set a new expiry time, cancelling pending waits
    // Parameter when: expiry time
    // Return value: number of waits cancelled
    std::size_t expires_at(time_point when)
    {
        const std::size_t cancelled = cancel();
        m_expiry = when;
        return cancelled;
    }

    // The current expiry time
    // Return value: the time
    time_point expiry() const noexcept
    {
        return m_expiry;
    }

    // Wait for the timer to expire
    // Parameter handler: called as handler(std::error_code)
    template <typename Handler>
    void async_wait(Handler&& handler)
    {
        detail::context_access::schedule_timer(*m_context, m_state, m_expiry, detail::make_wait_op(std::forward<Handler>(handler)));
    }

    // Cancel pending waits; their handlers get errc::operation_canceled
    // Return value: number of waits cancelled; 0 if none is pending, which
    //               includes waits whose timer already expired
    std::size_t cancel()
    {
        return detail::context_access::cancel_timer(*m_context, m_state);
    }

private:
    // Context that runs the handlers; never null
    io_context* m_context;
    // When the timer expires
    time_point m_expiry;
    // Pending waits; guarded by the context
    detail::timer_state m_state;
};

}

#endif // SPANIO_TIMER_H
