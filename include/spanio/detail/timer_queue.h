// ============================================================================
// File purpose: Deadline-ordered queue of timers, used by io_context:
//               - timer_wait_op: task node for one async_wait, which becomes
//                                an ordinary queued handler when it is due
//               - timer_state:   the waits of one steady_timer
//               - timer_queue:   every timer with a pending wait, ordered by
//                                deadline
// Usage:        Internal to io_context and steady_timer. timer_queue is not
//               synchronized: io_context calls it under its mutex.
//
//               A timer is in the queue once, however many waits it has, so
//               cancelling one is O(log n) in the number of timers. Entries
//               with equal deadlines stay in the order they were added
//               (std::multimap keeps that order).
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#ifndef SPANIO_DETAIL_TIMER_QUEUE_H
#define SPANIO_DETAIL_TIMER_QUEUE_H

#include "spanio/detail/scheduler_op.h"

#include <chrono>
#include <climits>
#include <cstddef>
#include <map>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>

namespace spanio { namespace detail {

// Clock all timers use
using timer_clock = std::chrono::steady_clock;

// A point in time on timer_clock
using timer_time_point = timer_clock::time_point;

// Longest wait a proactor poll() can be asked for, in milliseconds
constexpr long long kMaxTimerWaitMs = INT_MAX;

// Task node for one async_wait; the result is set before the node runs
class timer_wait_op : public scheduler_op
{
public:
    // Empty when the timer expired; errc::operation_canceled when cancelled
    std::error_code m_ec;

protected:
    // Parameter func: completion function of the concrete type
    explicit timer_wait_op(func_type func) noexcept
        : scheduler_op(func)
    {
    }
};

// Wait node that calls Handler with the result
template <typename Handler>
class wait_op : public timer_wait_op
{
public:
    // Parameter handler: function object taking (std::error_code)
    template <typename H>
    explicit wait_op(H&& handler)
        : timer_wait_op(&wait_op::do_complete)
        , m_handler(std::forward<H>(handler))
    {
    }

private:
    // Free the node, then call the handler if asked to
    // Parameter base:   this node
    // Parameter invoke: whether to call the handler
    static void do_complete(scheduler_op* base, bool invoke)
    {
        std::unique_ptr<wait_op> self(static_cast<wait_op*>(base));
        if (!invoke)
        {
            return;
        }
        const std::error_code ec = self->m_ec;
        Handler handler(std::move(self->m_handler));
        self.reset();
        handler(ec);
    }

    // Function object called with the result
    Handler m_handler;
};

// Allocate a wait node
// Parameter handler: function object taking (std::error_code)
// Return value: the new node, owned by the caller until handed over
template <typename Handler>
timer_wait_op* make_wait_op(Handler&& handler)
{
    return new wait_op<typename std::decay<Handler>::type>(std::forward<Handler>(handler));
}

struct timer_state;

// Timers with a pending wait, by deadline
using timer_map = std::multimap<timer_time_point, timer_state*>;

// The pending waits of one timer. It belongs to the steady_timer object;
// timer_queue guards it through the owner's io_context mutex.
struct timer_state
{
    // Pending waits, in the order they were started
    op_queue m_waits;
    // Number of nodes in m_waits
    std::size_t m_wait_count = 0;
    // The timer's entry in the queue; valid while m_scheduled
    timer_map::iterator m_entry;
    // The timer is in the queue
    bool m_scheduled = false;
};

// Every timer with a pending wait, earliest deadline first
class timer_queue
{
public:
    // Whether no timer is pending
    // Return value: true when empty
    bool empty() const noexcept
    {
        return m_entries.empty();
    }

    // Add a wait to a timer
    // Parameter state:    the timer
    // Parameter deadline: when it expires; it cannot change while the timer
    //                     has waits, because changing it cancels them
    // Parameter op:       the wait; the timer owns it until it is due
    // Return value: true if this timer is now the first to expire, so a
    //               thread waiting for the old first one must wake up
    bool add(timer_state& state, timer_time_point deadline, timer_wait_op* op)
    {
        if (!state.m_scheduled)
        {
            state.m_entry = m_entries.emplace(deadline, &state);
            state.m_scheduled = true;
        }
        state.m_waits.push(op);
        ++state.m_wait_count;
        return state.m_entry == m_entries.begin();
    }

    // Take a timer out of the queue and mark its waits as cancelled
    // Parameter state: the timer
    // Parameter out:   receives the waits, ready to run
    // Return value: number of waits cancelled
    std::size_t cancel(timer_state& state, op_queue& out)
    {
        if (!state.m_scheduled)
        {
            return 0;
        }
        m_entries.erase(state.m_entry);
        const std::size_t cancelled = state.m_wait_count;
        for (scheduler_op* op = state.m_waits.pop(); op != nullptr; op = state.m_waits.pop())
        {
            static_cast<timer_wait_op*>(op)->m_ec = std::make_error_code(std::errc::operation_canceled);
            out.push(op);
        }
        reset(state);
        return cancelled;
    }

    // Take every timer that has expired
    // Parameter now: current time
    // Parameter out: receives their waits, in deadline order
    void take_due(timer_time_point now, op_queue& out)
    {
        while (!m_entries.empty() && m_entries.begin()->first <= now)
        {
            take_first(out);
        }
    }

    // Take every timer, whether expired or not
    // Parameter out: receives their waits
    void take_all(op_queue& out)
    {
        while (!m_entries.empty())
        {
            take_first(out);
        }
    }

    // How long a poll may wait before the first timer expires. Rounded up,
    // so the wait does not end just before the deadline and spin.
    // Parameter now: current time
    // Return value: milliseconds, from 0 to kMaxTimerWaitMs; the queue must
    //               not be empty
    int wait_ms(timer_time_point now) const
    {
        const timer_time_point deadline = m_entries.begin()->first;
        if (deadline <= now)
        {
            return 0;
        }
        const long long wait = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
        return static_cast<int>(wait < kMaxTimerWaitMs ? wait : kMaxTimerWaitMs);
    }

private:
    // Remove the first timer and hand over its waits
    // Parameter out: receives the waits
    void take_first(op_queue& out)
    {
        timer_state& state = *m_entries.begin()->second;
        m_entries.erase(m_entries.begin());
        out.splice(state.m_waits);
        reset(state);
    }

    // Mark a timer as having no waits
    // Parameter state: the timer
    static void reset(timer_state& state) noexcept
    {
        state.m_wait_count = 0;
        state.m_scheduled = false;
    }

    // Timers with a pending wait
    timer_map m_entries;
};

}}

#endif // SPANIO_DETAIL_TIMER_QUEUE_H
