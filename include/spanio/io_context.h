// ============================================================================
// File purpose: io_context, the event loop of SpanIO. It owns the platform
//               io_proactor and a task queue, and runs user handlers and I/O
//               completion handlers on the threads that call run().
//               Also defines io_context::executor_type and
//               executor_work_guard.
// Usage:        spanio::io_context ctx;
//               ctx.post([] { std::puts("hello"); });
//               ctx.run();          // returns once there is no more work
//
//               Semantics (modeled on Asio):
//               - "Work" is: queued handlers, pending timer waits, started
//                 I/O operations that
//                 have not finished, and live executor_work_guard objects.
//                 run() returns when work drops to zero or stop() is called;
//                 the context is then stopped() and needs restart() before
//                 it runs handlers again.
//               - run() may be called from several threads at once. One of
//                 them at a time waits in the proactor; the others wait for
//                 handlers on a condition variable. Each handler runs exactly
//                 once, on one of those threads.
//               - post() never runs the handler inline. dispatch() runs it
//                 inline when called from a thread that is running this
//                 context, and posts it otherwise.
//               - An exception thrown by a handler propagates out of run()
//                 and leaves the context consistent; call run() again to
//                 continue.
//               - The destructor destroys handlers that never ran without
//                 calling them, after collecting every started I/O operation
//                 from the proactor. Those operations must already be
//                 cancelled (unregister or cancel their sockets first).
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_IO_CONTEXT_H
#define SPANIO_IO_CONTEXT_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/io_reactor.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/platform_proactor.h"
#include "spanio/detail/scheduler_op.h"
#include "spanio/detail/timer_queue.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace spanio {

class io_context;

}

namespace spanio { namespace detail {

class context_access;

}}

namespace spanio { namespace detail {

// How long the destructor waits for started I/O operations to come back
// before it gives up and leaks them (and the proactor they live in)
constexpr int kContextDrainTimeoutMs = 5000;

// Wait of one proactor poll() inside the destructor's drain loop
constexpr int kContextDrainPollSliceMs = 100;

// Proactor wait that returns at once; used by poll() / poll_one()
constexpr int kContextNoWait = 0;

// One entry of the per-thread stack of contexts the thread is running.
// It lets dispatch() and running_in_this_thread() find out whether the
// calling thread is inside run() of a given context, even when run() calls
// are nested.
struct context_frame
{
    // Context being run by this frame
    const io_context* m_context;
    // Enclosing frame of the same thread, or nullptr
    context_frame* m_previous;
};

// Top of the calling thread's stack of running contexts
// Return value: reference to the thread-local top pointer
inline context_frame*& current_frame() noexcept
{
    thread_local context_frame* t_top = nullptr;
    return t_top;
}

// Pushes a frame on construction and pops it on destruction, so the stack
// stays correct when a handler throws
class context_frame_scope
{
public:
    // Parameter context: context the calling thread is about to run
    explicit context_frame_scope(const io_context* context) noexcept
        : m_frame{context, current_frame()}
    {
        current_frame() = &m_frame;
    }

    ~context_frame_scope()
    {
        current_frame() = m_frame.m_previous;
    }

    context_frame_scope(const context_frame_scope&) = delete;
    context_frame_scope& operator=(const context_frame_scope&) = delete;

private:
    // The frame this scope keeps on the stack
    context_frame m_frame;
};

}}

namespace spanio {

// Event loop and task scheduler. See the file header for the semantics.
class io_context
{
public:
    class executor_type;

    // Create a context on the platform's default proactor
    // (make_platform_proactor). Throws std::system_error if the backend
    // cannot be created.
    io_context()
        : io_context(create_default_proactor())
    {
    }

    // Create a context on a given proactor; used by tests to cover every
    // backend. Throws std::invalid_argument if proactor is null.
    // Parameter proactor: backend the context takes ownership of
    explicit io_context(detail::io_proactor_ptr proactor)
        : m_proactor(std::move(proactor))
        , m_outstanding_work(0)
        , m_outstanding_io(0)
        , m_idle_threads(0)
        , m_stopped(false)
        , m_poller_active(false)
        , m_poller_interrupted(false)
    {
        if (m_proactor == nullptr)
        {
            throw std::invalid_argument("spanio::io_context: null proactor");
        }
    }

    io_context(const io_context&) = delete;
    io_context& operator=(const io_context&) = delete;

    // Destroy pending handlers without calling them and collect every
    // started I/O operation. No thread may be inside run() any more.
    ~io_context()
    {
        shutdown();
    }

    // Run handlers until there is no more work or stop() is called
    // Return value: number of handlers run by this call
    std::size_t run()
    {
        detail::context_frame_scope scope(this);
        std::size_t count = 0;
        while (do_run_one(true) != 0)
        {
            ++count;
        }
        return count;
    }

    // Block until one handler has run, there is no more work, or stop()
    // Return value: 1 if a handler ran, otherwise 0
    std::size_t run_one()
    {
        detail::context_frame_scope scope(this);
        return do_run_one(true);
    }

    // Run every handler that is ready, without blocking. I/O operations that
    // have already finished are collected first.
    // Return value: number of handlers run by this call
    std::size_t poll()
    {
        detail::context_frame_scope scope(this);
        std::size_t count = 0;
        while (do_run_one(false) != 0)
        {
            ++count;
        }
        return count;
    }

    // Run at most one ready handler, without blocking
    // Return value: 1 if a handler ran, otherwise 0
    std::size_t poll_one()
    {
        detail::context_frame_scope scope(this);
        return do_run_one(false);
    }

    // Make every run() / run_one() / poll() return as soon as possible;
    // queued handlers stay queued. Safe from any thread and from handlers.
    void stop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        stop_locked();
    }

    // Whether the context is stopped, by stop() or by running out of work
    // Return value: true when stopped
    bool stopped() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_stopped;
    }

    // Clear the stopped state so the context can run again. Call it only
    // when no thread is inside run().
    void restart()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stopped = false;
    }

    // Queue a function object; it never runs inside this call
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void post(Handler&& handler)
    {
        post_op(detail::make_handler_op(std::forward<Handler>(handler)));
    }

    // Run a function object inline if the calling thread is running this
    // context, otherwise post() it
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void dispatch(Handler&& handler)
    {
        if (running_in_this_thread())
        {
            handler();
            return;
        }
        post(std::forward<Handler>(handler));
    }

    // Whether the calling thread is inside run() / poll() of this context
    // Return value: true when it is
    bool running_in_this_thread() const noexcept
    {
        for (detail::context_frame* frame = detail::current_frame(); frame != nullptr;
             frame = frame->m_previous)
        {
            if (frame->m_context == this)
            {
                return true;
            }
        }
        return false;
    }

    // Lightweight handle for submitting work to this context
    // Return value: executor bound to this context
    executor_type get_executor() noexcept;

    // Name of the proactor backend, for logs and tests
    // Return value: static string, never null
    const char* backend_name() const noexcept
    {
        return m_proactor->name();
    }

private:
    friend class executor_work_guard;
    friend class detail::context_access;

    // ---- Internal interface for I/O objects, reached through
    // ---- detail::context_access (tcp::socket, acceptor, steady_timer, ...) ----

    // Register a socket with the proactor before starting I/O on it
    // Parameter handle: open socket
    // Return value: empty on success, otherwise the proactor's error
    std::error_code register_handle(detail::native_handle_t handle)
    {
        return m_proactor->register_handle(handle);
    }

    // Unregister a socket before closing it; its pending operations
    // complete as cancelled
    // Parameter handle: registered socket
    // Return value: empty on success, otherwise the proactor's error
    std::error_code unregister_handle(detail::native_handle_t handle)
    {
        return m_proactor->unregister_handle(handle);
    }

    // Cancel every pending operation on a socket; their handlers get
    // errc::operation_canceled
    // Parameter handle: registered socket
    void cancel_handle(detail::native_handle_t handle)
    {
        m_proactor->cancel(handle);
    }

    // Start an I/O operation created with detail::make_io_op() and filled
    // with a prepare_*() helper. The context owns it from now on; its
    // handler runs (or is destroyed) exactly once. Counts as work until then.
    // Parameter op: operation to start
    void start_io(detail::io_op_base* op)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_outstanding_work;
            ++m_outstanding_io;
        }
        m_proactor->start(op);
    }

    // Add a wait to a timer; used by steady_timer. The context owns the wait
    // from now on, and it counts as work until its handler has run.
    // Parameter state:    the timer's state; must stay valid until the wait
    //                     leaves the timer (the timer cancels in its
    //                     destructor)
    // Parameter deadline: when the timer expires
    // Parameter op:       wait created with detail::make_wait_op()
    void schedule_timer(detail::timer_state& state, detail::timer_time_point deadline,
                        detail::timer_wait_op* op)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_outstanding_work;
        if (m_timers.add(state, deadline, op))
        {
            // The thread polling is waiting for a later deadline, or for no
            // deadline at all
            if (m_poller_active)
            {
                interrupt_poller_locked();
            }
            else
            {
                wake_one_idle_locked();
            }
        }
    }

    // Cancel the waits of a timer; used by steady_timer. Their handlers run
    // with errc::operation_canceled.
    // Parameter state: the timer's state
    // Return value: number of waits cancelled
    std::size_t cancel_timer(detail::timer_state& state)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        detail::op_queue cancelled;
        const std::size_t count = m_timers.cancel(state, cancelled);
        if (count != 0)
        {
            // The waits stay counted as work, now as queued handlers
            m_queue.splice(cancelled);
            if (m_idle_threads > 0)
            {
                m_cond.notify_one();
            }
            else
            {
                interrupt_poller_locked();
            }
        }
        return count;
    }


    // Decrements the work count when a handler finishes, also when it throws
    class work_finished_on_exit
    {
    public:
        // Parameter owner: context whose work count to decrement
        explicit work_finished_on_exit(io_context& owner) noexcept
            : m_owner(owner)
        {
        }

        ~work_finished_on_exit()
        {
            m_owner.work_finished();
        }

        work_finished_on_exit(const work_finished_on_exit&) = delete;
        work_finished_on_exit& operator=(const work_finished_on_exit&) = delete;

    private:
        // Context whose work count is decremented
        io_context& m_owner;
    };

    // Create the platform's default proactor or throw
    // Return value: the proactor, never null
    static detail::io_proactor_ptr create_default_proactor()
    {
        std::error_code ec;
        detail::io_proactor_ptr proactor = detail::make_platform_proactor(ec);
        if (ec || proactor == nullptr)
        {
            detail::log_error("io_context: make_platform_proactor", ec);
            throw std::system_error(ec, "spanio::io_context: cannot create the proactor");
        }
        return proactor;
    }

    // Add one unit of work (used by executor_work_guard)
    void work_started()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_outstanding_work;
    }

    // Remove one unit of work; stops the context when none is left
    void work_finished()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        --m_outstanding_work;
        if (m_outstanding_work == 0)
        {
            stop_locked();
        }
    }

    // Queue a task node and wake a thread to run it
    // Parameter op: node from make_handler_op(); owned by the context now
    void post_op(detail::scheduler_op* op)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_outstanding_work;
        m_queue.push(op);
        if (m_idle_threads > 0)
        {
            m_cond.notify_one();
        }
        else
        {
            interrupt_poller_locked();
        }
    }

    // Set the stopped flag and wake every thread inside run(). Caller holds
    // m_mutex.
    void stop_locked()
    {
        m_stopped = true;
        m_cond.notify_all();
        interrupt_poller_locked();
    }

    // Wake the thread blocked in the proactor, once per poll. Caller holds
    // m_mutex.
    void interrupt_poller_locked()
    {
        if (m_poller_active && !m_poller_interrupted)
        {
            m_poller_interrupted = true;
            m_proactor->interrupt();
        }
    }

    // Wake one thread waiting for handlers, if any. Caller holds m_mutex.
    void wake_one_idle_locked()
    {
        if (m_idle_threads > 0)
        {
            m_cond.notify_one();
        }
    }

    // Queue the handlers of every timer that has expired. Caller holds
    // m_mutex.
    void take_due_timers_locked()
    {
        if (!m_timers.empty())
        {
            m_timers.take_due(detail::timer_clock::now(), m_queue);
        }
    }

    // How long the next proactor poll may wait. Caller holds m_mutex.
    // Parameter block: false means not at all
    // Return value: timeout for io_proactor::poll()
    int poll_timeout_locked(bool block) const
    {
        if (!block)
        {
            return detail::kContextNoWait;
        }
        if (m_timers.empty())
        {
            return detail::kInfiniteTimeout;
        }
        return m_timers.wait_ms(detail::timer_clock::now());
    }

    // Run at most one handler
    // Parameter block: true waits for a handler, false returns at once
    // Return value: 1 if a handler ran, otherwise 0
    std::size_t do_run_one(bool block)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (!m_stopped)
        {
            // Checked before every handler, so a steady stream of handlers
            // cannot keep a timer from firing
            take_due_timers_locked();
            if (!m_queue.empty())
            {
                detail::scheduler_op* op = m_queue.pop();
                // Hand the remaining handlers to another thread, and make
                // sure someone keeps collecting I/O and timers while this thread is
                // busy in the handler
                if (!m_queue.empty() || (!m_poller_active && (m_outstanding_io > 0 || !m_timers.empty())))
                {
                    wake_one_idle_locked();
                }
                lock.unlock();
                work_finished_on_exit finished(*this);
                op->complete(true);
                return 1;
            }
            if (m_outstanding_work == 0)
            {
                stop_locked();
                return 0;
            }
            if (!m_poller_active)
            {
                poll_proactor(lock, block);
                if (!block && m_queue.empty())
                {
                    return 0;
                }
                continue;
            }
            if (!block)
            {
                return 0;
            }
            ++m_idle_threads;
            m_cond.wait(lock);
            --m_idle_threads;
        }
        return 0;
    }

    // Become the poller: wait in the proactor without holding the lock, then
    // queue the completion handlers of the returned operations.
    // Parameter lock:  holds m_mutex on entry and on return
    // Parameter block: true waits until the next timer is due (without limit
    //                  if there is none), false does not wait
    void poll_proactor(std::unique_lock<std::mutex>& lock, bool block)
    {
        m_poller_active = true;
        m_poller_interrupted = false;
        const int timeout_ms = poll_timeout_locked(block);
        lock.unlock();

        std::error_code ec;
        m_proactor->poll(m_completions, timeout_ms, ec);

        lock.lock();
        m_poller_active = false;
        for (detail::io_operation* io : m_completions)
        {
            // Every operation the proactor sees was started by start_io(),
            // so it is the io_operation part of an io_op_base
            --m_outstanding_io;
            m_queue.push(static_cast<detail::io_op_base*>(io));
        }
        if (m_completions.size() > 1)
        {
            m_cond.notify_all();
        }
        m_completions.clear();
        if (ec)
        {
            detail::log_error("io_context: proactor poll", ec);
            throw std::system_error(ec, "spanio::io_context: proactor poll failed");
        }
    }

    // Destroy queued handlers without calling them, then collect every I/O
    // operation that is still in the proactor and destroy it too
    void shutdown()
    {
        detail::op_queue pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stopped = true;
            pending.splice(m_queue);
            m_timers.take_all(pending);
        }
        destroy_all(pending);

        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(detail::kContextDrainTimeoutMs);
        while (m_outstanding_io > 0)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                // Freeing the proactor (or the operations) while the kernel
                // may still write into them would corrupt memory; leaking is
                // the only safe choice
                detail::log_error("io_context: I/O operations not cancelled at shutdown, leaking",
                                  std::make_error_code(std::errc::timed_out));
                static_cast<void>(m_proactor.release());
                return;
            }
            std::error_code ec;
            m_proactor->poll(m_completions, detail::kContextDrainPollSliceMs, ec);
            if (ec)
            {
                detail::log_error("io_context: proactor poll during shutdown", ec);
            }
            for (detail::io_operation* io : m_completions)
            {
                --m_outstanding_io;
                static_cast<detail::io_op_base*>(io)->complete(false);
            }
            m_completions.clear();
        }

        // Destructors of discarded handlers may have posted new handlers
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            pending.splice(m_queue);
        }
        destroy_all(pending);
    }

    // Destroy every node of a queue without running it
    // Parameter queue: nodes to destroy; empty afterwards
    static void destroy_all(detail::op_queue& queue)
    {
        while (!queue.empty())
        {
            queue.pop()->complete(false);
        }
    }

    // Backend that runs the I/O operations
    detail::io_proactor_ptr m_proactor;
    // Guards every field below except m_completions
    mutable std::mutex m_mutex;
    // Signals threads waiting for handlers
    std::condition_variable m_cond;
    // Handlers ready to run, in order
    detail::op_queue m_queue;
    // Timers with pending waits; each wait counts as work
    detail::timer_queue m_timers;
    // Queued handlers + started I/O operations + live work guards
    std::size_t m_outstanding_work;
    // Started I/O operations the proactor has not returned yet
    std::size_t m_outstanding_io;
    // Threads waiting on m_cond
    std::size_t m_idle_threads;
    // Set by stop() and when work runs out; cleared by restart()
    bool m_stopped;
    // A thread is inside m_proactor->poll()
    bool m_poller_active;
    // interrupt() was already called for the current poll
    bool m_poller_interrupted;
    // Buffer for poll() results; only the thread that is the poller uses it
    detail::completion_list m_completions;
};

// Lightweight, copyable handle that submits work to an io_context
class io_context::executor_type
{
public:
    // Parameter context: context to submit to; must outlive the executor
    explicit executor_type(io_context& context) noexcept
        : m_context(&context)
    {
    }

    // The context this executor submits to
    // Return value: reference to the context
    io_context& context() const noexcept
    {
        return *m_context;
    }

    // Queue a function object on the context (see io_context::post)
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void post(Handler&& handler) const
    {
        m_context->post(std::forward<Handler>(handler));
    }

    // Run inline when inside the context, otherwise post (see
    // io_context::dispatch)
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void dispatch(Handler&& handler) const
    {
        m_context->dispatch(std::forward<Handler>(handler));
    }

    // Whether the calling thread is running the context
    // Return value: true when it is
    bool running_in_this_thread() const noexcept
    {
        return m_context->running_in_this_thread();
    }

    // Two executors are equal when they submit to the same context
    // Parameter other: executor to compare with
    // Return value: true when both refer to the same context
    bool operator==(const executor_type& other) const noexcept
    {
        return m_context == other.m_context;
    }

    // Negation of operator==
    // Parameter other: executor to compare with
    // Return value: true when the contexts differ
    bool operator!=(const executor_type& other) const noexcept
    {
        return m_context != other.m_context;
    }

private:
    // Context work is submitted to; never null
    io_context* m_context;
};

inline io_context::executor_type io_context::get_executor() noexcept
{
    return executor_type(*this);
}

// Keeps an io_context from running out of work, so run() keeps waiting for
// new handlers until the guard is reset or destroyed. Movable, not copyable.
class executor_work_guard
{
public:
    // Add one unit of work to a context
    // Parameter context: context to keep alive; must outlive the guard
    explicit executor_work_guard(io_context& context)
        : m_context(&context)
    {
        m_context->work_started();
    }

    // Take over the work unit of another guard
    // Parameter other: guard to move from; it no longer owns work afterwards
    executor_work_guard(executor_work_guard&& other) noexcept
        : m_context(other.m_context)
    {
        other.m_context = nullptr;
    }

    executor_work_guard(const executor_work_guard&) = delete;
    executor_work_guard& operator=(const executor_work_guard&) = delete;
    executor_work_guard& operator=(executor_work_guard&&) = delete;

    ~executor_work_guard()
    {
        reset();
    }

    // Give the work unit back; run() may return once nothing else is left.
    // Calling it again does nothing.
    void reset()
    {
        if (m_context != nullptr)
        {
            m_context->work_finished();
            m_context = nullptr;
        }
    }

    // Whether the guard still holds its work unit
    // Return value: true until reset()
    bool owns_work() const noexcept
    {
        return m_context != nullptr;
    }

private:
    // Context the work unit belongs to; nullptr after reset() or a move
    io_context* m_context;
};

// Create a work guard for a context
// Parameter context: context to keep alive
// Return value: guard holding one unit of work
inline executor_work_guard make_work_guard(io_context& context)
{
    return executor_work_guard(context);
}

// Create a work guard for the context of an executor
// Parameter executor: executor whose context to keep alive
// Return value: guard holding one unit of work
inline executor_work_guard make_work_guard(const io_context::executor_type& executor)
{
    return executor_work_guard(executor.context());
}

}

namespace spanio { namespace detail {

// The only way into the internal interface of io_context. I/O objects
// (tcp::socket, tcp::acceptor, udp::socket, steady_timer) and the tests of
// the execution engine call it; it is not part of the public API, so it may
// change without notice.
class context_access
{
public:
    // Register a socket with the proactor before starting I/O on it
    // Parameter context: the context
    // Parameter handle:  open socket
    // Return value: empty on success, otherwise the proactor's error
    static std::error_code register_handle(io_context& context, native_handle_t handle)
    {
        return context.register_handle(handle);
    }

    // Unregister a socket before closing it; pending operations complete as
    // cancelled
    // Parameter context: the context
    // Parameter handle:  registered socket
    // Return value: empty on success, otherwise the proactor's error
    static std::error_code unregister_handle(io_context& context, native_handle_t handle)
    {
        return context.unregister_handle(handle);
    }

    // Cancel every pending operation on a socket
    // Parameter context: the context
    // Parameter handle:  registered socket
    static void cancel_handle(io_context& context, native_handle_t handle)
    {
        context.cancel_handle(handle);
    }

    // Start an I/O operation made with make_io_op() and a prepare_*() helper
    // Parameter context: the context; owns the operation from now on
    // Parameter op:      operation to start
    static void start_io(io_context& context, io_op_base* op)
    {
        context.start_io(op);
    }

    // Add a wait to a timer
    // Parameter context:  the context; owns the wait from now on
    // Parameter state:    the timer's state
    // Parameter deadline: when the timer expires
    // Parameter op:       wait made with make_wait_op()
    static void schedule_timer(io_context& context, timer_state& state, timer_time_point deadline,
                               timer_wait_op* op)
    {
        context.schedule_timer(state, deadline, op);
    }

    // Cancel the waits of a timer
    // Parameter context: the context
    // Parameter state:   the timer's state
    // Return value: number of waits cancelled
    static std::size_t cancel_timer(io_context& context, timer_state& state)
    {
        return context.cancel_timer(state);
    }
};

}}

#endif // SPANIO_IO_CONTEXT_H
