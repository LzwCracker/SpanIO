// ============================================================================
// File purpose: strand, a serialization domain on top of an executor, and
//               bind_executor, which routes a completion handler through an
//               executor (typically a strand).
// Usage:        spanio::io_context ctx;
//               auto s = spanio::make_strand(ctx);
//               auto on_read = [&](std::error_code ec, std::size_t n)
//               {
//                   // never runs at the same time as another handler of s
//               };
//               sock.async_read_some(spanio::buffer(buf),
//                                    spanio::bind_executor(s, on_read));
//               // run ctx from as many threads as wanted
//
//               Rules:
//               - Handlers of one strand (and of its copies) never run at the
//                 same time and run in the order they were queued. Memory
//                 written by one handler is visible to the next one.
//                 Different strands run in parallel.
//               - post() never runs the handler inline. dispatch() runs it
//                 inline when the calling thread is already running a handler
//                 of this strand, or when it is inside run() of the strand's
//                 context and the strand is idle; otherwise it acts as post().
//               - Each queued handler is one handler of the io_context: it
//                 counts as work until it ran, and run() counts it once.
//                 A strand without queued handlers is not work.
//               - A handler that throws: the exception leaves run(); the
//                 handlers queued after it stay queued in order and run on the
//                 next run().
//               - When the io_context is destroyed, handlers still queued on
//                 its strands are destroyed without being called. A strand
//                 may outlive the context, but must not be used afterwards.
//               - bind_executor(s, handler) returns a function object that,
//                 when the operation completes, saves the arguments (decay
//                 copies, moved when possible) and dispatch()es the handler to
//                 s with them. The handler receives them as rvalues, so it may
//                 take move-only values such as tcp::socket by value.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-08
// ============================================================================

#ifndef SPANIO_STRAND_H
#define SPANIO_STRAND_H

#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/scheduler_op.h"

#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

namespace spanio { namespace detail {

// Shared state of one strand and all of its copies
class strand_impl
{
public:
    strand_impl() noexcept
        : m_locked(false)
    {
    }

    strand_impl(const strand_impl&) = delete;
    strand_impl& operator=(const strand_impl&) = delete;

    // Destroy, without calling, every handler that never ran
    ~strand_impl()
    {
        destroy_waiting(m_waiting);
    }

    // Discard every node of a queue without running it
    // Parameter queue: nodes to destroy; empty afterwards
    static void destroy_waiting(op_queue& queue) noexcept
    {
        while (scheduler_op* op = queue.pop())
        {
            op->complete(false);
        }
    }

    // Guards m_waiting and m_locked
    std::mutex m_mutex;
    // Handlers queued on the strand that have not started, oldest first
    op_queue m_waiting;
    // True while the strand is owned: an invoker is queued on the context or
    // a handler of the strand is running. At most one owner exists, which is
    // what keeps handlers from overlapping.
    bool m_locked;
};

// One entry of the per-thread stack of strands whose handler the thread is
// running; nested when a strand handler dispatches to another strand
struct strand_frame
{
    // Strand whose handler is running
    const strand_impl* m_impl;
    // Enclosing frame of the same thread, or nullptr
    strand_frame* m_previous;
};

// Top of the calling thread's stack of running strands
// Return value: reference to the thread-local top pointer
inline strand_frame*& current_strand_frame() noexcept
{
    thread_local strand_frame* t_top = nullptr;
    return t_top;
}

// Pushes a strand frame on construction and pops it on destruction, so the
// stack stays correct when a handler throws
class strand_frame_scope
{
public:
    // Parameter impl: strand whose handler the thread is about to run
    explicit strand_frame_scope(const strand_impl* impl) noexcept
        : m_frame{impl, current_strand_frame()}
    {
        current_strand_frame() = &m_frame;
    }

    ~strand_frame_scope()
    {
        current_strand_frame() = m_frame.m_previous;
    }

    strand_frame_scope(const strand_frame_scope&) = delete;
    strand_frame_scope& operator=(const strand_frame_scope&) = delete;

private:
    // The frame this scope keeps on the stack
    strand_frame m_frame;
};

// Whether the calling thread is running a handler of a strand
// Parameter impl: the strand
// Return value: true when a frame of that strand is on the thread's stack
inline bool strand_running_in_this_thread(const strand_impl* impl) noexcept
{
    for (strand_frame* frame = current_strand_frame(); frame != nullptr; frame = frame->m_previous)
    {
        if (frame->m_impl == impl)
        {
            return true;
        }
    }
    return false;
}

// Handler posted to the inner executor while the strand is owned. Running
// it runs the oldest queued handler of the strand, then either posts the
// next invoker (more handlers are waiting) or releases the strand.
template <typename Executor>
class strand_invoker
{
public:
    // Parameter impl:  strand to run; must be owned (m_locked) by the caller,
    //                  who hands that ownership to this invoker
    // Parameter inner: executor to post the next invoker to
    strand_invoker(std::shared_ptr<strand_impl> impl, const Executor& inner)
        : m_impl(std::move(impl))
        , m_inner(inner)
    {
    }

    strand_invoker(strand_invoker&& other) noexcept
        : m_impl(std::move(other.m_impl))
        , m_inner(other.m_inner)
    {
    }

    strand_invoker(const strand_invoker&) = delete;
    strand_invoker& operator=(const strand_invoker&) = delete;
    strand_invoker& operator=(strand_invoker&&) = delete;

    // An invoker the context destroys without running it (the context is
    // being destroyed) takes the queued handlers with it, so they are
    // destroyed now and not when the last strand copy goes away
    ~strand_invoker()
    {
        if (m_impl == nullptr)
        {
            return;
        }
        op_queue discarded;
        {
            std::lock_guard<std::mutex> lock(m_impl->m_mutex);
            discarded.splice(m_impl->m_waiting);
        }
        // Outside the lock: a handler's destructor may use the strand
        strand_impl::destroy_waiting(discarded);
    }

    // Run the oldest queued handler of the strand
    void operator()()
    {
        // Running releases this invoker's claim on the queue, see the
        // destructor
        std::shared_ptr<strand_impl> impl = std::move(m_impl);
        scheduler_op* op = nullptr;
        {
            std::lock_guard<std::mutex> lock(impl->m_mutex);
            op = impl->m_waiting.pop();
        }
        if (op == nullptr)
        {
            // Defensive: an owned strand always has a queued handler when an
            // invoker runs. Release it so it cannot stay locked forever.
            log_error("strand: invoker (no queued handler)",
                      std::make_error_code(std::errc::state_not_recoverable));
            finish(impl, m_inner);
            return;
        }
        run_and_finish(impl, m_inner, op);
    }

    // Run one handler on the calling thread as a handler of the strand, then
    // hand the strand on (see finish). The strand must be owned by the
    // caller.
    // Parameter impl:  the strand
    // Parameter inner: executor to post the next invoker to
    // Parameter op:    handler node to run; freed by this call
    static void run_and_finish(const std::shared_ptr<strand_impl>& impl, const Executor& inner,
                               scheduler_op* op)
    {
        finish_on_exit finished(impl, inner);
        strand_frame_scope scope(impl.get());
        op->complete(true);
    }

    // Hand the strand on after a handler: post the next invoker when more
    // handlers are queued, otherwise release the strand
    // Parameter impl:  the strand, owned by the caller
    // Parameter inner: executor to post the next invoker to
    static void finish(const std::shared_ptr<strand_impl>& impl, const Executor& inner)
    {
        {
            std::lock_guard<std::mutex> lock(impl->m_mutex);
            if (impl->m_waiting.empty())
            {
                impl->m_locked = false;
                return;
            }
        }
        // Still owned: the new invoker inherits the ownership
        inner.post(strand_invoker(impl, inner));
    }

private:
    // Calls finish() when a handler returns, also when it throws, so the
    // strand never stays owned by a handler that is gone
    class finish_on_exit
    {
    public:
        // Parameter impl:  the strand
        // Parameter inner: executor to post the next invoker to
        finish_on_exit(const std::shared_ptr<strand_impl>& impl, const Executor& inner) noexcept
            : m_impl(impl)
            , m_inner(inner)
        {
        }

        ~finish_on_exit()
        {
            if (std::uncaught_exceptions() > 0)
            {
                log_error("strand: handler", make_error_code(error::misc_errors::handler_failed));
            }
            finish(m_impl, m_inner);
        }

        finish_on_exit(const finish_on_exit&) = delete;
        finish_on_exit& operator=(const finish_on_exit&) = delete;

    private:
        // The strand to hand on
        const std::shared_ptr<strand_impl>& m_impl;
        // Executor to post the next invoker to
        const Executor& m_inner;
    };

    // Strand to run; nullptr once the invoker ran or was moved from
    std::shared_ptr<strand_impl> m_impl;
    // Executor the strand runs on
    Executor m_inner;
};

// Handler and saved arguments of a completed operation, called later on
// the bound executor
template <typename Handler, typename... Args>
class bound_call
{
public:
    // Parameter handler: user handler; moved in
    // Parameter args:    completion arguments; decay-copied or moved in
    template <typename H, typename... A>
    explicit bound_call(H&& handler, A&&... args)
        : m_handler(std::forward<H>(handler))
        , m_args(std::forward<A>(args)...)
    {
    }

    // Call the handler with the saved arguments, as rvalues
    void operator()()
    {
        invoke(std::index_sequence_for<Args...>());
    }

private:
    // Unpack the saved arguments
    // Parameter (unnamed): indexes of m_args
    template <std::size_t... Index>
    void invoke(std::index_sequence<Index...>)
    {
        m_handler(std::move(std::get<Index>(m_args))...);
    }

    // The user handler
    Handler m_handler;
    // Arguments the operation completed with
    std::tuple<Args...> m_args;
};

}}

namespace spanio {

// Serialization domain: handlers bound to one strand (and its copies) never
// run at the same time and run in FIFO order, even with several threads in
// run(). See the file header for the rules. Copies share the domain.
template <typename Executor>
class strand
{
public:
    // Type of the executor the strand runs its handlers on
    using inner_executor_type = Executor;

    // Create a new, independent strand
    // Parameter inner: executor to run the handlers on; its context must
    //                  outlive every use of the strand
    explicit strand(const Executor& inner)
        : m_impl(std::make_shared<detail::strand_impl>())
        , m_inner(inner)
    {
    }

    // The executor the strand runs its handlers on
    // Return value: copy of the inner executor
    Executor get_inner_executor() const noexcept
    {
        return m_inner;
    }

    // The context the handlers run in
    // Return value: reference to the context
    io_context& context() const noexcept
    {
        return m_inner.context();
    }

    // Queue a function object on the strand; it never runs inside this call
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void post(Handler&& handler) const
    {
        enqueue(detail::make_handler_op(std::forward<Handler>(handler)));
    }

    // Run a function object inline when allowed (the calling thread runs a
    // handler of this strand, or runs the context while the strand is idle),
    // otherwise queue it like post()
    // Parameter handler: function object taking no arguments
    template <typename Handler>
    void dispatch(Handler&& handler) const
    {
        if (running_in_this_thread())
        {
            handler();
            return;
        }
        detail::scheduler_op* op = detail::make_handler_op(std::forward<Handler>(handler));
        if (m_inner.running_in_this_thread())
        {
            bool acquired = false;
            {
                std::lock_guard<std::mutex> lock(m_impl->m_mutex);
                if (!m_impl->m_locked)
                {
                    m_impl->m_locked = true;
                    acquired = true;
                }
            }
            if (acquired)
            {
                detail::strand_invoker<Executor>::run_and_finish(m_impl, m_inner, op);
                return;
            }
        }
        enqueue(op);
    }

    // Whether the calling thread is running a handler of this strand
    // Return value: true when it is, also inside a nested strand's handler
    bool running_in_this_thread() const noexcept
    {
        return detail::strand_running_in_this_thread(m_impl.get());
    }

    // Two strands are equal when they share the serialization domain
    // Parameter other: strand to compare with
    // Return value: true when one is a copy of the other
    bool operator==(const strand& other) const noexcept
    {
        return m_impl == other.m_impl;
    }

    // Negation of operator==
    // Parameter other: strand to compare with
    // Return value: true for different domains
    bool operator!=(const strand& other) const noexcept
    {
        return m_impl != other.m_impl;
    }

private:
    // Queue a handler node; post an invoker when the strand was idle
    // Parameter op: node from make_handler_op(); owned by the strand now
    void enqueue(detail::scheduler_op* op) const
    {
        bool first = false;
        {
            std::lock_guard<std::mutex> lock(m_impl->m_mutex);
            m_impl->m_waiting.push(op);
            if (!m_impl->m_locked)
            {
                m_impl->m_locked = true;
                first = true;
            }
        }
        if (first)
        {
            m_inner.post(detail::strand_invoker<Executor>(m_impl, m_inner));
        }
    }

    // Shared state of this strand and its copies; never null
    std::shared_ptr<detail::strand_impl> m_impl;
    // Executor the handlers run on
    Executor m_inner;
};

// Create a new strand on a context
// Parameter context: context to run the handlers in
// Return value: the strand
inline strand<io_context::executor_type> make_strand(io_context& context)
{
    return strand<io_context::executor_type>(context.get_executor());
}

// Create a new strand on an executor
// Parameter executor: executor to run the handlers on
// Return value: the strand
template <typename Executor>
strand<Executor> make_strand(const Executor& executor)
{
    return strand<Executor>(executor);
}

// Function object that dispatches a handler to an executor with the
// arguments it is called with. Made by bind_executor().
template <typename Handler, typename Executor>
class executor_binder
{
public:
    // Parameter executor: executor to dispatch to
    // Parameter handler:  handler to call there; moved or copied in
    template <typename H>
    executor_binder(const Executor& executor, H&& handler)
        : m_executor(executor)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Save the arguments and dispatch the handler with them. Called at most
    // once, because the handler is moved out.
    // Parameter args: completion arguments of the operation
    template <typename... Args>
    void operator()(Args&&... args)
    {
        m_executor.dispatch(detail::bound_call<Handler, typename std::decay<Args>::type...>(
            std::move(m_handler), std::forward<Args>(args)...));
    }

    // The executor handlers are dispatched to
    // Return value: copy of the executor
    Executor get_executor() const noexcept
    {
        return m_executor;
    }

private:
    // Executor the handler runs on
    Executor m_executor;
    // The user handler
    Handler m_handler;
};

// Bind a handler to an executor, usually a strand
// Parameter executor: executor to run the handler on
// Parameter handler:  completion handler
// Return value: function object to pass as the operation's handler
template <typename Executor, typename Handler>
executor_binder<typename std::decay<Handler>::type, Executor> bind_executor(const Executor& executor,
                                                                           Handler&& handler)
{
    return executor_binder<typename std::decay<Handler>::type, Executor>(executor,
                                                                         std::forward<Handler>(handler));
}

}

#endif // SPANIO_STRAND_H
