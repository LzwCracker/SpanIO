// ============================================================================
// File purpose: C++20 coroutine support (design doc 6.3, M5):
//               - spanio::awaitable<T>: return type of a SpanIO coroutine
//               - spanio::use_awaitable: completion token; the async_*
//                 function returns an awaitable<value> for co_await. An
//                 error code is thrown as std::system_error; wrap the token
//                 in spanio::as_tuple() to get it as a value instead.
//               - spanio::co_spawn(executor, awaitable, token): start a
//                 coroutine on an io_context or a strand. Its completion is
//                 void(std::exception_ptr) or void(std::exception_ptr, T),
//                 so it takes any token (callback, detached, use_future,
//                 use_awaitable).
//
//               Semantics:
//               - co_spawn never runs the coroutine inside the call; it
//                 starts on the executor like a posted handler.
//               - After each co_await of an operation the coroutine resumes
//                 through its executor: on a thread inside io_context::run(),
//                 and inside the strand when spawned on one, so coroutines of
//                 one strand never run at the same time.
//               - Closing a socket or cancelling a timer makes the pending
//                 co_await fail with errc::operation_canceled.
//               - If the io_context is destroyed while a coroutine waits, the
//                 handler that would resume it is destroyed, and it destroys
//                 the whole coroutine (all frames, all locals) instead of
//                 leaking it. Sockets owned by the coroutine must be closed
//                 before that, as for callbacks (io_context.h).
//               - An awaitable starts only when it is co_awaited, and the
//                 objects an operation uses (socket, buffer) must stay valid
//                 until then: co_await operations right away.
// Usage:        Needs C++20 (-std=c++20 / /std:c++20). spanio/spanio.h
//               includes this header when SPANIO_ENABLE_COROUTINE is defined
//               (CMake option of the same name); it can also be included on
//               its own.
//
//               spanio::awaitable<void> echo(spanio::tcp::socket sock)
//               {
//                   char data[1024];
//                   for (;;)
//                   {
//                       auto [ec, n] = co_await sock.async_read_some(
//                           spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
//                       if (ec)
//                       {
//                           co_return;
//                       }
//                       co_await sock.async_write(spanio::buffer(data, n), spanio::use_awaitable);
//                   }
//               }
//               spanio::co_spawn(ctx, echo(std::move(sock)), spanio::detached);
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19
// ============================================================================

#ifndef SPANIO_COROUTINE_H
#define SPANIO_COROUTINE_H

#if !defined(__cpp_impl_coroutine)
#error "spanio/coroutine.h needs C++20 coroutines: compile with -std=c++20 or /std:c++20"
#endif

#include "spanio/async_result.h"
#include "spanio/io_context.h"
#include "spanio/detail/platform.h"

#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace spanio {

template <typename T = void>
class awaitable;

}

namespace spanio { namespace detail {

// Resumes one suspended coroutine, exactly once. If it is destroyed without
// having run (its io_context or strand was destroyed first), it destroys the
// coroutine tree it belongs to, so the frames and their locals do not leak.
// Move-only; a moved-from resumer owns nothing.
class coro_resumer
{
public:
    // Parameter handle: coroutine to resume
    // Parameter owner:  outermost frame of the coroutine tree (the co_spawn
    //                   frame); destroyed when the resumer is dropped
    coro_resumer(std::coroutine_handle<> handle, std::coroutine_handle<> owner) noexcept
        : m_handle(handle)
        , m_owner(owner)
    {
    }

    // Parameter other: resumer to take over; owns nothing afterwards
    coro_resumer(coro_resumer&& other) noexcept
        : m_handle(std::exchange(other.m_handle, nullptr))
        , m_owner(std::exchange(other.m_owner, nullptr))
    {
    }

    coro_resumer(const coro_resumer&) = delete;
    coro_resumer& operator=(const coro_resumer&) = delete;
    coro_resumer& operator=(coro_resumer&&) = delete;

    // Destroy the coroutine tree if the resumer never ran
    ~coro_resumer()
    {
        if (m_owner)
        {
            m_owner.destroy();
        }
    }

    // Resume the coroutine; it now owns itself again
    void operator()()
    {
        const std::coroutine_handle<> handle = std::exchange(m_handle, nullptr);
        m_owner = nullptr;
        handle.resume();
    }

private:
    // Coroutine to resume, or null once run or moved from
    std::coroutine_handle<> m_handle;
    // Frame destroyed if the resumer is dropped, or null
    std::coroutine_handle<> m_owner;
};

// Executor of a coroutine tree with its concrete type erased, so every frame
// of the tree can resume through it
class coro_executor
{
public:
    virtual ~coro_executor() = default;

    // Queue a resumption; it never runs inside the call
    // Parameter resumer: what to run
    virtual void post(coro_resumer resumer) = 0;

    // Run a resumption inline when the executor allows it, otherwise queue it
    // Parameter resumer: what to run
    virtual void dispatch(coro_resumer resumer) = 0;
};

// coro_executor for a concrete executor (io_context::executor_type or a
// strand): forwards to its post() and dispatch()
template <typename Executor>
class coro_executor_impl final : public coro_executor
{
public:
    // Parameter executor: executor to run the coroutine on; copied
    explicit coro_executor_impl(const Executor& executor)
        : m_executor(executor)
    {
    }

    // See coro_executor::post
    void post(coro_resumer resumer) override
    {
        // A local copy, as in dispatch()
        Executor executor(m_executor);
        executor.post(std::move(resumer));
    }

    // See coro_executor::dispatch. The resumption may run inline and finish
    // the coroutine tree, which destroys this object (the co_spawn frame
    // owns it), while the executor still uses its own state afterwards (a
    // strand unlocks itself after running the handler inline). Dispatching
    // through a local copy keeps that state alive until the call returns.
    void dispatch(coro_resumer resumer) override
    {
        Executor executor(m_executor);
        executor.dispatch(std::move(resumer));
    }

private:
    // The executor the coroutine tree runs on
    Executor m_executor;
};

// Data every promise of a coroutine tree carries: where to resume and which
// frame owns the tree. A frame inherits both from the frame that awaits it.
class coro_promise_base
{
public:
    // Executor of the tree; owned by the co_spawn frame
    coro_executor* m_executor = nullptr;
    // The co_spawn frame, destroyed when a resumption is dropped
    std::coroutine_handle<> m_owner;
};

// Result of a coroutine or of an awaited operation: a value or an exception
template <typename T>
class coro_result
{
public:
    // Parameter value: the result
    void set_value(T&& value)
    {
        m_value.emplace(std::move(value));
    }

    // Parameter error: the failure
    void set_error(const std::exception_ptr& error) noexcept
    {
        m_error = error;
    }

    // Hand out the result
    // Return value: the value; the exception is rethrown instead if there is one
    T take()
    {
        if (m_error)
        {
            std::rethrow_exception(m_error);
        }
        return std::move(*m_value);
    }

private:
    // The value, once set
    std::optional<T> m_value;
    // The failure, or null
    std::exception_ptr m_error;
};

// Result without a value: success or an exception
template <>
class coro_result<void>
{
public:
    // Report success
    void set_value() noexcept
    {
    }

    // Parameter error: the failure
    void set_error(const std::exception_ptr& error) noexcept
    {
        m_error = error;
    }

    // Rethrow the failure, if any
    void take()
    {
        if (m_error)
        {
            std::rethrow_exception(m_error);
        }
    }

private:
    // The failure, or null
    std::exception_ptr m_error;
};

// Final suspension of an awaitable: transfers control to the frame that
// awaited it (symmetric transfer, so long chains do not grow the stack)
template <typename Promise>
class awaitable_final_awaiter
{
public:
    // Always suspend: the awaiting frame reads the result, then the
    // awaitable object destroys this frame
    bool await_ready() const noexcept
    {
        return false;
    }

    // Parameter self: the finishing frame
    // Return value: the frame to continue with
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> self) noexcept
    {
        return self.promise().m_continuation;
    }

    // Never resumed
    void await_resume() const noexcept
    {
    }
};

// Promise parts shared by awaitable<T> and awaitable<void>
template <typename T>
class awaitable_promise_base : public coro_promise_base
{
public:
    // Lazy start: the body runs when the awaitable is co_awaited
    std::suspend_always initial_suspend() const noexcept
    {
        return {};
    }

    // Store an exception escaping the body; the awaiting frame rethrows it
    void unhandled_exception() noexcept
    {
        m_result.set_error(std::current_exception());
    }

    // Frame to resume when this one finishes
    std::coroutine_handle<> m_continuation;
    // Value or exception of the body
    coro_result<T> m_result;
};

// Promise of awaitable<T>
template <typename T>
class awaitable_promise : public awaitable_promise_base<T>
{
public:
    // Return value: the awaitable owning this frame
    awaitable<T> get_return_object() noexcept;

    // Return value: awaiter that hands control back to the awaiting frame
    awaitable_final_awaiter<awaitable_promise> final_suspend() const noexcept
    {
        return {};
    }

    // Parameter value: operand of co_return
    void return_value(T value)
    {
        this->m_result.set_value(std::move(value));
    }
};

// Promise of awaitable<void>
template <>
class awaitable_promise<void> : public awaitable_promise_base<void>
{
public:
    // Return value: the awaitable owning this frame
    awaitable<void> get_return_object() noexcept;

    // Return value: awaiter that hands control back to the awaiting frame
    awaitable_final_awaiter<awaitable_promise> final_suspend() const noexcept
    {
        return {};
    }

    // co_return without a value, or the end of the body
    void return_void() noexcept
    {
    }
};

}}

namespace spanio {

// Return type of a SpanIO coroutine, and the result of
// async_*(..., use_awaitable). Owns the coroutine frame; move-only. Starts
// when co_awaited from another awaitable or from co_spawn.
template <typename T>
class awaitable
{
public:
    // Promise type the compiler uses for coroutines returning awaitable<T>
    using promise_type = detail::awaitable_promise<T>;

    // Value produced by co_await
    using value_type = T;

    // Parameter handle: frame to own
    explicit awaitable(std::coroutine_handle<promise_type> handle) noexcept
        : m_handle(handle)
    {
    }

    // Parameter other: awaitable to take over; empty afterwards
    awaitable(awaitable&& other) noexcept
        : m_handle(std::exchange(other.m_handle, nullptr))
    {
    }

    awaitable(const awaitable&) = delete;
    awaitable& operator=(const awaitable&) = delete;
    awaitable& operator=(awaitable&&) = delete;

    // Destroy the frame (and, through its locals, every frame it awaits)
    ~awaitable()
    {
        if (m_handle)
        {
            m_handle.destroy();
        }
    }

    // A coroutine is never finished before it is awaited
    // Return value: false
    bool await_ready() const noexcept
    {
        return false;
    }

    // Start the coroutine as part of the awaiting frame's tree
    // Parameter caller: the awaiting frame; its promise must derive from
    //                   detail::coro_promise_base (a SpanIO coroutine)
    // Return value: this coroutine, started by symmetric transfer
    template <typename CallerPromise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<CallerPromise> caller) noexcept
    {
        static_assert(std::is_base_of<detail::coro_promise_base, CallerPromise>::value,
                      "spanio::awaitable can only be co_awaited from a spanio::awaitable coroutine");
        promise_type& promise = m_handle.promise();
        promise.m_continuation = caller;
        promise.m_executor = caller.promise().m_executor;
        promise.m_owner = caller.promise().m_owner;
        return m_handle;
    }

    // Return value: what the coroutine returned; its exception is rethrown
    T await_resume()
    {
        return m_handle.promise().m_result.take();
    }

private:
    // Owned frame, or null after a move
    std::coroutine_handle<promise_type> m_handle;
};

}

namespace spanio { namespace detail {

template <typename T>
awaitable<T> awaitable_promise<T>::get_return_object() noexcept
{
    return awaitable<T>(std::coroutine_handle<awaitable_promise>::from_promise(*this));
}

inline awaitable<void> awaitable_promise<void>::get_return_object() noexcept
{
    return awaitable<void>(std::coroutine_handle<awaitable_promise>::from_promise(*this));
}

}}

namespace spanio {

// Token type of spanio::use_awaitable
class use_awaitable_t
{
public:
    constexpr use_awaitable_t() noexcept = default;
};

// Token: the async_* function returns an awaitable of the result; must be
// co_awaited inside a SpanIO coroutine
inline constexpr use_awaitable_t use_awaitable{};

}

namespace spanio { namespace detail {

// Handler built for use_awaitable: stores the completion in the waiting
// frame and resumes it through the tree's executor. Destroyed without being
// called, its resumer destroys the coroutine tree.
template <typename Signature>
class awaitable_handler
{
public:
    // Value the awaiting frame receives
    using value_type = typename completion_traits<Signature>::value_type;

    // Parameter result:   slot in the waiting frame
    // Parameter resumer:  resumes the waiting frame
    // Parameter executor: executor of the tree
    awaitable_handler(coro_result<value_type>* result, coro_resumer resumer, coro_executor* executor) noexcept
        : m_result(result)
        , m_resumer(std::move(resumer))
        , m_executor(executor)
    {
    }

    // Store the completion and resume the waiting frame
    // Parameter args: the completion's arguments
    template <typename... Args>
    void operator()(Args&&... args)
    {
        completion_traits<Signature>::store(*m_result, std::forward<Args>(args)...);
        m_executor->dispatch(std::move(m_resumer));
    }

private:
    // Where the completion goes
    coro_result<value_type>* m_result;
    // Resumes the waiting frame, or destroys the tree if dropped
    coro_resumer m_resumer;
    // Executor the frame resumes on
    coro_executor* m_executor;
};

// Awaiter that starts an asynchronous operation and suspends until its
// handler runs
template <typename Signature, typename Initiation>
class operation_awaiter
{
public:
    // Value the operation delivers
    using value_type = typename completion_traits<Signature>::value_type;

    // Parameter initiation: the async_* function's initiation
    explicit operation_awaiter(Initiation initiation)
        : m_initiation(std::move(initiation))
    {
    }

    // Handlers never run inside the initiating call, so always suspend
    // Return value: false
    bool await_ready() const noexcept
    {
        return false;
    }

    // Start the operation with a handler that resumes this frame
    // Parameter self: the suspended frame (a SpanIO coroutine)
    template <typename Promise>
    void await_suspend(std::coroutine_handle<Promise> self)
    {
        coro_promise_base& promise = self.promise();
        awaitable_handler<Signature> handler(&m_result, coro_resumer(self, promise.m_owner), promise.m_executor);
        // From the moment the operation is started, another thread may
        // resume (and finish) this frame, so nothing of *this may be touched
        // afterwards: run a local copy of the initiation
        Initiation initiation(std::move(m_initiation));
        initiation(std::move(handler));
    }

    // Return value: the operation's value; its error is thrown
    value_type await_resume()
    {
        return m_result.take();
    }

private:
    // The async_* function's initiation, run once
    Initiation m_initiation;
    // Filled by the handler before the frame resumes
    coro_result<value_type> m_result;
};

// Coroutine that wraps one operation, so every token use returns the same
// type, awaitable<value>
// Parameter initiation: the async_* function's initiation
// Return value: awaitable of the operation's value
template <typename Signature, typename Initiation>
awaitable<typename completion_traits<Signature>::value_type> run_operation(Initiation initiation)
{
    co_return co_await operation_awaiter<Signature, Initiation>(std::move(initiation));
}

}}

namespace spanio {

// use_awaitable: the operation starts when the returned awaitable is
// co_awaited
template <typename Signature>
class async_result<use_awaitable_t, Signature>
{
public:
    // Value of the operation (see async_result.h)
    using value_type = typename detail::completion_traits<Signature>::value_type;

    // What the async_* function returns
    using return_type = awaitable<value_type>;

    // Wrap the initiation in an awaitable
    // Parameter initiation: function object taking the handler
    // Return value: awaitable that runs the operation when co_awaited
    template <typename Initiation, typename RawToken>
    static return_type initiate(Initiation&& initiation, RawToken&&)
    {
        using initiation_type = typename std::decay<Initiation>::type;
        return detail::run_operation<Signature, initiation_type>(std::forward<Initiation>(initiation));
    }
};

}

namespace spanio { namespace detail {

// Completion signature of co_spawn for a coroutine returning T
template <typename T>
struct spawn_signature
{
    // The exception that ended the coroutine (or null), then its value
    using type = void(std::exception_ptr, T);
};

// Completion signature of co_spawn for a coroutine returning nothing
template <>
struct spawn_signature<void>
{
    // The exception that ended the coroutine, or null
    using type = void(std::exception_ptr);
};

// Coroutine type of the outermost frame that co_spawn creates. The frame
// owns the executor of the tree and destroys itself when it finishes.
class spawn_task
{
public:
    // Promise of the co_spawn frame
    class promise_type : public coro_promise_base
    {
    public:
        // Return value: the task owning this frame until co_spawn starts it
        spawn_task get_return_object() noexcept
        {
            return spawn_task(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        // Started by co_spawn through the executor, never inline
        std::suspend_always initial_suspend() const noexcept
        {
            return {};
        }

        // The frame frees itself (and its executor) when the body ends
        std::suspend_never final_suspend() const noexcept
        {
            return {};
        }

        // End of the body
        void return_void() noexcept
        {
        }

        // The body catches the user's exceptions itself; reaching here means
        // posting the completion failed (out of memory), with no one left to
        // report it to
        void unhandled_exception() noexcept
        {
            log_error("co_spawn: completion could not be delivered",
                      std::make_error_code(std::errc::not_enough_memory));
            std::terminate();
        }

        // Owns the executor the tree resumes on
        std::unique_ptr<coro_executor> m_executor_storage;
    };

    // Parameter handle: frame to own until release()
    explicit spawn_task(std::coroutine_handle<promise_type> handle) noexcept
        : m_handle(handle)
    {
    }

    // Parameter other: task to take over
    spawn_task(spawn_task&& other) noexcept
        : m_handle(std::exchange(other.m_handle, nullptr))
    {
    }

    spawn_task(const spawn_task&) = delete;
    spawn_task& operator=(const spawn_task&) = delete;
    spawn_task& operator=(spawn_task&&) = delete;

    // Destroy a frame that was never started
    ~spawn_task()
    {
        if (m_handle)
        {
            m_handle.destroy();
        }
    }

    // Give up ownership; the frame then owns itself
    // Return value: the frame
    std::coroutine_handle<promise_type> release() noexcept
    {
        return std::exchange(m_handle, nullptr);
    }

private:
    // Owned frame, or null after release()
    std::coroutine_handle<promise_type> m_handle;
};

// Posted task that delivers the end of a co_spawn coroutine to its handler
template <typename Handler, typename T>
class spawn_completion
{
public:
    // Parameter handler: completion handler of co_spawn
    // Parameter error:   exception that ended the coroutine, or null
    // Parameter value:   its value; empty when it threw
    spawn_completion(Handler&& handler, std::exception_ptr error, std::optional<T>&& value)
        : m_handler(std::move(handler))
        , m_error(std::move(error))
        , m_value(std::move(value))
    {
    }

    // Call the handler; a value-initialized T stands in when it threw
    void operator()()
    {
        if (m_value.has_value())
        {
            m_handler(m_error, std::move(*m_value));
        }
        else
        {
            m_handler(m_error, T());
        }
    }

private:
    // Completion handler of co_spawn
    Handler m_handler;
    // Exception that ended the coroutine, or null
    std::exception_ptr m_error;
    // Its value, or empty
    std::optional<T> m_value;
};

// Posted task that delivers the end of a co_spawn coroutine returning void
template <typename Handler>
class spawn_completion<Handler, void>
{
public:
    // Parameter handler: completion handler of co_spawn
    // Parameter error:   exception that ended the coroutine, or null
    spawn_completion(Handler&& handler, std::exception_ptr error)
        : m_handler(std::move(handler))
        , m_error(std::move(error))
    {
    }

    // Call the handler
    void operator()()
    {
        m_handler(m_error);
    }

private:
    // Completion handler of co_spawn
    Handler m_handler;
    // Exception that ended the coroutine, or null
    std::exception_ptr m_error;
};

// Body of the co_spawn frame for a coroutine returning T: awaits it, catches
// its exception and posts the completion, so a throwing completion handler
// propagates out of io_context::run() like any other handler
// Parameter executor: executor the completion is posted to
// Parameter body:     the user's coroutine
// Parameter handler:  completion handler of co_spawn
template <typename Executor, typename T, typename Handler>
spawn_task spawn_main(Executor executor, awaitable<T> body, Handler handler)
{
    std::exception_ptr error;
    std::optional<T> value;
    try
    {
        value.emplace(co_await std::move(body));
    }
    catch (...)
    {
        error = std::current_exception();
    }
    executor.post(spawn_completion<Handler, T>(std::move(handler), std::move(error), std::move(value)));
}

// Body of the co_spawn frame for a coroutine returning void (see above)
// Parameter executor: executor the completion is posted to
// Parameter body:     the user's coroutine
// Parameter handler:  completion handler of co_spawn
template <typename Executor, typename Handler>
spawn_task spawn_main(Executor executor, awaitable<void> body, Handler handler)
{
    std::exception_ptr error;
    try
    {
        co_await std::move(body);
    }
    catch (...)
    {
        error = std::current_exception();
    }
    executor.post(spawn_completion<Handler, void>(std::move(handler), std::move(error)));
}

// Create the co_spawn frame and queue its first resumption on the executor
// Parameter executor: io_context executor or strand to run on
// Parameter body:     the user's coroutine
// Parameter handler:  completion handler of co_spawn
template <typename Executor, typename T, typename Handler>
void start_spawn(const Executor& executor, awaitable<T> body, Handler&& handler)
{
    using handler_type = typename std::decay<Handler>::type;
    // Deduction picks the overload for awaitable<void> when T is void
    spawn_task task = spawn_main(executor, std::move(body), handler_type(std::forward<Handler>(handler)));
    const std::coroutine_handle<spawn_task::promise_type> frame = task.release();
    spawn_task::promise_type& promise = frame.promise();
    promise.m_executor_storage.reset(new coro_executor_impl<Executor>(executor));
    promise.m_executor = promise.m_executor_storage.get();
    promise.m_owner = frame;
    promise.m_executor->post(coro_resumer(frame, frame));
}

// Initiation of co_spawn: holds the coroutine until the token supplies the
// handler
template <typename Executor, typename T>
class spawn_initiation
{
public:
    // Parameter executor: where the coroutine runs
    // Parameter body:     the user's coroutine
    spawn_initiation(const Executor& executor, awaitable<T>&& body)
        : m_executor(executor)
        , m_body(std::move(body))
    {
    }

    // Parameter handler: completion handler of co_spawn
    template <typename Handler>
    void operator()(Handler&& handler)
    {
        start_spawn(m_executor, std::move(m_body), std::forward<Handler>(handler));
    }

private:
    // Where the coroutine runs
    Executor m_executor;
    // The user's coroutine, moved into the co_spawn frame when started
    awaitable<T> m_body;
};

}}

namespace spanio {

// Start a coroutine on an executor (io_context::executor_type or a strand)
// Parameter executor: where the coroutine and its resumptions run
// Parameter body:     the coroutine, e.g. session(std::move(sock))
// Parameter token:    completion token for void(std::exception_ptr, T), or
//                     void(std::exception_ptr) when T is void; spanio::detached
//                     to ignore the end
// Return value: void for a callback or detached, otherwise what the token
//               produces (e.g. an awaitable<T> for use_awaitable)
template <typename Executor, typename T, typename Token>
auto co_spawn(const Executor& executor, awaitable<T> body, Token&& token)
    -> async_return_t<Token, typename detail::spawn_signature<T>::type>
{
    using signature = typename detail::spawn_signature<T>::type;
    return async_initiate<signature>(detail::spawn_initiation<Executor, T>(executor, std::move(body)),
                                     std::forward<Token>(token));
}

// Start a coroutine on an io_context (see the overload above)
// Parameter context: context whose run() threads run the coroutine
// Parameter body:    the coroutine
// Parameter token:   completion token
// Return value: as for the executor overload
template <typename T, typename Token>
auto co_spawn(io_context& context, awaitable<T> body, Token&& token)
    -> async_return_t<Token, typename detail::spawn_signature<T>::type>
{
    return co_spawn(context.get_executor(), std::move(body), std::forward<Token>(token));
}

}

#endif // SPANIO_COROUTINE_H
