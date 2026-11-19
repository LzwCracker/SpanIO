// ============================================================================
// File purpose: Completion tokens (design doc 6.3): the last argument of every
//               async_* function decides how its result is delivered.
//               - a function object (callback): called with the result, as
//                 before; the async_* function returns void
//               - spanio::use_future: the async_* function returns a
//                 std::future of the result
//               - spanio::as_tuple(token): the result is packed into one
//                 std::tuple (error code included) and delivered through the
//                 inner token, so errors are values, never exceptions
//               - spanio::detached: the result is dropped
//               - spanio::use_awaitable (spanio/coroutine.h, C++20): the
//                 async_* function returns an awaitable for co_await
//               All of them sit on the same completion events; only the
//               adaptation of the handler differs.
//
//               How an async_* function supports tokens:
//                   template <typename Token>
//                   auto async_x(args, Token&& token)
//                       -> async_return_t<Token, void(std::error_code, std::size_t)>
//                   {
//                       auto initiation = [...](auto&& handler) { start_x(args, handler); };
//                       return async_initiate<void(std::error_code, std::size_t)>(
//                           std::move(initiation), std::forward<Token>(token));
//                   }
//               The initiation is called exactly once with the final handler.
//               A new token type specializes async_result<Token, Signature>.
//
//               Value of a result for use_future / use_awaitable, from the
//               completion signature:
//               - void(std::error_code, Ts...): a non-empty error code is
//                 thrown as std::system_error; the value is Ts...
//               - void(std::exception_ptr, Ts...): a non-null pointer is
//                 rethrown; the value is Ts...
//               - any other void(Ts...): the value is Ts...
//               where "Ts..." means void for none, T for one, and
//               std::tuple<Ts...> for several.
// Usage:        std::future<std::size_t> f = sock.async_read_some(buf, spanio::use_future);
//               auto on_result = [](std::tuple<std::error_code, std::size_t> result) { ... };
//               sock.async_read_some(buf, spanio::as_tuple(on_result));
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19
// ============================================================================

#ifndef SPANIO_ASYNC_RESULT_H
#define SPANIO_ASYNC_RESULT_H

#include "spanio/detail/platform.h"

#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

namespace spanio {

// How the result of an asynchronous operation is delivered for a given
// token type. The primary template treats the token as a callback: the
// initiation receives it as the handler and the async_* function returns
// void. Specialize it for new token types.
// Template parameter Token:     decayed token type
// Template parameter Signature: completion signature, e.g.
//                               void(std::error_code, std::size_t)
template <typename Token, typename Signature>
class async_result
{
public:
    // What the async_* function returns for this token
    using return_type = void;

    // Start the operation with the token itself as the handler
    // Parameter initiation: function object taking the handler; called once
    // Parameter token:      the callback
    template <typename Initiation, typename RawToken>
    static return_type initiate(Initiation&& initiation, RawToken&& token)
    {
        std::forward<Initiation>(initiation)(std::forward<RawToken>(token));
    }
};

// Return type of an async_* function called with a given token
template <typename Token, typename Signature>
using async_return_t = typename async_result<typename std::decay<Token>::type, Signature>::return_type;

// Start an asynchronous operation through the token's async_result
// Template parameter Signature: completion signature of the operation
// Parameter initiation: function object taking the final handler; called once
// Parameter token:      completion token passed to the async_* function
// Return value: whatever the token's async_result returns (void for callbacks)
template <typename Signature, typename Initiation, typename Token>
async_return_t<Token, Signature> async_initiate(Initiation&& initiation, Token&& token)
{
    return async_result<typename std::decay<Token>::type, Signature>::initiate(
        std::forward<Initiation>(initiation), std::forward<Token>(token));
}

}

namespace spanio { namespace detail {

// Collapses the value part of a completion into one type: void for no
// values, T for one, std::tuple<Ts...> for several
template <typename... Ts>
struct completion_value_of
{
    // Several values: a tuple
    using type = std::tuple<Ts...>;
};

// One value: the value itself
template <typename T>
struct completion_value_of<T>
{
    // The single value
    using type = T;
};

// No value: void
template <>
struct completion_value_of<>
{
    // Nothing
    using type = void;
};

// Builds the collapsed value from the arguments of a completion
template <typename Value>
struct completion_value_maker
{
    // Parameter args: the value arguments of the completion
    // Return value: Value built from them (a tuple from several)
    template <typename... Args>
    static Value make(Args&&... args)
    {
        return Value(std::forward<Args>(args)...);
    }
};

// Turns the arguments of a completion into a value or an error and hands
// them to a target with set_value(Value) / set_value() and
// set_error(std::exception_ptr). This general form has no error argument.
template <typename Signature>
struct completion_traits;

// Completion without an error argument: every argument is part of the value
template <typename... Args>
struct completion_traits<void(Args...)>
{
    // Collapsed value type
    using value_type = typename completion_value_of<typename std::decay<Args>::type...>::type;

    // Deliver a completion to a target
    // Parameter target: receives the value
    // Parameter args:   the completion's arguments
    template <typename Target, typename... CallArgs>
    static void store(Target& target, CallArgs&&... args)
    {
        set_value(target, std::forward<CallArgs>(args)...);
    }

    // Hand the value arguments to a target, or nothing for a void value
    // Parameter target: receives the value
    // Parameter args:   the value arguments
    template <typename Target, typename... CallArgs>
    static void set_value(Target& target, CallArgs&&... args)
    {
        if constexpr (std::is_void<value_type>::value)
        {
            target.set_value();
        }
        else
        {
            target.set_value(completion_value_maker<value_type>::make(std::forward<CallArgs>(args)...));
        }
    }
};

// Completion whose first argument is an error code: a non-empty code becomes
// a std::system_error, the other arguments are the value
template <typename... Args>
struct completion_traits<void(std::error_code, Args...)>
{
    // Collapsed value type, without the error code
    using value_type = typename completion_value_of<typename std::decay<Args>::type...>::type;

    // Deliver a completion to a target
    // Parameter target: receives the value or the error
    // Parameter ec:     error of the operation
    // Parameter args:   the other arguments
    template <typename Target, typename... CallArgs>
    static void store(Target& target, const std::error_code& ec, CallArgs&&... args)
    {
        if (ec)
        {
            target.set_error(std::make_exception_ptr(std::system_error(ec)));
            return;
        }
        completion_traits<void(Args...)>::set_value(target, std::forward<CallArgs>(args)...);
    }
};

// Completion whose first argument is an exception pointer (co_spawn): a
// non-null pointer is rethrown, the other arguments are the value
template <typename... Args>
struct completion_traits<void(std::exception_ptr, Args...)>
{
    // Collapsed value type, without the exception pointer
    using value_type = typename completion_value_of<typename std::decay<Args>::type...>::type;

    // Deliver a completion to a target
    // Parameter target: receives the value or the exception
    // Parameter error:  exception of the operation, or null
    // Parameter args:   the other arguments
    template <typename Target, typename... CallArgs>
    static void store(Target& target, const std::exception_ptr& error, CallArgs&&... args)
    {
        if (error)
        {
            target.set_error(error);
            return;
        }
        completion_traits<void(Args...)>::set_value(target, std::forward<CallArgs>(args)...);
    }
};

// Completion target that fulfils a std::promise
template <typename Value>
class promise_target
{
public:
    // Parameter promise: promise to fulfil; must outlive the target
    explicit promise_target(std::promise<Value>& promise) noexcept
        : m_promise(promise)
    {
    }

    // Parameter value: the result
    void set_value(Value&& value)
    {
        m_promise.set_value(std::move(value));
    }

    // Parameter error: the failure
    void set_error(const std::exception_ptr& error)
    {
        m_promise.set_exception(error);
    }

private:
    // Promise behind the caller's future
    std::promise<Value>& m_promise;
};

// Completion target that fulfils a std::promise<void>
template <>
class promise_target<void>
{
public:
    // Parameter promise: promise to fulfil; must outlive the target
    explicit promise_target(std::promise<void>& promise) noexcept
        : m_promise(promise)
    {
    }

    // Report success
    void set_value()
    {
        m_promise.set_value();
    }

    // Parameter error: the failure
    void set_error(const std::exception_ptr& error)
    {
        m_promise.set_exception(error);
    }

private:
    // Promise behind the caller's future
    std::promise<void>& m_promise;
};

// Handler created for use_future: fulfils the promise behind the future
// returned to the caller. Destroyed without being called (its io_context
// was destroyed first), it leaves the future with std::future_errc::broken_promise.
template <typename Signature>
class future_handler
{
public:
    // Value the future carries
    using value_type = typename completion_traits<Signature>::value_type;

    // Parameter promise: shared with nobody else; the caller holds its future
    explicit future_handler(std::shared_ptr<std::promise<value_type>> promise) noexcept
        : m_promise(std::move(promise))
    {
    }

    // Deliver the completion to the future
    // Parameter args: the completion's arguments
    template <typename... Args>
    void operator()(Args&&... args)
    {
        promise_target<value_type> target(*m_promise);
        completion_traits<Signature>::store(target, std::forward<Args>(args)...);
    }

private:
    // Promise of the caller's future; shared so the handler stays copyable
    std::shared_ptr<std::promise<value_type>> m_promise;
};

// Handler created for as_tuple: packs the completion's arguments into one
// tuple and passes it to the inner handler
template <typename Handler, typename Signature>
class as_tuple_handler;

// Specialization that knows the completion's argument types
template <typename Handler, typename... Args>
class as_tuple_handler<Handler, void(Args...)>
{
public:
    // Tuple delivered to the inner handler
    using tuple_type = std::tuple<typename std::decay<Args>::type...>;

    // Parameter handler: inner handler taking one tuple_type
    template <typename H>
    explicit as_tuple_handler(H&& handler)
        : m_handler(std::forward<H>(handler))
    {
    }

    // Pack the completion and pass it on
    // Parameter args: the completion's arguments
    template <typename... CallArgs>
    void operator()(CallArgs&&... args)
    {
        m_handler(tuple_type(std::forward<CallArgs>(args)...));
    }

private:
    // Handler produced by the inner token
    Handler m_handler;
};

// Initiation used by as_tuple: wraps the handler produced by the inner token
// in an as_tuple_handler before calling the original initiation
template <typename Initiation, typename Signature>
class as_tuple_initiation
{
public:
    // Parameter initiation: the async_* function's own initiation
    template <typename I>
    explicit as_tuple_initiation(I&& initiation)
        : m_initiation(std::forward<I>(initiation))
    {
    }

    // Parameter handler: handler from the inner token, taking one tuple
    template <typename Handler>
    void operator()(Handler&& handler)
    {
        using wrapped_type = as_tuple_handler<typename std::decay<Handler>::type, Signature>;
        m_initiation(wrapped_type(std::forward<Handler>(handler)));
    }

private:
    // The async_* function's own initiation
    Initiation m_initiation;
};

}}

namespace spanio {

// Token type of spanio::use_future
class use_future_t
{
public:
    constexpr use_future_t() noexcept = default;
};

// Token: the async_* function returns a std::future of the result. An error
// code or exception becomes the future's exception (std::system_error).
inline constexpr use_future_t use_future{};

// use_future: the operation runs as usual and its result fulfils a promise
template <typename Signature>
class async_result<use_future_t, Signature>
{
public:
    // Value the future carries (see the file header)
    using value_type = typename detail::completion_traits<Signature>::value_type;

    // A future of the result
    using return_type = std::future<value_type>;

    // Start the operation with a promise-fulfilling handler
    // Parameter initiation: function object taking the handler; called once
    // Return value: future of the result
    template <typename Initiation, typename RawToken>
    static return_type initiate(Initiation&& initiation, RawToken&&)
    {
        std::shared_ptr<std::promise<value_type>> promise = std::make_shared<std::promise<value_type>>();
        return_type future = promise->get_future();
        std::forward<Initiation>(initiation)(detail::future_handler<Signature>(std::move(promise)));
        return future;
    }
};

// Token returned by as_tuple(): delivers the whole completion, error code
// included, as one std::tuple through the inner token
template <typename Token>
class as_tuple_t
{
public:
    // Parameter token: inner token (callback, use_future, use_awaitable, ...)
    template <typename T>
    explicit as_tuple_t(T&& token)
        : m_token(std::forward<T>(token))
    {
    }

    // Inner token
    Token m_token;
};

// Wrap a token so the completion arrives as one tuple: with
// as_tuple(use_awaitable) or as_tuple(use_future) errors are values, not
// exceptions
// Parameter token: inner token
// Return value: the wrapping token
template <typename Token>
as_tuple_t<typename std::decay<Token>::type> as_tuple(Token&& token)
{
    return as_tuple_t<typename std::decay<Token>::type>(std::forward<Token>(token));
}

// as_tuple: forwards to the inner token with signature void(std::tuple<...>)
template <typename Token, typename... Args>
class async_result<as_tuple_t<Token>, void(Args...)>
{
public:
    // Signature seen by the inner token
    using inner_signature = void(std::tuple<typename std::decay<Args>::type...>);

    // Whatever the inner token returns
    using return_type = async_return_t<Token, inner_signature>;

    // Start the operation through the inner token
    // Parameter initiation: function object taking the handler; called once
    // Parameter token:      the as_tuple_t wrapper
    // Return value: the inner token's result
    template <typename Initiation, typename RawToken>
    static return_type initiate(Initiation&& initiation, RawToken&& token)
    {
        using wrapped_type = detail::as_tuple_initiation<typename std::decay<Initiation>::type, void(Args...)>;
        return async_initiate<inner_signature>(wrapped_type(std::forward<Initiation>(initiation)),
                                               std::forward<RawToken>(token).m_token);
    }
};

// Token that drops the result. Meant for co_spawn: an exception that ends a
// detached coroutine is logged (SPANIO_ENABLE_LOG) and otherwise ignored.
class detached_t
{
public:
    constexpr detached_t() noexcept = default;

    // Drop a completion, logging an exception it carries
    // Parameter args: the completion's arguments
    template <typename... Args>
    void operator()(Args&&... args) const noexcept
    {
        report(args...);
    }

private:
    // Log the exception that ended a detached coroutine
    // Parameter error: the exception, or null
    template <typename... Rest>
    static void report(const std::exception_ptr& error, const Rest&...) noexcept
    {
        if (error)
        {
            detail::log_error("detached operation ended with an exception",
                              std::make_error_code(std::errc::operation_canceled));
        }
    }

    // Completions without an exception pointer carry nothing to log
    // Parameter args: ignored
    template <typename... Rest>
    static void report(const Rest&...) noexcept
    {
    }
};

// Token: drop the result
inline constexpr detached_t detached{};

}

#endif // SPANIO_ASYNC_RESULT_H
