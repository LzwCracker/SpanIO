// ============================================================================
// File purpose: Host and service name resolution, and connecting to the
//               first reachable result:
//               - tcp::resolver / udp::resolver: turn "example.com" + "80"
//                 into endpoints, synchronously or asynchronously
//               - tcp::async_connect: try a list of endpoints in order until
//                 one connects
// Usage:        spanio::tcp::resolver resolver(ctx);
//               auto on_resolve = [&](std::error_code ec,
//                                     std::vector<spanio::tcp::endpoint> endpoints)
//               {
//                   if (!ec)
//                   {
//                       spanio::tcp::async_connect(sock, std::move(endpoints), on_connect);
//                   }
//               };
//               resolver.async_resolve("example.com", "80", on_resolve);
//
//               How it works, on every platform: each resolver has a worker
//               thread, started on first use, that calls the blocking
//               getaddrinfo() and posts the result to the io_context. One
//               resolver handles its requests one at a time; use several
//               resolvers to resolve in parallel.
//
//               Rules:
//               - Handlers run inside io_context::run(), never inline, and
//                 exactly once. A pending request counts as work.
//               - Results keep the order the system returned (it sorts them
//                 by preference, RFC 6724), without duplicates.
//               - Errors are spanio::error::netdb_errors on every platform:
//                 host_not_found, host_not_found_try_again, no_data,
//                 no_recovery, service_not_found.
//               - cancel() completes pending requests with
//                 errc::operation_canceled at once. A lookup the system has
//                 already started cannot be interrupted: its result is
//                 dropped when it arrives. For the same reason the
//                 destructor waits for that lookup, which may take seconds
//                 when DNS is slow.
//               - The io_context must outlive the resolver.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-08
// ============================================================================

#ifndef SPANIO_RESOLVER_H
#define SPANIO_RESOLVER_H

#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/tcp.h"
#include "spanio/udp.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/socket_ops.h"

#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace spanio {

// Which address families a resolution may return
enum class resolve_family
{
    // IPv4 and IPv6
    any,
    // IPv4 only
    v4,
    // IPv6 only
    v6
};

// How a name is resolved
struct resolve_options
{
    // Address families to return
    resolve_family m_family = resolve_family::any;
    // The host must be a numeric address; no DNS query is made
    bool m_numeric_host = false;
    // The service must be a port number; the services file is not read
    bool m_numeric_service = false;
};

}

namespace spanio { namespace detail {

// Signature of the function that does the actual resolution: a blocking
// call that returns the endpoints or sets ec. Tests replace it to simulate
// slow lookups.
// Parameter host:        host name or numeric address; may be empty
// Parameter service:     service name or port number; may be empty
// Parameter socket_type: SOCK_STREAM or SOCK_DGRAM
// Parameter options:     families and numeric-only flags
// Parameter ec:          set to the failure reason, or cleared
// Return value: the endpoints
using resolve_function = std::function<std::vector<ip::endpoint>(
    const std::string& host, const std::string& service, int socket_type, const resolve_options& options,
    std::error_code& ec)>;

// getaddrinfo() result meaning success
constexpr int kGetaddrinfoSuccess = 0;

// Map a getaddrinfo() result to an error code. Windows defines the EAI_
// names as Winsock errors, and some of them share a value there, so this
// uses comparisons instead of a switch.
// Parameter result: non-zero value returned by getaddrinfo()
// Return value: the matching netdb error, or a system / generic error
inline std::error_code getaddrinfo_error(int result) noexcept
{
    if (result == EAI_NONAME)
    {
        return make_error_code(error::host_not_found);
    }
    if (result == EAI_AGAIN)
    {
        return make_error_code(error::host_not_found_try_again);
    }
    if (result == EAI_FAIL)
    {
        return make_error_code(error::no_recovery);
    }
    if (result == EAI_SERVICE)
    {
        return make_error_code(error::service_not_found);
    }
#if defined(EAI_NODATA)
    if (result == EAI_NODATA)
    {
        return make_error_code(error::no_data);
    }
#endif
#if defined(EAI_ADDRFAMILY)
    if (result == EAI_ADDRFAMILY)
    {
        return make_error_code(error::no_data);
    }
#endif
    if (result == EAI_FAMILY || result == EAI_SOCKTYPE)
    {
        return std::make_error_code(std::errc::address_family_not_supported);
    }
    if (result == EAI_MEMORY)
    {
        return std::make_error_code(std::errc::not_enough_memory);
    }
#if defined(EAI_SYSTEM)
    if (result == EAI_SYSTEM)
    {
        return std::error_code(errno, std::system_category());
    }
#endif
    return make_error_code(error::no_recovery);
}

// Resolve with the system's getaddrinfo(); the default resolve_function
// Parameter host:        host name or numeric address; empty means none
// Parameter service:     service name or port number; empty means none
// Parameter socket_type: SOCK_STREAM or SOCK_DGRAM
// Parameter options:     families and numeric-only flags
// Parameter ec:          set to the failure reason, or cleared
// Return value: the endpoints, in the system's order, without duplicates
inline std::vector<ip::endpoint> system_resolve(const std::string& host, const std::string& service,
                                                int socket_type, const resolve_options& options,
                                                std::error_code& ec)
{
    std::vector<ip::endpoint> endpoints;
    ec = ensure_network_initialized();
    if (ec)
    {
        return endpoints;
    }
    addrinfo hints{};
    hints.ai_socktype = socket_type;
    hints.ai_family = options.m_family == resolve_family::v4 ? AF_INET
                    : options.m_family == resolve_family::v6 ? AF_INET6
                                                             : AF_UNSPEC;
    if (options.m_numeric_host)
    {
        hints.ai_flags |= AI_NUMERICHOST;
    }
    if (options.m_numeric_service)
    {
        hints.ai_flags |= AI_NUMERICSERV;
    }

    addrinfo* list = nullptr;
    const int result = ::getaddrinfo(host.empty() ? nullptr : host.c_str(),
                                     service.empty() ? nullptr : service.c_str(), &hints, &list);
    if (result != kGetaddrinfoSuccess)
    {
        ec = getaddrinfo_error(result);
        return endpoints;
    }
    for (const addrinfo* entry = list; entry != nullptr; entry = entry->ai_next)
    {
        if (entry->ai_family != AF_INET && entry->ai_family != AF_INET6)
        {
            continue;
        }
        const ip::endpoint found = ip::endpoint::from_native(entry->ai_addr,
                                                             static_cast<socklen_type>(entry->ai_addrlen));
        bool duplicate = false;
        for (const ip::endpoint& known : endpoints)
        {
            duplicate = duplicate || known == found;
        }
        if (!duplicate)
        {
            endpoints.push_back(found);
        }
    }
    ::freeaddrinfo(list);
    if (endpoints.empty())
    {
        ec = make_error_code(error::no_data);
    }
    return endpoints;
}

// One pending asynchronous request. It keeps its io_context from running out
// of work until its result has been posted.
class resolve_op_base
{
public:
    // Parameter context: context that runs the handler
    // Parameter host:    copied host name
    // Parameter service: copied service name
    // Parameter options: resolution options
    resolve_op_base(io_context& context, std::string host, std::string service, const resolve_options& options)
        : m_context(&context)
        , m_work(context)
        , m_host(std::move(host))
        , m_service(std::move(service))
        , m_options(options)
    {
    }

    virtual ~resolve_op_base() = default;

    resolve_op_base(const resolve_op_base&) = delete;
    resolve_op_base& operator=(const resolve_op_base&) = delete;

    // Post the handler with a result; called exactly once
    // Parameter ec:        result
    // Parameter endpoints: endpoints found
    virtual void post_result(const std::error_code& ec, std::vector<ip::endpoint> endpoints) = 0;

    // Context that runs the handler
    io_context* m_context;
    // Keeps run() from returning while the request is pending
    executor_work_guard m_work;
    // Host name to resolve
    std::string m_host;
    // Service name to resolve
    std::string m_service;
    // Resolution options
    resolve_options m_options;
};

// Task that calls a resolve handler with a stored result
template <typename Handler>
class resolve_binder
{
public:
    // Parameter handler:   function object taking (std::error_code, std::vector<ip::endpoint>)
    // Parameter ec:        result
    // Parameter endpoints: endpoints found
    resolve_binder(Handler handler, const std::error_code& ec, std::vector<ip::endpoint> endpoints)
        : m_handler(std::move(handler))
        , m_ec(ec)
        , m_endpoints(std::move(endpoints))
    {
    }

    // Call the handler
    void operator()()
    {
        m_handler(m_ec, std::move(m_endpoints));
    }

private:
    // User handler
    Handler m_handler;
    // Result
    std::error_code m_ec;
    // Endpoints found
    std::vector<ip::endpoint> m_endpoints;
};

// Pending request whose result goes to Handler
template <typename Handler>
class resolve_op : public resolve_op_base
{
public:
    // Parameter context: context that runs the handler
    // Parameter host:    copied host name
    // Parameter service: copied service name
    // Parameter options: resolution options
    // Parameter handler: function object taking (std::error_code, std::vector<ip::endpoint>)
    template <typename H>
    resolve_op(io_context& context, std::string host, std::string service, const resolve_options& options,
               H&& handler)
        : resolve_op_base(context, std::move(host), std::move(service), options)
        , m_handler(std::forward<H>(handler))
    {
    }

    // See resolve_op_base::post_result
    // Parameter ec:        result
    // Parameter endpoints: endpoints found
    void post_result(const std::error_code& ec, std::vector<ip::endpoint> endpoints) override
    {
        m_context->post(resolve_binder<Handler>(std::move(m_handler), ec, std::move(endpoints)));
    }

private:
    // User handler
    Handler m_handler;
};

// The worker thread of a resolver and its queue. Only the resolver and the
// worker touch it, under m_mutex.
class resolver_worker
{
public:
    // Parameter resolve:     function doing the actual resolution
    // Parameter socket_type: SOCK_STREAM or SOCK_DGRAM
    resolver_worker(resolve_function resolve, int socket_type)
        : m_resolve(std::move(resolve))
        , m_socket_type(socket_type)
        , m_stopping(false)
    {
    }

    // Cancel pending requests and wait for the worker thread
    ~resolver_worker()
    {
        cancel();
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stopping = true;
        }
        m_wake.notify_one();
        if (m_thread.joinable())
        {
            m_thread.join();
        }
    }

    resolver_worker(const resolver_worker&) = delete;
    resolver_worker& operator=(const resolver_worker&) = delete;

    // Queue a request, starting the thread on first use
    // Parameter op: the request
    void submit(std::unique_ptr<resolve_op_base> op)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push_back(std::move(op));
        if (!m_thread.joinable())
        {
            auto work_loop = [this]()
            {
                run();
            };
            m_thread = std::thread(work_loop);
        }
        m_wake.notify_one();
    }

    // Complete every pending request with operation_canceled
    void cancel()
    {
        std::deque<std::unique_ptr<resolve_op_base>> cancelled;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            cancelled.swap(m_queue);
            if (m_current != nullptr)
            {
                // The worker drops the result of this one when it arrives
                cancelled.push_back(std::move(m_current));
            }
        }
        const std::error_code ec = std::make_error_code(std::errc::operation_canceled);
        for (std::unique_ptr<resolve_op_base>& op : cancelled)
        {
            op->post_result(ec, std::vector<ip::endpoint>());
        }
    }

    // Resolve on the calling thread
    // Parameter host:    host name
    // Parameter service: service name
    // Parameter options: resolution options
    // Parameter ec:      set to the failure reason, or cleared
    // Return value: the endpoints
    std::vector<ip::endpoint> resolve_now(const std::string& host, const std::string& service,
                                          const resolve_options& options, std::error_code& ec) const
    {
        return m_resolve(host, service, m_socket_type, options, ec);
    }

private:
    // Body of the worker thread: take requests one at a time until stopped
    void run()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (true)
        {
            auto has_work = [this]()
            {
                return m_stopping || !m_queue.empty();
            };
            m_wake.wait(lock, has_work);
            if (m_queue.empty())
            {
                return;
            }
            m_current = std::move(m_queue.front());
            m_queue.pop_front();
            const std::string host = m_current->m_host;
            const std::string service = m_current->m_service;
            const resolve_options options = m_current->m_options;
            lock.unlock();

            std::error_code ec;
            std::vector<ip::endpoint> endpoints = m_resolve(host, service, m_socket_type, options, ec);

            lock.lock();
            // Null if cancel() already answered this request
            std::unique_ptr<resolve_op_base> done = std::move(m_current);
            if (done != nullptr)
            {
                lock.unlock();
                done->post_result(ec, std::move(endpoints));
                done.reset();
                lock.lock();
            }
        }
    }

    // Function doing the actual resolution
    resolve_function m_resolve;
    // SOCK_STREAM or SOCK_DGRAM, for service lookups
    int m_socket_type;
    // Guards the fields below
    std::mutex m_mutex;
    // Signals the worker that work arrived or that it should stop
    std::condition_variable m_wake;
    // Requests not started yet
    std::deque<std::unique_ptr<resolve_op_base>> m_queue;
    // Request the worker is resolving; null when idle or cancelled
    std::unique_ptr<resolve_op_base> m_current;
    // The resolver is being destroyed
    bool m_stopping;
    // Worker thread; started on first submit()
    std::thread m_thread;
};

}}

namespace spanio {

// Resolves host and service names for one socket type. Use it as
// tcp::resolver or udp::resolver. Not copyable or movable: its worker thread
// refers to it.
template <int SocketType>
class basic_resolver
{
public:
    // Endpoint type the results use
    using endpoint = ip::endpoint;

    // Parameter context: context that runs the handlers; must outlive it
    explicit basic_resolver(io_context& context)
        : basic_resolver(context, &detail::system_resolve)
    {
    }

    // Use a given resolution function instead of getaddrinfo(); for tests
    // Parameter context: context that runs the handlers; must outlive it
    // Parameter resolve: blocking resolution function
    basic_resolver(io_context& context, detail::resolve_function resolve)
        : m_context(&context)
        , m_worker(new detail::resolver_worker(std::move(resolve), SocketType))
    {
    }

    basic_resolver(const basic_resolver&) = delete;
    basic_resolver& operator=(const basic_resolver&) = delete;

    // Resolve on the calling thread; blocks until the system answers
    // Parameter host:    host name or numeric address; may be empty
    // Parameter service: service name or port number; may be empty
    // Parameter ec:      set to the failure reason, or cleared
    // Parameter options: families and numeric-only flags
    // Return value: the endpoints
    std::vector<endpoint> resolve(std::string_view host, std::string_view service, std::error_code& ec,
                                  const resolve_options& options = resolve_options())
    {
        const std::vector<endpoint> endpoints =
            m_worker->resolve_now(std::string(host), std::string(service), options, ec);
        if (ec)
        {
            detail::log_error("resolver::resolve", ec);
        }
        return endpoints;
    }

    // Resolve on the worker thread
    // Parameter host:    host name or numeric address; may be empty
    // Parameter service: service name or port number; may be empty
    // Parameter handler: called as handler(std::error_code, std::vector<endpoint>)
    // Parameter options: families and numeric-only flags
    template <typename Handler>
    void async_resolve(std::string_view host, std::string_view service, Handler&& handler,
                       const resolve_options& options = resolve_options())
    {
        using handler_type = typename std::decay<Handler>::type;
        std::unique_ptr<detail::resolve_op_base> op(new detail::resolve_op<handler_type>(
            *m_context, std::string(host), std::string(service), options, std::forward<Handler>(handler)));
        m_worker->submit(std::move(op));
    }

    // Complete pending requests with errc::operation_canceled
    void cancel()
    {
        m_worker->cancel();
    }

private:
    // Context that runs the handlers
    io_context* m_context;
    // Worker thread and queue
    std::unique_ptr<detail::resolver_worker> m_worker;
};

}

namespace spanio { namespace tcp {

// Resolver for TCP endpoints
using resolver = basic_resolver<SOCK_STREAM>;

}}

namespace spanio { namespace udp {

// Resolver for UDP endpoints
using resolver = basic_resolver<SOCK_DGRAM>;

}}

namespace spanio { namespace detail {

// Task that reports the end of a connect sequence without I/O
template <typename Handler>
class connect_result_binder
{
public:
    // Parameter handler: function object taking (std::error_code, const tcp::endpoint&)
    // Parameter ec:      result
    template <typename H>
    connect_result_binder(H&& handler, const std::error_code& ec)
        : m_handler(std::forward<H>(handler))
        , m_ec(ec)
    {
    }

    // Call the handler
    void operator()()
    {
        m_handler(m_ec, tcp::endpoint());
    }

private:
    // User handler
    Handler m_handler;
    // Result
    std::error_code m_ec;
};

// Composed operation behind tcp::async_connect: connects to each endpoint in
// turn until one succeeds. The object moves from one attempt to the next as
// the completion of each connect.
template <typename Handler>
class connect_sequence_op
{
public:
    // Parameter sock:      socket to connect; closed and reopened as needed
    // Parameter endpoints: candidates, in order
    // Parameter handler:   function object taking (std::error_code, const tcp::endpoint&)
    template <typename H>
    connect_sequence_op(tcp::socket& sock, std::vector<tcp::endpoint> endpoints, H&& handler)
        : m_socket(&sock)
        , m_endpoints(std::move(endpoints))
        , m_index(0)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Try the next endpoint, or report the last error when none is left.
    // This object is moved into the connect, so it must not be used
    // afterwards.
    void start()
    {
        if (m_index >= m_endpoints.size())
        {
            const std::error_code ec = m_last_error ? m_last_error : make_error_code(error::host_not_found);
            m_socket->context().post(connect_result_binder<Handler>(std::move(m_handler), ec));
            return;
        }
        // A failed connect leaves the socket unusable, and the next endpoint
        // may be of another family: start from a fresh socket
        m_socket->close();
        tcp::socket& sock = *m_socket;
        const tcp::endpoint target = m_endpoints[m_index];
        sock.async_connect(target, std::move(*this));
    }

    // Completion of one connect
    // Parameter ec: result
    void operator()(std::error_code ec)
    {
        if (!ec)
        {
            m_handler(ec, m_endpoints[m_index]);
            return;
        }
        m_last_error = ec;
        ++m_index;
        start();
    }

private:
    // Socket being connected
    tcp::socket* m_socket;
    // Candidates, in order
    std::vector<tcp::endpoint> m_endpoints;
    // Candidate being tried
    std::size_t m_index;
    // Error of the last failed attempt
    std::error_code m_last_error;
    // User handler
    Handler m_handler;
};

}}

namespace spanio { namespace tcp {

// Connect a socket to the first endpoint of a list that accepts. Handlers
// run inside io_context::run(), never inline.
// Parameter sock:      socket to connect; it is closed and reopened for each
//                      attempt, and must outlive the operation
// Parameter endpoints: candidates in order, e.g. a resolver's result
// Parameter handler:   called as handler(std::error_code, const tcp::endpoint&
//                      connected). On failure the error is that of the last
//                      attempt, or error::host_not_found for an empty list.
template <typename Handler>
void async_connect(socket& sock, std::vector<endpoint> endpoints, Handler&& handler)
{
    using handler_type = typename std::decay<Handler>::type;
    detail::connect_sequence_op<handler_type> op(sock, std::move(endpoints), std::forward<Handler>(handler));
    op.start();
}

}}

#endif // SPANIO_RESOLVER_H
