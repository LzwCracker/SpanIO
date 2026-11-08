// ============================================================================
// File purpose: Unit tests for tcp::resolver, udp::resolver and
//               tcp::async_connect (resolver.h). They use numeric addresses,
//               "localhost" and the services file, so they need no network;
//               only the unknown-host case makes a DNS query. The connect
//               cases run once per proactor backend of the platform.
// Usage:        Built as the test_resolver executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_resolver cancel`.
//
//               The cancel and destructor cases replace getaddrinfo() with a
//               function that blocks until the test releases it, so a lookup
//               is reliably in progress when they act.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-08
// ============================================================================

#include "spanio/resolver.h"
#include "test_framework.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace tcp = spanio::tcp;
namespace udp = spanio::udp;
using spanio::io_context;
using spanio::resolve_family;
using spanio::resolve_options;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;

namespace {

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Port used by the numeric cases ("8080")
constexpr std::uint16_t kSamplePort = 8080;

// Well-known port of the "http" service
constexpr std::uint16_t kHttpPort = 80;

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Requests in the many-requests case
constexpr int kManyRequests = 50;

// First port used by the many-requests case; request i asks for this + i
constexpr int kFirstRequestPort = 10000;

// Generous upper bound for anything that should finish soon; covers the
// ~2 s Windows takes to report a refused loopback connect, twice
constexpr std::chrono::seconds kGenerousLimit(20);

// Pause that makes sure the worker thread has started a blocked lookup
constexpr std::chrono::milliseconds kSettle(50);

// Time since a starting point
// Parameter start: time the measurement began
// Return value: the elapsed time
test_clock::duration elapsed(test_clock::time_point start)
{
    return test_clock::now() - start;
}

// Whether a list holds a given address
// Parameter endpoints: the list
// Parameter address:   address to look for
// Return value: true if some endpoint has that address
bool contains_address(const std::vector<tcp::endpoint>& endpoints, const spanio::ip::address& address)
{
    for (const tcp::endpoint& endpoint : endpoints)
    {
        if (endpoint.address() == address)
        {
            return true;
        }
    }
    return false;
}

// Whether a list holds the same endpoint twice
// Parameter endpoints: the list
// Return value: true on a duplicate
bool has_duplicates(const std::vector<tcp::endpoint>& endpoints)
{
    for (std::size_t i = 0; i < endpoints.size(); ++i)
    {
        for (std::size_t j = i + 1; j < endpoints.size(); ++j)
        {
            if (endpoints[i] == endpoints[j])
            {
                return true;
            }
        }
    }
    return false;
}

// Result of an asynchronous resolve, recorded by a test
struct resolve_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // Endpoints passed to the handler
    std::vector<tcp::endpoint> m_endpoints;
};

// Handler that records its result in a resolve_record
class record_resolve
{
public:
    // Parameter record: where to store the result; must outlive the handler
    explicit record_resolve(resolve_record& record)
        : m_record(&record)
    {
    }

    // Parameter ec:        result
    // Parameter endpoints: endpoints found
    void operator()(std::error_code ec, std::vector<tcp::endpoint> endpoints)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
        m_record->m_endpoints = std::move(endpoints);
    }

private:
    // Destination of the recorded values
    resolve_record* m_record;
};

// A resolution function that blocks until released, then answers with
// 127.0.0.1. It stands in for a slow DNS server.
class blocking_lookup
{
public:
    // The function to give to the resolver; it refers to this object, which
    // must outlive the resolver
    // Return value: the function
    spanio::detail::resolve_function function()
    {
        auto lookup = [this](const std::string&, const std::string&, int, const resolve_options&,
                             std::error_code& ec)
        {
            return wait_and_answer(ec);
        };
        return lookup;
    }

    // Let the blocked lookup finish
    void release()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_released = true;
        }
        m_signal.notify_all();
    }

    // Wait until a lookup has started
    // Return value: true if one started within the limit
    bool wait_started()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto started = [this]()
        {
            return m_started;
        };
        return m_signal.wait_for(lock, kGenerousLimit, started);
    }

    // Whether a lookup has returned
    // Return value: true once one has
    bool finished() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_finished;
    }

private:
    // Block until released, then return one loopback endpoint
    // Parameter ec: cleared
    // Return value: the endpoint list
    std::vector<spanio::ip::endpoint> wait_and_answer(std::error_code& ec)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_started = true;
        m_signal.notify_all();
        auto released = [this]()
        {
            return m_released;
        };
        m_signal.wait(lock, released);
        m_finished = true;
        ec.clear();
        return std::vector<spanio::ip::endpoint>{spanio::ip::endpoint(spanio::ipv4::loopback(), kSamplePort)};
    }

    // Guards the flags
    mutable std::mutex m_mutex;
    // Signals changes of the flags
    std::condition_variable m_signal;
    // A lookup is in progress
    bool m_started = false;
    // The test allowed the lookup to finish
    bool m_released = false;
    // The lookup returned
    bool m_finished = false;
};

// A listening acceptor's port, or fail the case
// Parameter acc: open acceptor
// Return value: its port
std::uint16_t listening_port(const tcp::acceptor& acc)
{
    std::error_code ec;
    const tcp::endpoint local = acc.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return local.port();
}

}

// Case 1: a numeric IPv4 address resolves to itself, synchronously and
// asynchronously
SPANIO_TEST(resolve_numeric_ipv4)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("127.0.0.1", "8080", ec);
    SPANIO_CHECK(!ec);
    SPANIO_REQUIRE(found.size() == 1);
    SPANIO_CHECK(found[0] == tcp::endpoint(spanio::ipv4::loopback(), kSamplePort));

    resolve_record async;
    resolver.async_resolve("127.0.0.1", "8080", record_resolve(async));
    SPANIO_CHECK(async.m_calls == 0);
    ctx.run();
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(!async.m_ec);
    SPANIO_CHECK(async.m_endpoints == found);
}

// Case 2: a numeric IPv6 address resolves to itself
SPANIO_TEST(resolve_numeric_ipv6)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("::1", "8080", ec);
    SPANIO_CHECK(!ec);
    SPANIO_REQUIRE(found.size() == 1);
    SPANIO_CHECK(found[0].family() == AF_INET6);
    SPANIO_CHECK(found[0] == tcp::endpoint(spanio::ipv6::loopback(), kSamplePort));
}

// Case 3: "localhost" resolves to a loopback address, with the port, and
// without duplicates
SPANIO_TEST(resolve_localhost)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("localhost", "8080", ec);
    SPANIO_CHECK(!ec);
    SPANIO_REQUIRE(!found.empty());
    SPANIO_CHECK(contains_address(found, spanio::ipv4::loopback())
                 || contains_address(found, spanio::ipv6::loopback()));
    for (const tcp::endpoint& endpoint : found)
    {
        SPANIO_CHECK(endpoint.port() == kSamplePort);
    }
    SPANIO_CHECK(!has_duplicates(found));
}

// Case 4: a service name resolves to its port; skipped if the system has no
// services entry for it
SPANIO_TEST(resolve_service_name)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("127.0.0.1", "http", ec);
    if (ec == spanio::error::service_not_found)
    {
        std::printf("    skipped: no services entry for \"http\"\n");
        return;
    }
    SPANIO_CHECK(!ec);
    SPANIO_REQUIRE(!found.empty());
    SPANIO_CHECK(found[0].port() == kHttpPort);
}

// Case 5: an unknown service name reports service_not_found
SPANIO_TEST(unknown_service)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("127.0.0.1", "no-such-service-spanio", ec);
    SPANIO_CHECK(ec == spanio::error::service_not_found);
    SPANIO_CHECK(found.empty());
}

// Case 6: with numeric_host, a name is rejected without a DNS query
SPANIO_TEST(numeric_host_rejects_names)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    resolve_options options;
    options.m_numeric_host = true;
    std::error_code ec;
    const std::vector<tcp::endpoint> found = resolver.resolve("localhost", "8080", ec, options);
    SPANIO_CHECK(ec == spanio::error::host_not_found);
    SPANIO_CHECK(found.empty());

    const std::vector<tcp::endpoint> numeric = resolver.resolve("127.0.0.1", "8080", ec, options);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(numeric.size() == 1);
}

// Case 7: a name that cannot exist reports host_not_found (or, on a machine
// without DNS, host_not_found_try_again); this is the one DNS query
SPANIO_TEST(unknown_host)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    resolve_record async;
    resolver.async_resolve("nonexistent.invalid", "80", record_resolve(async));
    ctx.run();
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(async.m_ec == spanio::error::host_not_found
                 || async.m_ec == spanio::error::host_not_found_try_again);
    SPANIO_CHECK(async.m_endpoints.empty());
    SPANIO_CHECK(std::string(async.m_ec.category().name()) == "spanio.netdb");
}

// Case 8: the family filter returns only addresses of that family
SPANIO_TEST(family_filter)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::error_code ec;
    resolve_options v4_only;
    v4_only.m_family = resolve_family::v4;
    const std::vector<tcp::endpoint> v4 = resolver.resolve("localhost", "8080", ec, v4_only);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(!v4.empty());
    for (const tcp::endpoint& endpoint : v4)
    {
        SPANIO_CHECK(endpoint.family() == AF_INET);
    }

    resolve_options v6_only;
    v6_only.m_family = resolve_family::v6;
    const std::vector<tcp::endpoint> v6 = resolver.resolve("::1", "8080", ec, v6_only);
    SPANIO_CHECK(!ec);
    for (const tcp::endpoint& endpoint : v6)
    {
        SPANIO_CHECK(endpoint.family() == AF_INET6);
    }
    resolver.resolve("127.0.0.1", "8080", ec, v6_only);
    SPANIO_CHECK(static_cast<bool>(ec));
}

// Case 9: an asynchronous resolve never runs its handler inline, and run()
// waits for it
SPANIO_TEST(async_counts_as_work)
{
    io_context ctx;
    blocking_lookup lookup;
    tcp::resolver resolver(ctx, lookup.function());
    resolve_record async;
    resolver.async_resolve("slow.example", "80", record_resolve(async));
    SPANIO_CHECK(async.m_calls == 0);
    SPANIO_REQUIRE(lookup.wait_started());

    auto release_later = [&lookup]()
    {
        std::this_thread::sleep_for(kSettle);
        lookup.release();
    };
    std::thread releaser(release_later);
    ctx.run();
    releaser.join();
    SPANIO_CHECK(lookup.finished());
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(!async.m_ec);
    SPANIO_CHECK(async.m_endpoints.size() == 1);
}

// Case 10: cancel() answers a request whose lookup is still running at
// once; the late result is dropped and the handler runs only once
SPANIO_TEST(cancel_reports_canceled)
{
    io_context ctx;
    blocking_lookup lookup;
    tcp::resolver resolver(ctx, lookup.function());
    resolve_record async;
    resolver.async_resolve("slow.example", "80", record_resolve(async));
    SPANIO_REQUIRE(lookup.wait_started());

    resolver.cancel();
    const test_clock::time_point start = test_clock::now();
    ctx.run();
    SPANIO_CHECK(elapsed(start) < kGenerousLimit);
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(async.m_ec == std::errc::operation_canceled);
    SPANIO_CHECK(async.m_endpoints.empty());

    // The lookup finishes later; its result must not reach the handler
    lookup.release();
    ctx.restart();
    ctx.poll();
    auto wait_finished = [&lookup]()
    {
        return lookup.finished();
    };
    while (!wait_finished())
    {
        std::this_thread::sleep_for(kSettle);
    }
    std::this_thread::sleep_for(kSettle);
    ctx.restart();
    ctx.poll();
    SPANIO_CHECK(async.m_calls == 1);
}

// Case 11: destroying a resolver cancels its request, and waits for the
// lookup in progress
SPANIO_TEST(destroy_with_pending_request)
{
    io_context ctx;
    blocking_lookup lookup;
    resolve_record async;
    std::unique_ptr<tcp::resolver> resolver(new tcp::resolver(ctx, lookup.function()));
    resolver->async_resolve("slow.example", "80", record_resolve(async));
    SPANIO_REQUIRE(lookup.wait_started());

    auto release_later = [&lookup]()
    {
        std::this_thread::sleep_for(kSettle);
        lookup.release();
    };
    std::thread releaser(release_later);
    resolver.reset();
    // The destructor returned, so the lookup it waited for has finished
    SPANIO_CHECK(lookup.finished());
    releaser.join();

    ctx.run();
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(async.m_ec == std::errc::operation_canceled);
}

// Case 12: many requests each complete exactly once with their own result
SPANIO_TEST(many_requests_complete_once)
{
    io_context ctx;
    tcp::resolver resolver(ctx);
    std::vector<resolve_record> records(static_cast<std::size_t>(kManyRequests));
    for (int i = 0; i < kManyRequests; ++i)
    {
        resolver.async_resolve("127.0.0.1", std::to_string(kFirstRequestPort + i),
                               record_resolve(records[static_cast<std::size_t>(i)]));
    }
    ctx.run();
    for (int i = 0; i < kManyRequests; ++i)
    {
        const resolve_record& record = records[static_cast<std::size_t>(i)];
        SPANIO_CHECK(record.m_calls == 1);
        SPANIO_CHECK(!record.m_ec);
        SPANIO_REQUIRE(record.m_endpoints.size() == 1);
        SPANIO_CHECK(record.m_endpoints[0].port() == static_cast<std::uint16_t>(kFirstRequestPort + i));
    }
}

// Case 13: async_connect moves on when an endpoint refuses: with ::1 first
// and the server only on 127.0.0.1, it connects to 127.0.0.1
SPANIO_TEST(async_connect_falls_back)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        const std::uint16_t port = listening_port(acc);
        tcp::socket sock(*ctx);

        int calls = 0;
        std::error_code connect_ec;
        tcp::endpoint connected;
        auto on_connect = [&](std::error_code ec, const tcp::endpoint& peer)
        {
            ++calls;
            connect_ec = ec;
            connected = peer;
        };
        const std::vector<tcp::endpoint> candidates = {
            tcp::endpoint(spanio::ipv6::loopback(), port), tcp::endpoint(spanio::ipv4::loopback(), port)};
        const test_clock::time_point start = test_clock::now();
        tcp::async_connect(sock, candidates, on_connect);
        SPANIO_CHECK(calls == 0);
        ctx->run();
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(calls == 1);
        SPANIO_CHECK(!connect_ec);
        SPANIO_CHECK(connected == candidates[1]);
        SPANIO_CHECK(sock.is_open());
    }
}

// Case 14: when no endpoint accepts, async_connect reports the last error
// once; an empty list reports host_not_found
SPANIO_TEST(async_connect_all_fail)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::uint16_t closed_port = 0;
        {
            // Borrow a free port, then release it so nothing listens there
            tcp::acceptor probe(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
            closed_port = listening_port(probe);
        }
        tcp::socket sock(*ctx);
        int calls = 0;
        std::error_code connect_ec;
        auto on_connect = [&](std::error_code ec, const tcp::endpoint&)
        {
            ++calls;
            connect_ec = ec;
        };
        const std::vector<tcp::endpoint> candidates = {tcp::endpoint(spanio::ipv4::loopback(), closed_port)};
        tcp::async_connect(sock, candidates, on_connect);
        ctx->run();
        SPANIO_CHECK(calls == 1);
        SPANIO_CHECK(connect_ec == std::errc::connection_refused);

        ctx->restart();
        int empty_calls = 0;
        std::error_code empty_ec;
        auto on_empty = [&](std::error_code ec, const tcp::endpoint&)
        {
            ++empty_calls;
            empty_ec = ec;
        };
        tcp::async_connect(sock, std::vector<tcp::endpoint>(), on_empty);
        SPANIO_CHECK(empty_calls == 0);
        ctx->run();
        SPANIO_CHECK(empty_calls == 1);
        SPANIO_CHECK(empty_ec == spanio::error::host_not_found);
    }
}

// Case 15: udp::resolver resolves numeric addresses and "localhost"
SPANIO_TEST(udp_resolver)
{
    io_context ctx;
    udp::resolver resolver(ctx);
    std::error_code ec;
    const std::vector<udp::endpoint> numeric = resolver.resolve("127.0.0.1", "9000", ec);
    SPANIO_CHECK(!ec);
    SPANIO_REQUIRE(numeric.size() == 1);
    SPANIO_CHECK(numeric[0].address() == spanio::ipv4::loopback());

    resolve_record async;
    resolver.async_resolve("localhost", "9000", record_resolve(async));
    ctx.run();
    SPANIO_CHECK(async.m_calls == 1);
    SPANIO_CHECK(!async.m_ec);
    SPANIO_CHECK(!async.m_endpoints.empty());
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
