// ============================================================================
// File purpose: Tests of the C++17 completion tokens (spanio/async_result.h):
//               use_future, as_tuple and detached, used with the async_*
//               functions of sockets, acceptors, timers and resolvers. The
//               coroutine token use_awaitable is tested in test_coroutine.
//               Every case that does I/O runs once per proactor backend.
// Usage:        test_completion_tokens [case-name-filter]
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19
// ============================================================================

#include "spanio/async_result.h"
#include "spanio/resolver.h"
#include "spanio/tcp.h"
#include "spanio/timer.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <future>
#include <memory>
#include <string>
#include <system_error>
#include <tuple>
#include <vector>

using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;

namespace tcp = spanio::tcp;

namespace {

// Message the client sends
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// Short timer wait
constexpr std::chrono::milliseconds kShortWait(10);

// Timer that would outlive every case
constexpr std::chrono::hours kNeverExpires(1);

// Port of the numeric resolve
constexpr char kResolvePort[] = "8080";

// Port number kResolvePort stands for
constexpr std::uint16_t kResolvePortNumber = 8080;

// Receive buffer of the error cases
constexpr std::size_t kSmallBuffer = 16;

// Listening endpoint of an acceptor, or fail the case
// Parameter acc: open, bound acceptor
// Return value: its local endpoint
tcp::endpoint listening_endpoint(const tcp::acceptor& acc)
{
    std::error_code ec;
    const tcp::endpoint local = acc.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return local;
}

// An endpoint nobody listens on: a listener's port, closed again
// Parameter ctx: context for the temporary acceptor
// Return value: loopback endpoint with a free port
tcp::endpoint closed_endpoint(io_context& ctx)
{
    tcp::acceptor temporary(ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    return listening_endpoint(temporary);
}

// Error code of the std::system_error a future holds
// Parameter future: a ready future expected to hold an exception
// Return value: its code, or an empty code if get() did not throw one
template <typename T>
std::error_code thrown_code(std::future<T>& future)
{
    try
    {
        future.get();
    }
    catch (const std::system_error& ex)
    {
        return ex.code();
    }
    return std::error_code();
}

// What an as_tuple callback received
struct tuple_record
{
    // Number of times it ran
    int m_calls = 0;
    // Error of the completion
    std::error_code m_ec;
    // Bytes of the completion
    std::size_t m_bytes = 0;
};

// Callback taking a whole (error, bytes) completion as one tuple
class record_tuple
{
public:
    // Parameter record: where to record; must outlive the callback
    explicit record_tuple(tuple_record& record)
        : m_record(&record)
    {
    }

    // Parameter result: the completion
    void operator()(std::tuple<std::error_code, std::size_t> result)
    {
        ++m_record->m_calls;
        m_record->m_ec = std::get<0>(result);
        m_record->m_bytes = std::get<1>(result);
    }

private:
    // Destination
    tuple_record* m_record;
};

// Accept a client and connect to the acceptor, both through use_future
// Parameter ctx:    context of the objects
// Parameter acc:    listening acceptor
// Parameter client: socket that is not open yet; connected afterwards
// Return value: the accepted socket
tcp::socket connect_with_futures(io_context& ctx, tcp::acceptor& acc, tcp::socket& client)
{
    std::future<tcp::socket> accepted = acc.async_accept(spanio::use_future);
    std::future<void> connected = client.async_connect(listening_endpoint(acc), spanio::use_future);
    ctx.run();
    ctx.restart();
    connected.get();
    return accepted.get();
}

} // namespace

// Case 1: use_future delivers the value of accept, connect, write, read, a
// timer wait and a resolve; operations without a value give future<void>
SPANIO_TEST(use_future_delivers_values)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server = connect_with_futures(*ctx, acc, client);
        SPANIO_REQUIRE(server.is_open());
        SPANIO_REQUIRE(client.is_open());

        char received[kMessageLength] = {};
        std::future<std::size_t> written = client.async_write(spanio::buffer(kMessage, kMessageLength),
                                                              spanio::use_future);
        std::future<std::size_t> read = server.async_read(spanio::buffer(received, sizeof(received)),
                                                          spanio::use_future);
        spanio::steady_timer timer(*ctx, kShortWait);
        std::future<void> waited = timer.async_wait(spanio::use_future);
        tcp::resolver resolver(*ctx);
        std::future<std::vector<tcp::endpoint>> resolved =
            resolver.async_resolve("127.0.0.1", kResolvePort, spanio::use_future);
        ctx->run();

        SPANIO_CHECK(written.get() == kMessageLength);
        SPANIO_CHECK(read.get() == kMessageLength);
        SPANIO_CHECK(std::memcmp(received, kMessage, kMessageLength) == 0);
        waited.get();
        const std::vector<tcp::endpoint> endpoints = resolved.get();
        SPANIO_REQUIRE(!endpoints.empty());
        SPANIO_CHECK(endpoints.front().port() == kResolvePortNumber);
    }
}

// Case 2: an error makes future.get() throw std::system_error with its code
// (connection refused, end of file); as_tuple(use_future) returns the code
// and the byte count as values instead
SPANIO_TEST(use_future_errors_and_as_tuple)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        const tcp::endpoint closed = closed_endpoint(*ctx);
        tcp::socket refused_throw(*ctx);
        tcp::socket refused_tuple(*ctx);
        std::future<void> thrown = refused_throw.async_connect(closed, spanio::use_future);
        std::future<std::tuple<std::error_code>> as_value =
            refused_tuple.async_connect(closed, spanio::as_tuple(spanio::use_future));
        ctx->run();
        ctx->restart();
        SPANIO_CHECK(thrown_code(thrown) == std::errc::connection_refused);
        SPANIO_CHECK(std::get<0>(as_value.get()) == std::errc::connection_refused);

        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server = connect_with_futures(*ctx, acc, client);
        server.close();
        char data[kSmallBuffer] = {};
        std::future<std::tuple<std::error_code, std::size_t>> eof_value =
            client.async_read(spanio::buffer(data), spanio::as_tuple(spanio::use_future));
        ctx->run();
        ctx->restart();
        const std::tuple<std::error_code, std::size_t> eof = eof_value.get();
        SPANIO_CHECK(std::get<0>(eof) == spanio::error::misc_errors::eof);
        SPANIO_CHECK(std::get<1>(eof) == 0);

        std::future<std::size_t> eof_thrown = client.async_read_some(spanio::buffer(data), spanio::use_future);
        ctx->run();
        SPANIO_CHECK(thrown_code(eof_thrown) == spanio::error::misc_errors::eof);
    }
}

// Case 3: when the io_context is destroyed before the operation completes,
// the future reports broken_promise instead of waiting forever
SPANIO_TEST(use_future_broken_when_context_destroyed)
{
    std::future<void> pending;
    {
        io_context ctx;
        spanio::steady_timer timer(ctx, kNeverExpires);
        pending = timer.async_wait(spanio::use_future);
        ctx.poll();
        SPANIO_CHECK(pending.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);
        // The timer is destroyed first (cancelling the wait into the queue),
        // then the context destroys the queued handler without calling it
    }
    bool broken = false;
    try
    {
        pending.get();
    }
    catch (const std::future_error& ex)
    {
        broken = ex.code() == std::future_errc::broken_promise;
    }
    catch (const std::system_error&)
    {
        // The cancelled wait ran: not what this case is about
    }
    SPANIO_CHECK(broken);
}

// Case 4: as_tuple with a callback passes the whole completion as one tuple,
// exactly once and never inside the initiating call
SPANIO_TEST(as_tuple_with_callback)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server = connect_with_futures(*ctx, acc, client);

        tuple_record write_record;
        tuple_record read_record;
        char received[kMessageLength] = {};
        client.async_write(spanio::buffer(kMessage, kMessageLength), spanio::as_tuple(record_tuple(write_record)));
        server.async_read(spanio::buffer(received, sizeof(received)), spanio::as_tuple(record_tuple(read_record)));
        SPANIO_CHECK(write_record.m_calls == 0);
        SPANIO_CHECK(read_record.m_calls == 0);
        ctx->run();

        SPANIO_CHECK(write_record.m_calls == 1);
        SPANIO_CHECK(!write_record.m_ec);
        SPANIO_CHECK(write_record.m_bytes == kMessageLength);
        SPANIO_CHECK(read_record.m_calls == 1);
        SPANIO_CHECK(!read_record.m_ec);
        SPANIO_CHECK(read_record.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(received, kMessage, kMessageLength) == 0);
    }
}

// Case 5: detached runs the operation and drops its result: the data of a
// detached write reaches the peer
SPANIO_TEST(detached_runs_and_drops_result)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server = connect_with_futures(*ctx, acc, client);

        char received[kMessageLength] = {};
        client.async_write(spanio::buffer(kMessage, kMessageLength), spanio::detached);
        std::future<std::size_t> read = server.async_read(spanio::buffer(received, sizeof(received)),
                                                          spanio::use_future);
        ctx->run();
        SPANIO_CHECK(read.get() == kMessageLength);
        SPANIO_CHECK(std::memcmp(received, kMessage, kMessageLength) == 0);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
