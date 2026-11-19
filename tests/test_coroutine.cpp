// ============================================================================
// File purpose: Tests of the C++20 coroutine support (spanio/coroutine.h):
//               awaitable<T>, use_awaitable, as_tuple(use_awaitable),
//               co_spawn on an io_context and on a strand, cancellation,
//               exceptions, nesting, and what happens to suspended
//               coroutines when their io_context is destroyed. Every case
//               that does I/O runs once per proactor backend.
// Usage:        Built as C++20 by CMake (the rest of the library stays C++17).
//               test_coroutine [case-name-filter]
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19
// ============================================================================

// The coroutine echo example, without its main(): case 10 runs the shipped code
#define SPANIO_CORO_ECHO_SERVER_NO_MAIN
#include "../examples/coro_echo_server.cpp"

#include "spanio/coroutine.h"
#include "spanio/resolver.h"
#include "spanio/strand.h"
#include "spanio/tcp.h"
#include "spanio/timer.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <vector>

using spanio::awaitable;
using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;

namespace tcp = spanio::tcp;

namespace {

// Message the client sends and the server echoes
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// Short timer wait inside the I/O case
constexpr std::chrono::milliseconds kShortWait(10);

// Timer that would outlive every case; only cancellation or destruction ends it
constexpr std::chrono::hours kNeverExpires(1);

// Coroutines sharing one strand in the serialization case. Two, so the
// strand often drains between steps and can move to the other thread (with
// more, its queue rarely empties and it stays on the thread holding it)
constexpr int kStrandCoroutines = 2;

// Steps each of them takes (each step increments the counter, then yields)
constexpr int kStrandSteps = 200;

// Threads calling run() in the serialization case
constexpr int kRunThreads = 2;

// How long each strand step holds the strand: long enough for the other
// run() thread to pick up due timers meanwhile, which widens the window in
// which an overlap would show
constexpr std::chrono::microseconds kStepHold(20);

// Wait between two steps of one coroutine. Not 0: a timer already due is
// taken by the thread that just ran a step, so only one thread would run
// steps; a real wait makes the threads take turns waiting and running
constexpr std::chrono::milliseconds kStrandYield(1);

// How long the blocker coroutine (not on the strand) keeps its thread busy
// per round. Without it, the thread that frees the strand always takes the
// next due timer itself, so every step would run on one thread; while the
// blocker holds one thread, the other one finds the strand idle and runs
// steps, so the strand really moves between threads
constexpr std::chrono::milliseconds kBlockerHold(2);

// Rounds of the blocker coroutine; together they last about as long as the
// strand coroutines
constexpr int kBlockerRounds = 100;

// Threads the steps must have run on: proof that the strand moved between
// threads, so the case checks real parallelism
constexpr std::size_t kMinStepThreads = 2;

// Rounds case 6 may need until the steps used kMinStepThreads threads. A
// single round stays on one thread about one time in four on a 2-vCPU VM,
// so ten rounds all doing so is practically impossible
constexpr int kMaxStrandRounds = 10;

// Values the nesting case adds up
constexpr int kFirstAddend = 1;

// Second value of the nesting case
constexpr int kSecondAddend = 2;

// Third value of the nesting case
constexpr int kThirdAddend = 3;

// Value returned by the outer coroutine when the inner one threw
constexpr int kCaughtMarker = -1;

// Text of the exceptions thrown by test coroutines
constexpr char kBoom[] = "boom";

// Port of the numeric resolve in the I/O case
constexpr char kResolvePort[] = "8080";

// Port number kResolvePort stands for
constexpr std::uint16_t kResolvePortNumber = 8080;

// Receive buffer of the error cases
constexpr std::size_t kSmallBuffer = 16;

// Bytes each client sends the echo example; more than one read chunk
constexpr std::size_t kExampleMessageBytes = 10000;

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

// Completion handler of co_spawn that records how the coroutine ended
class spawn_record
{
public:
    // Number of times the handler ran
    int m_calls = 0;
    // Exception that ended the coroutine, or null
    std::exception_ptr m_error;
};

// Callback for co_spawn of an awaitable<void>
class record_end
{
public:
    // Parameter record: where to record; must outlive the handler
    explicit record_end(spawn_record& record)
        : m_record(&record)
    {
    }

    // Parameter error: exception that ended the coroutine, or null
    void operator()(std::exception_ptr error)
    {
        ++m_record->m_calls;
        m_record->m_error = error;
    }

private:
    // Destination
    spawn_record* m_record;
};

// Callback for co_spawn of an awaitable<int>
class record_value
{
public:
    // Parameter record: where to record the end
    // Parameter value:  where to record the value
    record_value(spawn_record& record, int& value)
        : m_record(&record)
        , m_value(&value)
    {
    }

    // Parameter error: exception that ended the coroutine, or null
    // Parameter value: its value
    void operator()(std::exception_ptr error, int value)
    {
        ++m_record->m_calls;
        m_record->m_error = error;
        *m_value = value;
    }

private:
    // Destination of the end
    spawn_record* m_record;
    // Destination of the value
    int* m_value;
};

// Message of the exception a record holds, or "" when there is none
// Parameter record: spawn result
// Return value: what() of the exception
std::string error_text(const spawn_record& record)
{
    if (!record.m_error)
    {
        return std::string();
    }
    try
    {
        std::rethrow_exception(record.m_error);
    }
    catch (const std::exception& ex)
    {
        return ex.what();
    }
    catch (...)
    {
        return "unknown exception";
    }
}

// ---- Case 1 coroutines -----------------------------------------------------

// What the I/O case observed
struct io_observations
{
    // Bytes the server read
    std::size_t m_server_read = 0;
    // What the client received back
    std::string m_echo;
    // Whether the timer wait ended without an error
    bool m_timer_ok = false;
    // Endpoints the resolver returned
    std::vector<tcp::endpoint> m_resolved;
};

// Accept one client, read kMessageLength bytes and write them back
// Parameter acc:  listening acceptor
// Parameter seen: observations to fill
awaitable<void> echo_server(tcp::acceptor& acc, io_observations& seen)
{
    tcp::socket peer = co_await acc.async_accept(spanio::use_awaitable);
    char data[kMessageLength] = {};
    seen.m_server_read = co_await peer.async_read(spanio::buffer(data, sizeof(data)), spanio::use_awaitable);
    co_await peer.async_write(spanio::buffer(data, sizeof(data)), spanio::use_awaitable);
}

// Resolve a numeric address, wait on a timer, connect, send, read the echo
// Parameter ctx:    context of the objects
// Parameter target: server address
// Parameter seen:   observations to fill
awaitable<void> echo_client(io_context& ctx, tcp::endpoint target, io_observations& seen)
{
    tcp::resolver resolver(ctx);
    seen.m_resolved = co_await resolver.async_resolve("127.0.0.1", kResolvePort, spanio::use_awaitable);

    spanio::steady_timer timer(ctx, kShortWait);
    co_await timer.async_wait(spanio::use_awaitable);
    seen.m_timer_ok = true;

    tcp::socket sock(ctx);
    co_await sock.async_connect(target, spanio::use_awaitable);
    co_await sock.async_write(spanio::buffer(kMessage, kMessageLength), spanio::use_awaitable);
    char reply[kMessageLength] = {};
    const std::size_t got = co_await sock.async_read(spanio::buffer(reply, sizeof(reply)), spanio::use_awaitable);
    seen.m_echo.assign(reply, got);
}

// ---- Case 2 coroutines -----------------------------------------------------

// Errors seen by the error case
struct error_observations
{
    // as_tuple: error of a connect to a closed port
    std::error_code m_refused_tuple;
    // use_awaitable: code of the system_error thrown for the same connect
    std::error_code m_refused_thrown;
    // as_tuple: error and bytes of a read after the peer closed
    std::error_code m_eof_tuple;
    // as_tuple: bytes of that read
    std::size_t m_eof_bytes = 1;
    // use_awaitable: code of the system_error thrown for the same read
    std::error_code m_eof_thrown;
};

// Connect to a closed port, first as a tuple, then expecting an exception
// Parameter ctx:    context of the sockets
// Parameter closed: endpoint nobody listens on
// Parameter seen:   observations to fill
awaitable<void> refused_client(io_context& ctx, tcp::endpoint closed, error_observations& seen)
{
    tcp::socket first(ctx);
    auto [ec] = co_await first.async_connect(closed, spanio::as_tuple(spanio::use_awaitable));
    seen.m_refused_tuple = ec;

    tcp::socket second(ctx);
    try
    {
        co_await second.async_connect(closed, spanio::use_awaitable);
    }
    catch (const std::system_error& ex)
    {
        seen.m_refused_thrown = ex.code();
    }
}

// Accept one client and close it at once
// Parameter acc: listening acceptor
awaitable<void> closing_server(tcp::acceptor& acc)
{
    tcp::socket peer = co_await acc.async_accept(spanio::use_awaitable);
    peer.close();
}

// Read from a peer that closes without sending, twice
// Parameter ctx:    context of the socket
// Parameter target: server that closes every connection
// Parameter seen:   observations to fill
awaitable<void> eof_client(io_context& ctx, tcp::endpoint target, error_observations& seen)
{
    tcp::socket sock(ctx);
    co_await sock.async_connect(target, spanio::use_awaitable);
    char data[kSmallBuffer] = {};
    auto [ec, bytes] = co_await sock.async_read(spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
    seen.m_eof_tuple = ec;
    seen.m_eof_bytes = bytes;
    try
    {
        co_await sock.async_read_some(spanio::buffer(data), spanio::use_awaitable);
    }
    catch (const std::system_error& ex)
    {
        seen.m_eof_thrown = ex.code();
    }
}

// ---- Case 3 coroutines -----------------------------------------------------

// Wait for data that never comes; the socket is closed from outside
// Parameter sock:   connected socket
// Parameter result: receives the read's error
awaitable<void> blocked_reader(tcp::socket& sock, std::error_code& result)
{
    char data[kSmallBuffer] = {};
    auto [ec, bytes] = co_await sock.async_read_some(spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
    static_cast<void>(bytes);
    result = ec;
}

// Wait on a timer that is cancelled from outside
// Parameter timer:  timer far in the future
// Parameter result: receives the wait's error
awaitable<void> blocked_waiter(spanio::steady_timer& timer, std::error_code& result)
{
    auto [ec] = co_await timer.async_wait(spanio::as_tuple(spanio::use_awaitable));
    result = ec;
}

// Closes a socket and cancels a timer when run
class canceller
{
public:
    // Parameter sock:  socket to close
    // Parameter timer: timer to cancel
    canceller(tcp::socket& sock, spanio::steady_timer& timer)
        : m_socket(&sock)
        , m_timer(&timer)
    {
    }

    // Close and cancel
    void operator()()
    {
        m_socket->close();
        m_timer->cancel();
    }

private:
    // Socket to close
    tcp::socket* m_socket;
    // Timer to cancel
    spanio::steady_timer* m_timer;
};

// ---- Case 4 coroutines -----------------------------------------------------

// Yield once through a timer, then throw
// Parameter ctx: context of the timer
// Parameter ran: set once the body runs
awaitable<void> throwing_coroutine(io_context& ctx, bool& ran)
{
    ran = true;
    spanio::steady_timer timer(ctx, std::chrono::milliseconds(0));
    co_await timer.async_wait(spanio::use_awaitable);
    throw std::runtime_error(kBoom);
}

// ---- Case 5 coroutines -----------------------------------------------------

// Hold a reference until the frame is destroyed, waiting on a timer that
// never expires
// Parameter ctx:   context of the timer
// Parameter probe: copy kept in the frame; its use count reveals the frame
awaitable<void> waits_forever(io_context& ctx, std::shared_ptr<int> probe)
{
    spanio::steady_timer timer(ctx, kNeverExpires);
    co_await timer.async_wait(spanio::use_awaitable);
    static_cast<void>(probe);
}

// Calls a nested coroutine that waits forever, so a whole chain of frames
// is suspended
// Parameter ctx:   context of the timer
// Parameter probe: copy kept in the outer frame
awaitable<void> nested_waits_forever(io_context& ctx, std::shared_ptr<int> probe)
{
    co_await waits_forever(ctx, probe);
}

// ---- Case 6 coroutines -----------------------------------------------------

// State the strand coroutines share; deliberately not synchronized
struct strand_state
{
    // Incremented by every step; correct only if steps never overlap
    int m_counter = 0;
    // Coroutine steps in progress; never above 1 on a strand
    int m_active = 0;
    // Largest m_active observed
    int m_max_active = 0;
    // Guards m_threads, which is only diagnostics
    std::mutex m_thread_mutex;
    // Threads the steps ran on
    std::set<std::thread::id> m_threads;
};

// Take kStrandSteps steps, yielding to the other coroutines after each
// Parameter ctx:   context of the timer
// Parameter state: shared, unsynchronized state
awaitable<void> strand_worker(io_context& ctx, strand_state& state)
{
    spanio::steady_timer timer(ctx);
    for (int step = 0; step < kStrandSteps; ++step)
    {
        ++state.m_active;
        if (state.m_active > state.m_max_active)
        {
            state.m_max_active = state.m_active;
        }
        ++state.m_counter;
        std::this_thread::sleep_for(kStepHold);
        {
            std::lock_guard<std::mutex> lock(state.m_thread_mutex);
            state.m_threads.insert(std::this_thread::get_id());
        }
        --state.m_active;
        timer.expires_after(kStrandYield);
        co_await timer.async_wait(spanio::use_awaitable);
    }
}

// Keep one run() thread busy from time to time, outside the strand (see
// kBlockerHold)
// Parameter ctx: context of the timer
awaitable<void> blocker(io_context& ctx)
{
    spanio::steady_timer timer(ctx);
    for (int round = 0; round < kBlockerRounds; ++round)
    {
        std::this_thread::sleep_for(kBlockerHold);
        timer.expires_after(kStrandYield);
        co_await timer.async_wait(spanio::use_awaitable);
    }
}

// ---- Case 7 coroutines -----------------------------------------------------

// Add two numbers after yielding once
// Parameter ctx: context of the timer
// Parameter a:   first addend
// Parameter b:   second addend
// Return value: a + b
awaitable<int> add_later(io_context& ctx, int a, int b)
{
    spanio::steady_timer timer(ctx, std::chrono::milliseconds(0));
    co_await timer.async_wait(spanio::use_awaitable);
    co_return a + b;
}

// Throw after yielding once
// Parameter ctx: context of the timer
// Return value: 0 only if the wait failed; normally it throws
awaitable<int> fail_later(io_context& ctx)
{
    spanio::steady_timer timer(ctx, std::chrono::milliseconds(0));
    auto [ec] = co_await timer.async_wait(spanio::as_tuple(spanio::use_awaitable));
    // Conditional only because MSVC wrongly warns (C4033) about a coroutine
    // returning awaitable<int> that ends in a throw
    if (!ec)
    {
        throw std::logic_error(kBoom);
    }
    co_return 0;
}

// Nest calls three levels deep and catch an inner exception
// Parameter ctx:    context of the timers
// Parameter caught: set when the inner exception was caught here
// Return value: (1 + 2) + 3, through nested awaitables
awaitable<int> nested_sum(io_context& ctx, bool& caught)
{
    const int partial = co_await add_later(ctx, kFirstAddend, kSecondAddend);
    const int total = co_await add_later(ctx, partial, kThirdAddend);
    int marker = 0;
    try
    {
        marker = co_await fail_later(ctx);
    }
    catch (const std::logic_error&)
    {
        caught = true;
        marker = kCaughtMarker;
    }
    // co_spawn with use_awaitable: a nested spawn is awaited like an operation
    const int spawned = co_await spanio::co_spawn(ctx, add_later(ctx, marker, -kCaughtMarker), spanio::use_awaitable);
    co_return total + spawned;
}

// An exception from a nested coroutine that nobody catches ends the spawn
// Parameter ctx: context of the timers
// Return value: never returns normally
awaitable<int> uncaught_nested(io_context& ctx)
{
    const int value = co_await fail_later(ctx);
    co_return value;
}

} // namespace

// Case 1: co_await of accept, connect, read, write, a timer wait and a
// resolve delivers the right results (every backend)
SPANIO_TEST(coroutine_io_operations)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        io_observations seen;
        spawn_record server_end;
        spawn_record client_end;
        spanio::co_spawn(*ctx, echo_server(acc, seen), record_end(server_end));
        spanio::co_spawn(*ctx, echo_client(*ctx, listening_endpoint(acc), seen), record_end(client_end));
        // co_spawn never runs the coroutine inside the call
        SPANIO_CHECK(server_end.m_calls == 0);
        SPANIO_CHECK(!seen.m_timer_ok);
        ctx->run();

        SPANIO_CHECK(server_end.m_calls == 1);
        SPANIO_CHECK(client_end.m_calls == 1);
        SPANIO_CHECK(error_text(server_end).empty());
        SPANIO_CHECK(error_text(client_end).empty());
        SPANIO_CHECK(seen.m_server_read == kMessageLength);
        SPANIO_CHECK(seen.m_echo == kMessage);
        SPANIO_CHECK(seen.m_timer_ok);
        SPANIO_REQUIRE(!seen.m_resolved.empty());
        SPANIO_CHECK(seen.m_resolved.front().port() == kResolvePortNumber);
    }
}

// Case 2: as_tuple(use_awaitable) returns errors as values (connection
// refused, end of file with the byte count); use_awaitable throws
// std::system_error with the same codes (every backend)
SPANIO_TEST(coroutine_errors_as_tuple_and_exceptions)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        error_observations seen;
        spawn_record refused_end;
        spawn_record server_end;
        spawn_record eof_end;
        spanio::co_spawn(*ctx, refused_client(*ctx, closed_endpoint(*ctx), seen), record_end(refused_end));
        spanio::co_spawn(*ctx, closing_server(acc), record_end(server_end));
        spanio::co_spawn(*ctx, eof_client(*ctx, listening_endpoint(acc), seen), record_end(eof_end));
        ctx->run();

        SPANIO_CHECK(refused_end.m_calls == 1);
        SPANIO_CHECK(server_end.m_calls == 1);
        SPANIO_CHECK(eof_end.m_calls == 1);
        SPANIO_CHECK(error_text(refused_end).empty());
        SPANIO_CHECK(error_text(eof_end).empty());
        SPANIO_CHECK(seen.m_refused_tuple == std::errc::connection_refused);
        SPANIO_CHECK(seen.m_refused_thrown == std::errc::connection_refused);
        SPANIO_CHECK(seen.m_eof_tuple == spanio::error::misc_errors::eof);
        SPANIO_CHECK(seen.m_eof_bytes == 0);
        SPANIO_CHECK(seen.m_eof_thrown == spanio::error::misc_errors::eof);
    }
}

// Case 3: closing a socket or cancelling a timer makes the pending co_await
// end with operation_canceled (every backend)
SPANIO_TEST(coroutine_cancellation)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        auto on_accept = [&server](std::error_code ec, tcp::socket peer)
        {
            SPANIO_CHECK(!ec);
            server = std::move(peer);
        };
        auto on_connect = [](std::error_code ec)
        {
            SPANIO_CHECK(!ec);
        };
        acc.async_accept(on_accept);
        client.async_connect(listening_endpoint(acc), on_connect);
        ctx->run();
        ctx->restart();

        spanio::steady_timer timer(*ctx, kNeverExpires);
        std::error_code read_result;
        std::error_code wait_result;
        spawn_record reader_end;
        spawn_record waiter_end;
        spanio::co_spawn(*ctx, blocked_reader(client, read_result), record_end(reader_end));
        spanio::co_spawn(*ctx, blocked_waiter(timer, wait_result), record_end(waiter_end));
        // Queued after both starts, so it runs once both are suspended
        ctx->post(canceller(client, timer));
        ctx->run();

        SPANIO_CHECK(reader_end.m_calls == 1);
        SPANIO_CHECK(waiter_end.m_calls == 1);
        SPANIO_CHECK(read_result == std::errc::operation_canceled);
        SPANIO_CHECK(wait_result == std::errc::operation_canceled);
    }
}

// Case 4: an exception leaving a coroutine reaches the co_spawn handler as
// an exception_ptr; with detached it is dropped and run() returns normally
SPANIO_TEST(coroutine_exception_reaches_spawn_handler)
{
    io_context ctx;
    bool ran = false;
    spawn_record end;
    spanio::co_spawn(ctx, throwing_coroutine(ctx, ran), record_end(end));
    ctx.run();
    SPANIO_CHECK(ran);
    SPANIO_CHECK(end.m_calls == 1);
    SPANIO_CHECK(error_text(end) == kBoom);

    ctx.restart();
    bool detached_ran = false;
    bool escaped = false;
    spanio::co_spawn(ctx, throwing_coroutine(ctx, detached_ran), spanio::detached);
    try
    {
        ctx.run();
    }
    catch (...)
    {
        escaped = true;
    }
    SPANIO_CHECK(detached_ran);
    SPANIO_CHECK(!escaped);
}

// Case 5: destroying the io_context destroys every suspended coroutine
// (a chain waiting on a timer, and one that never started): nothing they own
// leaks (the use count of a probe they copied drops back to 1; ASan/LSan
// check the frames themselves)
SPANIO_TEST(coroutine_frames_destroyed_with_context)
{
    std::shared_ptr<int> probe = std::make_shared<int>(0);
    {
        std::unique_ptr<io_context> ctx(new io_context());
        spanio::co_spawn(*ctx, nested_waits_forever(*ctx, probe), spanio::detached);
        // Runs the start: the chain is now suspended on the timer
        ctx->poll();
        spanio::co_spawn(*ctx, waits_forever(*ctx, probe), spanio::detached);
        // One copy per outer frame, one in the nested frame
        SPANIO_CHECK(probe.use_count() > 1);
        ctx.reset();
    }
    SPANIO_CHECK(probe.use_count() == 1);
}

// One round of case 6: two strand coroutines and the blocker on two run()
// threads; checks serialization
// Return value: number of threads the strand steps ran on
std::size_t run_strand_round()
{
    io_context ctx;
    spanio::strand<io_context::executor_type> strand = spanio::make_strand(ctx);
    strand_state state;
    spawn_record ends[kStrandCoroutines];
    for (int i = 0; i < kStrandCoroutines; ++i)
    {
        spanio::co_spawn(strand, strand_worker(ctx, state), record_end(ends[i]));
    }
    spawn_record blocker_end;
    spanio::co_spawn(ctx, blocker(ctx), record_end(blocker_end));
    auto runner = [&ctx]()
    {
        ctx.run();
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < kRunThreads; ++i)
    {
        threads.emplace_back(runner);
    }
    for (std::thread& thread : threads)
    {
        thread.join();
    }

    for (int i = 0; i < kStrandCoroutines; ++i)
    {
        SPANIO_CHECK(ends[i].m_calls == 1);
        SPANIO_CHECK(error_text(ends[i]).empty());
    }
    SPANIO_CHECK(state.m_counter == kStrandCoroutines * kStrandSteps);
    SPANIO_CHECK(state.m_max_active == 1);
    SPANIO_CHECK(blocker_end.m_calls == 1);
    return state.m_threads.size();
}

// Case 6: coroutines spawned on one strand, with several threads in run(),
// never run at the same time: an unsynchronized counter ends exact and at
// most one step is ever active (TSan checks for data races). Which thread
// resumes a coroutine is up to the scheduler, so a round may keep every step
// on one thread; rounds repeat (each checked) until the steps used at least
// two threads, so the case always covers a strand moving between threads.
SPANIO_TEST(coroutine_strand_serializes)
{
    std::size_t step_threads = 0;
    int rounds = 0;
    while (rounds < kMaxStrandRounds && step_threads < kMinStepThreads)
    {
        step_threads = run_strand_round();
        ++rounds;
    }
    std::printf("    [steps ran on %zu thread(s), after %d round(s)]\n", step_threads, rounds);
    SPANIO_CHECK(step_threads >= kMinStepThreads);
}
// Case 7: awaitables nest: values come back from inner coroutines, an inner
// exception can be caught by the outer one, a nested co_spawn can be awaited,
// and an uncaught inner exception ends the spawn with that exception
SPANIO_TEST(coroutine_nesting)
{
    io_context ctx;
    bool caught = false;
    int sum = 0;
    spawn_record end;
    spanio::co_spawn(ctx, nested_sum(ctx, caught), record_value(end, sum));
    int unused = 0;
    spawn_record uncaught_end;
    spanio::co_spawn(ctx, uncaught_nested(ctx), record_value(uncaught_end, unused));
    ctx.run();

    SPANIO_CHECK(end.m_calls == 1);
    SPANIO_CHECK(error_text(end).empty());
    SPANIO_CHECK(caught);
    // (1 + 2) + 3, plus (-1 + 1) from the nested spawn
    SPANIO_CHECK(sum == kFirstAddend + kSecondAddend + kThirdAddend);
    SPANIO_CHECK(uncaught_end.m_calls == 1);
    SPANIO_CHECK(error_text(uncaught_end) == kBoom);
}

// Case 9: co_spawn with use_future returns the coroutine's value through a
// std::future, and an exception that ends the coroutine comes back from get()
SPANIO_TEST(coroutine_co_spawn_use_future)
{
    io_context ctx;
    std::future<int> value = spanio::co_spawn(ctx, add_later(ctx, kFirstAddend, kSecondAddend), spanio::use_future);
    std::future<int> failure = spanio::co_spawn(ctx, fail_later(ctx), spanio::use_future);
    ctx.run();

    SPANIO_CHECK(value.get() == kFirstAddend + kSecondAddend);
    bool rethrown = false;
    try
    {
        failure.get();
    }
    catch (const std::logic_error& ex)
    {
        rethrown = std::strcmp(ex.what(), kBoom) == 0;
    }
    SPANIO_CHECK(rethrown);
}

// Case 10: the coroutine echo example (examples/coro_echo_server.cpp) serves
// two clients at once, each getting its own data back, and stops when the
// clients are gone and its acceptor is closed (every backend)
SPANIO_TEST(coroutine_echo_example)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        const std::uint16_t port = listening_endpoint(acc).port();
        coro_echo::start(*ctx, acc);
        io_context& context = *ctx;
        auto runner = [&context]()
        {
            context.run();
        };
        std::thread server_thread(runner);

        const std::string first(kExampleMessageBytes, 'a');
        const std::string second(kExampleMessageBytes, 'b');
        spanio::test::socket_guard client_a;
        spanio::test::socket_guard client_b;
        SPANIO_CHECK(spanio::test::connect_client(port, client_a));
        SPANIO_CHECK(spanio::test::connect_client(port, client_b));
        SPANIO_CHECK(spanio::test::send_all(client_a.get(), first.data(), first.size()));
        SPANIO_CHECK(spanio::test::send_all(client_b.get(), second.data(), second.size()));
        std::string echo_a(first.size(), '\0');
        std::string echo_b(second.size(), '\0');
        SPANIO_CHECK(spanio::test::recv_exact(client_a.get(), &echo_a[0], echo_a.size()));
        SPANIO_CHECK(spanio::test::recv_exact(client_b.get(), &echo_b[0], echo_b.size()));
        SPANIO_CHECK(echo_a == first);
        SPANIO_CHECK(echo_b == second);

        // Sessions end at the clients' EOF, the accept loop when the acceptor
        // closes; then run() runs out of work and the thread ends
        client_a.reset();
        client_b.reset();
        tcp::acceptor* const listener = &acc;
        auto close_acceptor = [listener]()
        {
            listener->close();
        };
        ctx->post(close_acceptor);
        server_thread.join();
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
