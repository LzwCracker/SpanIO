// ============================================================================
// File purpose: Unit tests for io_context (io_context.h): handler queueing,
//               run / run_one / poll / poll_one, stop and restart, work
//               guards, dispatch, multi-threaded run(), exception
//               propagation, destruction, and I/O completions delivered as
//               handlers. The I/O cases run once per proactor backend of the
//               platform (see test_proactor_backends.h); the other cases use
//               the default backend. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_io_context executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_io_context stop`.
//
//               In the I/O cases the sockets are declared before the
//               context, so the context (and its proactor) is destroyed
//               first and the sockets are closed afterwards.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#include "spanio/io_context.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using spanio::executor_work_guard;
using spanio::io_context;
using spanio::make_work_guard;
using spanio::detail::io_proactor_ptr;
using spanio::detail::io_result;
using spanio::detail::make_io_op;
using spanio::detail::prepare_recv;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;
using spanio::test::make_tcp_pair;
using spanio::test::socket_guard;

namespace {

// Number of handlers posted in the FIFO order case
constexpr int kOrderCount = 100;

// Length of the self-posting handler chain
constexpr int kChainLength = 1000;

// Handlers posted by the poll / run_one case before polling
constexpr int kPollBatch = 3;

// Handlers that are still queued after the first poll_one()
constexpr std::size_t kPollRest = kPollBatch - 1;

// Handlers queued behind the stopping handler in the stop case
constexpr std::size_t kAfterStopCount = 2;

// Threads calling run() in the multi-thread case
constexpr int kRunThreads = 4;

// Threads posting handlers in the multi-thread case
constexpr int kPostThreads = 4;

// Handlers posted in total in the multi-thread case
constexpr int kMultiThreadHandlers = 10000;

// Handlers posted by each posting thread
constexpr int kHandlersPerPoster = kMultiThreadHandlers / kPostThreads;

// Handlers posted in the destructor case
constexpr long kDestroyedHandlers = 2;

// Owners of the shared token in the destructor cases besides the test itself
constexpr long kSoleOwner = 1;

// Delay before a helper thread acts, so the main thread is surely blocked
constexpr int kHelperDelayMs = 200;

// Lower bound of a wake-up that a helper thread triggers after kHelperDelayMs
constexpr long long kMinWakeMs = kHelperDelayMs / 2;

// Upper bound for being woken by a helper thread or for a fast destructor
constexpr long long kWakeLimitMs = 2000;

// Size of the receive buffers
constexpr std::size_t kBufferSize = 64;

// Short message sent by the I/O cases
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// recv/send flags for raw socket calls: none
constexpr int kNoFlags = 0;

// Clock used for all time measurements
using test_clock = std::chrono::steady_clock;

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Sleep for the helper delay
void helper_delay()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
}

// Destroy a context and check it did not wait for I/O it should already
// have collected; a slow destructor means its I/O accounting is off
// Parameter ctx: context to destroy; null afterwards
void check_fast_destruction(std::unique_ptr<io_context>& ctx)
{
    const test_clock::time_point start = test_clock::now();
    ctx.reset();
    SPANIO_CHECK(elapsed_ms(start) < kWakeLimitMs);
}

// Send kMessage on a blocking socket or fail the case
// Parameter handle: connected socket
void send_message(spanio::detail::native_handle_t handle)
{
    SPANIO_REQUIRE(::send(handle, kMessage, static_cast<int>(kMessageLength), kNoFlags)
                   == static_cast<int>(kMessageLength));
}

}

// Case 1: run() with no work returns at once and leaves the context stopped
SPANIO_TEST(run_without_work_returns_immediately)
{
    io_context ctx;
    SPANIO_CHECK(!ctx.stopped());
    SPANIO_CHECK(ctx.run() == 0);
    SPANIO_CHECK(ctx.stopped());
}

// Case 2: a posted handler runs exactly once, on the thread calling run()
SPANIO_TEST(post_runs_handler_on_run_thread)
{
    io_context ctx;
    int calls = 0;
    std::thread::id runner;
    auto record_call = [&]()
    {
        ++calls;
        runner = std::this_thread::get_id();
    };
    ctx.post(record_call);
    SPANIO_CHECK(calls == 0);
    SPANIO_CHECK(ctx.run() == 1);
    SPANIO_CHECK(calls == 1);
    SPANIO_CHECK(runner == std::this_thread::get_id());
}

// Case 3: handlers run in the order they were posted
SPANIO_TEST(post_preserves_fifo_order)
{
    io_context ctx;
    std::vector<int> order;
    for (int i = 0; i < kOrderCount; ++i)
    {
        auto record_index = [&order, i]()
        {
            order.push_back(i);
        };
        ctx.post(record_index);
    }
    SPANIO_CHECK(ctx.run() == static_cast<std::size_t>(kOrderCount));
    SPANIO_REQUIRE(order.size() == static_cast<std::size_t>(kOrderCount));
    for (int i = 0; i < kOrderCount; ++i)
    {
        SPANIO_CHECK(order[static_cast<std::size_t>(i)] == i);
    }
}

// Case 4: handlers that post more handlers keep run() going until the last
SPANIO_TEST(handler_can_post_more_work)
{
    io_context ctx;
    int steps = 0;
    std::function<void()> step;
    step = [&]()
    {
        ++steps;
        if (steps < kChainLength)
        {
            ctx.post(step);
        }
    };
    ctx.post(step);
    SPANIO_CHECK(ctx.run() == static_cast<std::size_t>(kChainLength));
    SPANIO_CHECK(steps == kChainLength);
    SPANIO_CHECK(ctx.stopped());
}

// Case 5: poll_one runs one, poll runs all ready without blocking, poll with
// nothing ready returns 0, run_one runs one and returns
SPANIO_TEST(poll_and_run_one_variants)
{
    io_context ctx;
    executor_work_guard guard = make_work_guard(ctx);
    int calls = 0;
    auto count_call = [&]()
    {
        ++calls;
    };
    for (int i = 0; i < kPollBatch; ++i)
    {
        ctx.post(count_call);
    }
    SPANIO_CHECK(ctx.poll_one() == 1);
    SPANIO_CHECK(calls == 1);
    SPANIO_CHECK(ctx.poll() == kPollRest);
    SPANIO_CHECK(calls == kPollBatch);

    // The guard keeps work above zero, so an empty poll() must neither block
    // nor stop the context
    const test_clock::time_point start = test_clock::now();
    SPANIO_CHECK(ctx.poll() == 0);
    SPANIO_CHECK(ctx.poll_one() == 0);
    SPANIO_CHECK(elapsed_ms(start) < kMinWakeMs);
    SPANIO_CHECK(!ctx.stopped());

    ctx.post(count_call);
    SPANIO_CHECK(ctx.run_one() == 1);
    SPANIO_CHECK(calls == kPollBatch + 1);

    guard.reset();
    SPANIO_CHECK(ctx.run() == 0);
    SPANIO_CHECK(ctx.stopped());
}

// Case 6: stop() from a handler leaves the rest queued; run() does nothing
// until restart(), then runs them
SPANIO_TEST(stop_from_handler_then_restart)
{
    io_context ctx;
    int calls = 0;
    auto count_and_stop = [&]()
    {
        ++calls;
        ctx.stop();
    };
    auto count_call = [&]()
    {
        ++calls;
    };
    ctx.post(count_and_stop);
    for (std::size_t i = 0; i < kAfterStopCount; ++i)
    {
        ctx.post(count_call);
    }
    SPANIO_CHECK(ctx.run() == 1);
    SPANIO_CHECK(calls == 1);
    SPANIO_CHECK(ctx.stopped());

    SPANIO_CHECK(ctx.run() == 0);
    SPANIO_CHECK(calls == 1);

    ctx.restart();
    SPANIO_CHECK(!ctx.stopped());
    SPANIO_CHECK(ctx.run() == kAfterStopCount);
    SPANIO_CHECK(calls == static_cast<int>(1 + kAfterStopCount));
    SPANIO_CHECK(ctx.stopped());
}

// Case 7: stop() from another thread wakes a run() kept alive by a guard
SPANIO_TEST(stop_from_other_thread_wakes_run)
{
    io_context ctx;
    executor_work_guard guard = make_work_guard(ctx);
    auto delayed_stop = [&]()
    {
        helper_delay();
        ctx.stop();
    };
    std::thread stopper(delayed_stop);
    const test_clock::time_point start = test_clock::now();
    SPANIO_CHECK(ctx.run() == 0);
    const long long waited = elapsed_ms(start);
    stopper.join();
    SPANIO_CHECK(waited >= kMinWakeMs);
    SPANIO_CHECK(waited < kWakeLimitMs);
    SPANIO_CHECK(ctx.stopped());
}

// Case 8: with a guard held, a handler posted from another thread runs;
// after the guard is reset run() returns
SPANIO_TEST(work_guard_keeps_run_alive)
{
    io_context ctx;
    executor_work_guard guard = make_work_guard(ctx);
    SPANIO_CHECK(guard.owns_work());
    std::atomic<int> calls(0);
    std::thread::id runner;
    auto record_call = [&]()
    {
        ++calls;
        runner = std::this_thread::get_id();
    };
    auto post_then_release = [&]()
    {
        helper_delay();
        ctx.post(record_call);
        helper_delay();
        guard.reset();
    };
    std::thread helper(post_then_release);
    const test_clock::time_point start = test_clock::now();
    SPANIO_CHECK(ctx.run() == 1);
    const long long waited = elapsed_ms(start);
    helper.join();
    SPANIO_CHECK(calls.load() == 1);
    SPANIO_CHECK(runner == std::this_thread::get_id());
    // run() may only return after the guard was reset, two delays in
    SPANIO_CHECK(waited >= 2 * kMinWakeMs);
    SPANIO_CHECK(!guard.owns_work());
    SPANIO_CHECK(ctx.stopped());
}

// Case 9: dispatch runs inline inside run() and is queued outside it;
// running_in_this_thread agrees
SPANIO_TEST(dispatch_inline_inside_run_posts_outside)
{
    io_context ctx;
    bool outside_ran = false;
    bool inline_ran = false;
    bool inline_seen_right_after = false;
    bool inside_flag = false;
    bool other_thread_flag = true;

    auto mark_outside = [&]()
    {
        outside_ran = true;
    };
    auto mark_inline = [&]()
    {
        inline_ran = true;
    };
    auto probe_other_thread = [&]()
    {
        other_thread_flag = ctx.running_in_this_thread();
    };
    auto dispatch_from_inside = [&]()
    {
        inside_flag = ctx.get_executor().running_in_this_thread();
        ctx.get_executor().dispatch(mark_inline);
        inline_seen_right_after = inline_ran;
        std::thread other(probe_other_thread);
        other.join();
    };

    SPANIO_CHECK(!ctx.running_in_this_thread());
    ctx.dispatch(mark_outside);
    SPANIO_CHECK(!outside_ran);

    ctx.post(dispatch_from_inside);

    // The inline dispatch is not a queued handler, so only two are counted
    SPANIO_CHECK(ctx.run() == 2);
    SPANIO_CHECK(outside_ran);
    SPANIO_CHECK(inline_ran);
    SPANIO_CHECK(inline_seen_right_after);
    SPANIO_CHECK(inside_flag);
    SPANIO_CHECK(!other_thread_flag);
    SPANIO_CHECK(!ctx.running_in_this_thread());
    SPANIO_CHECK(ctx.get_executor() == ctx.get_executor());
}

// Case 10: four threads run() while four threads post; every handler runs
// exactly once and every run() returns
SPANIO_TEST(multi_thread_run_executes_each_once)
{
    io_context ctx;
    std::vector<std::atomic<int>> hits(static_cast<std::size_t>(kMultiThreadHandlers));
    for (std::atomic<int>& hit : hits)
    {
        hit.store(0);
    }
    std::atomic<std::size_t> handlers_run(0);
    executor_work_guard guard = make_work_guard(ctx);

    auto run_context = [&]()
    {
        handlers_run += ctx.run();
    };
    // Posts the handlers of one poster thread; each marks its own slot
    auto post_batch = [&](int poster)
    {
        for (int i = 0; i < kHandlersPerPoster; ++i)
        {
            const std::size_t index = static_cast<std::size_t>(poster * kHandlersPerPoster + i);
            auto mark_hit = [&hits, index]()
            {
                ++hits[index];
            };
            ctx.post(mark_hit);
        }
    };

    std::vector<std::thread> runners;
    for (int i = 0; i < kRunThreads; ++i)
    {
        runners.emplace_back(run_context);
    }
    std::vector<std::thread> posters;
    for (int p = 0; p < kPostThreads; ++p)
    {
        posters.emplace_back(post_batch, p);
    }
    for (std::thread& poster : posters)
    {
        poster.join();
    }
    guard.reset();
    for (std::thread& runner : runners)
    {
        runner.join();
    }

    SPANIO_CHECK(handlers_run.load() == static_cast<std::size_t>(kMultiThreadHandlers));
    int wrong = 0;
    for (const std::atomic<int>& hit : hits)
    {
        if (hit.load() != 1)
        {
            ++wrong;
        }
    }
    SPANIO_CHECK(wrong == 0);
    SPANIO_CHECK(ctx.stopped());
}

// Case 11: an exception from a handler escapes run(); a second run()
// continues with the remaining handlers
SPANIO_TEST(handler_exception_propagates)
{
    io_context ctx;
    int calls = 0;
    auto throw_failure = []()
    {
        throw std::runtime_error("handler failure");
    };
    auto count_call = [&]()
    {
        ++calls;
    };
    ctx.post(throw_failure);
    ctx.post(count_call);
    bool caught = false;
    try
    {
        ctx.run();
    }
    catch (const std::runtime_error&)
    {
        caught = true;
    }
    SPANIO_CHECK(caught);
    SPANIO_CHECK(calls == 0);
    SPANIO_CHECK(!ctx.stopped());
    SPANIO_CHECK(!ctx.running_in_this_thread());
    SPANIO_CHECK(ctx.run() == 1);
    SPANIO_CHECK(calls == 1);
}

// Case 12: the destructor destroys queued handlers without calling them
SPANIO_TEST(destructor_destroys_pending_handlers)
{
    std::shared_ptr<int> token = std::make_shared<int>(0);
    bool called = false;
    {
        io_context ctx;
        for (long i = 0; i < kDestroyedHandlers; ++i)
        {
            auto mark_called = [token, &called]()
            {
                called = true;
            };
            // Moved, so only the queued copy keeps a reference to token
            ctx.post(std::move(mark_called));
        }
        SPANIO_CHECK(token.use_count() == kSoleOwner + kDestroyedHandlers);
    }
    SPANIO_CHECK(token.use_count() == kSoleOwner);
    SPANIO_CHECK(!called);
}

// Case 13: a recv completes once the peer sends; its handler gets the data,
// and the pending I/O counts as work so run() waits for it
SPANIO_TEST(io_completion_invokes_handler)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        socket_guard client;
        socket_guard server;
        make_tcp_pair(client, server);
        std::unique_ptr<io_context> ctx = make_context(backend);
        SPANIO_REQUIRE(!spanio::detail::context_access::register_handle(*ctx, server.get()));

        char buffer[kBufferSize] = {};
        io_result result{};
        int calls = 0;
        std::thread::id runner;
        auto record_result = [&](const io_result& r)
        {
            result = r;
            ++calls;
            runner = std::this_thread::get_id();
        };
        auto delayed_send = [&]()
        {
            helper_delay();
            send_message(client.get());
        };
        spanio::detail::io_op_base* op = make_io_op(record_result);
        prepare_recv(*op, server.get(), buffer, sizeof(buffer));
        spanio::detail::context_access::start_io(*ctx, op);

        std::thread peer(delayed_send);
        const test_clock::time_point start = test_clock::now();
        const std::size_t ran = ctx->run();
        const long long waited = elapsed_ms(start);
        peer.join();

        SPANIO_CHECK(ran == 1);
        SPANIO_CHECK(calls == 1);
        SPANIO_CHECK(runner == std::this_thread::get_id());
        SPANIO_CHECK(!result.m_ec);
        SPANIO_CHECK(result.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(buffer, kMessage, kMessageLength) == 0);
        SPANIO_CHECK(waited >= kMinWakeMs);
        SPANIO_CHECK(ctx->stopped());
        SPANIO_CHECK(!spanio::detail::context_access::unregister_handle(*ctx, server.get()));
        check_fast_destruction(ctx);
    }
}

// Case 14: cancelling a pending recv from a handler delivers
// operation_canceled to its completion handler, then run() returns
SPANIO_TEST(cancel_pending_io_reports_canceled)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        socket_guard client;
        socket_guard server;
        make_tcp_pair(client, server);
        std::unique_ptr<io_context> ctx = make_context(backend);
        SPANIO_REQUIRE(!spanio::detail::context_access::register_handle(*ctx, server.get()));

        char buffer[kBufferSize] = {};
        io_result result{};
        int calls = 0;
        const spanio::detail::native_handle_t handle = server.get();
        io_context& context = *ctx;
        auto record_result = [&](const io_result& r)
        {
            result = r;
            ++calls;
        };
        auto cancel_recv = [&context, handle]()
        {
            spanio::detail::context_access::cancel_handle(context, handle);
        };
        spanio::detail::io_op_base* op = make_io_op(record_result);
        prepare_recv(*op, server.get(), buffer, sizeof(buffer));
        spanio::detail::context_access::start_io(*ctx, op);
        ctx->post(cancel_recv);

        const test_clock::time_point start = test_clock::now();
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(elapsed_ms(start) < kWakeLimitMs);
        SPANIO_CHECK(calls == 1);
        SPANIO_CHECK(result.m_ec == std::make_error_code(std::errc::operation_canceled));
        SPANIO_CHECK(result.m_bytes == 0);
        SPANIO_CHECK(!spanio::detail::context_access::unregister_handle(*ctx, server.get()));
        check_fast_destruction(ctx);
    }
}

// Case 15: destroying a context whose I/O was cancelled but never collected
// destroys the handler without calling it, quickly and without leaking
SPANIO_TEST(destructor_drains_canceled_io)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        socket_guard client;
        socket_guard server;
        make_tcp_pair(client, server);
        std::shared_ptr<int> token = std::make_shared<int>(0);
        bool called = false;
        char buffer[kBufferSize] = {};
        {
            std::unique_ptr<io_context> ctx = make_context(backend);
            SPANIO_REQUIRE(!spanio::detail::context_access::register_handle(*ctx, server.get()));
            auto mark_called = [token, &called](const io_result&)
            {
                called = true;
            };
            // Moved, so only the operation keeps a reference to token
            spanio::detail::io_op_base* op = make_io_op(std::move(mark_called));
            prepare_recv(*op, server.get(), buffer, sizeof(buffer));
            spanio::detail::context_access::start_io(*ctx, op);
            SPANIO_CHECK(token.use_count() == kSoleOwner + 1);
            // Unregistering is what a socket does before it closes; the recv
            // completes as cancelled but nobody runs the context
            SPANIO_CHECK(!spanio::detail::context_access::unregister_handle(*ctx, server.get()));
            check_fast_destruction(ctx);
        }
        SPANIO_CHECK(token.use_count() == kSoleOwner);
        SPANIO_CHECK(!called);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
