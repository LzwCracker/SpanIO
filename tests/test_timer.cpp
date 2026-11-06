// ============================================================================
// File purpose: Unit tests for steady_timer (timer.h) and its integration
//               with io_context. Every case runs once per proactor backend
//               of the platform, because timers shorten the proactor wait.
// Usage:        Built as the test_timer executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_timer cancel`.
//
//               Timing checks are strict in one direction only: a timer must
//               not fire early. Upper bounds are generous, so a busy machine
//               does not fail a case.
//
//               Declaration order in each case: recorders, context, timers.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#include "spanio/tcp.h"
#include "spanio/timer.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::steady_timer;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;
using spanio::test::make_tcp_pair;
using spanio::test::socket_guard;

namespace {

// Clock used for time measurements
using test_clock = steady_timer::clock;

// Delay of the timers that are expected to fire
constexpr std::chrono::milliseconds kShortDelay(50);

// Delay of a timer that must not fire during a case
constexpr std::chrono::seconds kLongDelay(60);

// Pause longer than kShortDelay, so a timer has surely expired
constexpr std::chrono::milliseconds kPastExpiry(100);

// Delay before a helper thread acts, so run() is surely blocked
constexpr std::chrono::milliseconds kHelperDelay(100);

// Generous upper bound for anything that should finish soon
constexpr std::chrono::seconds kGenerousLimit(10);

// Spacing of the deadlines in the ordering case
constexpr std::chrono::milliseconds kOrderStep(30);

// Timers in the ordering case, one more than the two at equal deadlines
constexpr int kOrderTimers = 3;

// Waits on one timer in the multiple-waits case
constexpr int kMultipleWaits = 3;

// Interval of the periodic case
constexpr std::chrono::milliseconds kPeriod(20);

// Ticks of the periodic case
constexpr int kPeriodicTicks = 5;

// Timers in the multi-threaded case
constexpr int kManyTimers = 1000;

// Threads calling run() in the multi-threaded case
constexpr int kRunThreads = 4;

// Timers in that case spread over this many milliseconds
constexpr int kDelaySpreadMs = 100;

// Size of the receive buffer in the read-timeout case
constexpr std::size_t kBufferSize = 64;

// Time since a starting point
// Parameter start: time the measurement began
// Return value: the elapsed time
test_clock::duration elapsed(test_clock::time_point start)
{
    return test_clock::now() - start;
}

// Result of a wait, recorded by a test
struct wait_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // When the handler ran
    test_clock::time_point m_when;
};

// Handler that records its result in a wait_record
class record_wait
{
public:
    // Parameter record: where to store the result; must outlive the handler
    explicit record_wait(wait_record& record)
        : m_record(&record)
    {
    }

    // Parameter ec: wait result
    void operator()(std::error_code ec)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
        m_record->m_when = test_clock::now();
    }

private:
    // Destination of the recorded values
    wait_record* m_record;
};

}

// Case 1: a wait completes once, with no error, and not before the expiry
SPANIO_TEST(wait_fires_after_expiry)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        timer.async_wait(record_wait(wait));
        SPANIO_CHECK(wait.m_calls == 0);

        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(!wait.m_ec);
        SPANIO_CHECK(wait.m_when >= timer.expiry());
        SPANIO_CHECK(wait.m_when - timer.expiry() < kGenerousLimit);
    }
}

// Case 2: waiting on an expired timer succeeds through run(), never inline
SPANIO_TEST(expired_timer_fires_through_run)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record default_timer;
        wait_record past_timer;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer expired_now(*ctx);
        steady_timer expired_earlier(*ctx, test_clock::now() - kLongDelay);

        expired_now.async_wait(record_wait(default_timer));
        expired_earlier.async_wait(record_wait(past_timer));
        SPANIO_CHECK(default_timer.m_calls == 0);
        SPANIO_CHECK(past_timer.m_calls == 0);

        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(default_timer.m_calls == 1);
        SPANIO_CHECK(past_timer.m_calls == 1);
        SPANIO_CHECK(!default_timer.m_ec);
        SPANIO_CHECK(!past_timer.m_ec);
    }
}

// Case 3: a pending wait counts as work, so run() does not return before it
SPANIO_TEST(pending_wait_counts_as_work)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        timer.async_wait(record_wait(wait));

        ctx->run();
        SPANIO_CHECK(test_clock::now() >= timer.expiry());
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(ctx->stopped());
    }
}

// Case 4: timers fire by deadline, whatever order they were started in, and
// equal deadlines fire in the order they were started
SPANIO_TEST(timers_fire_in_deadline_order)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::vector<int> order;
        std::unique_ptr<io_context> ctx = make_context(backend);
        const test_clock::time_point base = test_clock::now() + kOrderStep;
        steady_timer late(*ctx, base + kOrderStep * kOrderTimers);
        steady_timer equal_first(*ctx, base);
        steady_timer equal_second(*ctx, base);
        steady_timer middle(*ctx, base + kOrderStep);

        auto mark = [&order](int id)
        {
            auto handler = [&order, id](std::error_code)
            {
                order.push_back(id);
            };
            return handler;
        };
        late.async_wait(mark(3));
        equal_first.async_wait(mark(0));
        equal_second.async_wait(mark(1));
        middle.async_wait(mark(2));

        ctx->run();
        const std::vector<int> expected = {0, 1, 2, 3};
        SPANIO_CHECK(order == expected);
    }
}

// Case 5: while run() waits for a far deadline, a nearer timer added from
// another thread fires on time
SPANIO_TEST(earlier_timer_wakes_blocked_run)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record near_wait;
        wait_record far_wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer far_timer(*ctx, kLongDelay);
        steady_timer near_timer(*ctx);
        far_timer.async_wait(record_wait(far_wait));

        auto on_near = [&](std::error_code ec)
        {
            record_wait recorder(near_wait);
            recorder(ec);
            far_timer.cancel();
        };
        auto add_near_timer = [&]()
        {
            std::this_thread::sleep_for(kHelperDelay);
            near_timer.expires_after(kShortDelay);
            near_timer.async_wait(on_near);
        };
        std::thread helper(add_near_timer);
        const test_clock::time_point start = test_clock::now();
        ctx->run();
        helper.join();

        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(near_wait.m_calls == 1);
        SPANIO_CHECK(!near_wait.m_ec);
        SPANIO_CHECK(near_wait.m_when >= near_timer.expiry());
        SPANIO_CHECK(far_wait.m_ec == std::errc::operation_canceled);
    }
}

// Case 6: cancel() reports how many waits it cancelled, and they complete
// with operation_canceled straight away
SPANIO_TEST(cancel_reports_operation_canceled)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kLongDelay);
        SPANIO_CHECK(timer.cancel() == 0);

        timer.async_wait(record_wait(wait));
        SPANIO_CHECK(timer.cancel() == 1);
        SPANIO_CHECK(timer.cancel() == 0);
        SPANIO_CHECK(wait.m_calls == 0);

        const test_clock::time_point start = test_clock::now();
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(wait.m_ec == std::errc::operation_canceled);
    }
}

// Case 7: once an expired timer's handler is queued, cancel() is too late:
// it returns 0 and the handler sees success
SPANIO_TEST(cancel_after_expiry_is_too_late)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::size_t cancelled = 1;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        timer.async_wait(record_wait(wait));

        // The first handler outlasts the timer. Starting the second one
        // queues the expired wait behind it, so cancel() finds nothing.
        auto wait_past_expiry = []()
        {
            std::this_thread::sleep_for(kPastExpiry);
        };
        auto cancel_timer = [&]()
        {
            cancelled = timer.cancel();
        };
        ctx->post(wait_past_expiry);
        ctx->post(cancel_timer);

        ctx->run();
        SPANIO_CHECK(cancelled == 0);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(!wait.m_ec);
    }
}

// Case 8: setting a new expiry cancels pending waits, and a new wait uses
// the new expiry
SPANIO_TEST(expires_after_cancels_pending_wait)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record old_wait;
        wait_record new_wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kLongDelay);
        timer.async_wait(record_wait(old_wait));

        SPANIO_CHECK(timer.expires_after(kShortDelay) == 1);
        timer.async_wait(record_wait(new_wait));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(old_wait.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(new_wait.m_calls == 1);
        SPANIO_CHECK(!new_wait.m_ec);
        SPANIO_CHECK(new_wait.m_when >= timer.expiry());
    }
}

// Case 9: destroying a timer completes its waits as cancelled
SPANIO_TEST(destroy_timer_with_pending_wait)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<steady_timer> timer(new steady_timer(*ctx, kLongDelay));
        timer->async_wait(record_wait(wait));
        timer.reset();

        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(wait.m_ec == std::errc::operation_canceled);
    }
}

// Case 10: every wait on one timer completes when it expires
SPANIO_TEST(multiple_waits_on_one_timer)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::vector<wait_record> waits(static_cast<std::size_t>(kMultipleWaits));
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        for (wait_record& wait : waits)
        {
            timer.async_wait(record_wait(wait));
        }

        SPANIO_CHECK(ctx->run() == static_cast<std::size_t>(kMultipleWaits));
        for (const wait_record& wait : waits)
        {
            SPANIO_CHECK(wait.m_calls == 1);
            SPANIO_CHECK(!wait.m_ec);
            SPANIO_CHECK(wait.m_when >= timer.expiry());
        }
    }
}

// Case 11: a handler can re-arm its timer; periods add up without running
// short
SPANIO_TEST(periodic_rearm)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        int ticks = 0;
        std::unique_ptr<io_context> ctx = make_context(backend);
        const test_clock::time_point start = test_clock::now();
        steady_timer timer(*ctx, start + kPeriod);

        std::function<void(std::error_code)> on_tick;
        on_tick = [&](std::error_code ec)
        {
            SPANIO_CHECK(!ec);
            ++ticks;
            if (ticks < kPeriodicTicks)
            {
                timer.expires_at(timer.expiry() + kPeriod);
                timer.async_wait(on_tick);
            }
        };
        timer.async_wait(on_tick);

        ctx->run();
        SPANIO_CHECK(ticks == kPeriodicTicks);
        SPANIO_CHECK(elapsed(start) >= kPeriod * kPeriodicTicks);
    }
}

// Case 12: the usual timeout pattern: a timer cancels a read that takes too
// long, and the read's handler sees operation_canceled
SPANIO_TEST(timer_cancels_pending_read)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        socket_guard client;
        socket_guard server;
        make_tcp_pair(client, server);
        spanio::tcp::socket reader(*ctx);
        SPANIO_REQUIRE(!reader.assign(server.release()));

        char buffer[kBufferSize] = {};
        std::error_code read_ec;
        int read_calls = 0;
        auto on_read = [&](std::error_code ec, std::size_t)
        {
            ++read_calls;
            read_ec = ec;
        };
        steady_timer timeout(*ctx, kShortDelay);
        auto on_timeout = [&](std::error_code ec)
        {
            if (!ec)
            {
                reader.cancel();
            }
        };
        reader.async_read_some(spanio::buffer(buffer), on_read);
        timeout.async_wait(on_timeout);

        const test_clock::time_point start = test_clock::now();
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(read_calls == 1);
        SPANIO_CHECK(read_ec == std::errc::operation_canceled);
    }
}

// Case 13: stop() leaves a timer pending; after restart() it fires
SPANIO_TEST(stop_keeps_timers_pending)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kPastExpiry);
        timer.async_wait(record_wait(wait));

        io_context& context = *ctx;
        auto stop_context = [&context]()
        {
            context.stop();
        };
        ctx->post(stop_context);
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(wait.m_calls == 0);

        ctx->restart();
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(!wait.m_ec);
        SPANIO_CHECK(wait.m_when >= timer.expiry());
    }
}

// Case 14: poll() does not wait for a timer, and runs it once it is due
SPANIO_TEST(poll_fires_only_due_timers)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        wait_record wait;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        timer.async_wait(record_wait(wait));

        const test_clock::time_point start = test_clock::now();
        SPANIO_CHECK(ctx->poll() == 0);
        SPANIO_CHECK(elapsed(start) < kShortDelay);
        SPANIO_CHECK(wait.m_calls == 0);
        SPANIO_CHECK(!ctx->stopped());

        std::this_thread::sleep_for(kPastExpiry);
        SPANIO_CHECK(ctx->poll() == 1);
        SPANIO_CHECK(wait.m_calls == 1);
        SPANIO_CHECK(!wait.m_ec);
    }
}

// Case 15: handlers that keep posting more handlers do not stop a timer
// from firing
SPANIO_TEST(handler_flood_does_not_starve_timer)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        bool fired = false;
        std::unique_ptr<io_context> ctx = make_context(backend);
        steady_timer timer(*ctx, kShortDelay);
        auto on_timer = [&fired](std::error_code)
        {
            fired = true;
        };
        timer.async_wait(on_timer);

        const test_clock::time_point start = test_clock::now();
        std::function<void()> flood;
        flood = [&]()
        {
            if (!fired && elapsed(start) < kGenerousLimit)
            {
                ctx->post(flood);
            }
        };
        ctx->post(flood);

        ctx->run();
        SPANIO_CHECK(fired);
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
    }
}

// Case 16: with four run() threads, a thousand timers each fire exactly
// once and none fires early
SPANIO_TEST(multi_thread_timers)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::vector<std::atomic<int>> fired(static_cast<std::size_t>(kManyTimers));
        std::atomic<int> early(0);
        std::atomic<int> failed(0);
        std::unique_ptr<io_context> ctx = make_context(backend);

        std::vector<std::unique_ptr<steady_timer>> timers;
        for (int i = 0; i < kManyTimers; ++i)
        {
            const std::size_t index = static_cast<std::size_t>(i);
            fired[index].store(0);
            const std::chrono::milliseconds delay(i % kDelaySpreadMs);
            timers.emplace_back(new steady_timer(*ctx, delay));
            const test_clock::time_point expiry = timers.back()->expiry();
            auto on_timer = [&fired, &early, &failed, index, expiry](std::error_code ec)
            {
                if (ec)
                {
                    ++failed;
                }
                if (test_clock::now() < expiry)
                {
                    ++early;
                }
                ++fired[index];
            };
            timers.back()->async_wait(on_timer);
        }

        io_context& context = *ctx;
        auto run_context = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> runners;
        for (int i = 0; i < kRunThreads; ++i)
        {
            runners.emplace_back(run_context);
        }
        for (std::thread& runner : runners)
        {
            runner.join();
        }

        int wrong = 0;
        for (const std::atomic<int>& count : fired)
        {
            if (count.load() != 1)
            {
                ++wrong;
            }
        }
        SPANIO_CHECK(wrong == 0);
        SPANIO_CHECK(early.load() == 0);
        SPANIO_CHECK(failed.load() == 0);
    }
}

// Case 17: destroying a context discards a wait: its handler is released
// without being called
SPANIO_TEST(context_destructor_discards_wait)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::shared_ptr<int> token = std::make_shared<int>(0);
        bool called = false;
        {
            std::unique_ptr<io_context> ctx = make_context(backend);
            steady_timer timer(*ctx, kLongDelay);
            auto on_timer = [token, &called](std::error_code)
            {
                called = true;
            };
            // Moved, so only the wait keeps a reference to token
            timer.async_wait(std::move(on_timer));
            SPANIO_CHECK(token.use_count() == 2);
        }
        SPANIO_CHECK(token.use_count() == 1);
        SPANIO_CHECK(!called);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
