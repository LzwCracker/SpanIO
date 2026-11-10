// ============================================================================
// File purpose: Unit tests for strand, make_strand and bind_executor
//               (strand.h): ordering, mutual exclusion under several run()
//               threads, post / dispatch semantics, work counting,
//               exceptions, destruction, and handlers of timers and TCP
//               operations bound to a strand. The TCP cases run once per
//               proactor backend of the platform.
// Usage:        Built as the test_strand executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_strand dispatch`.
//
//               Handlers running on worker threads never use SPANIO_CHECK
//               (its failure counter is not thread-safe). They record into
//               atomics or into state that only strand handlers touch, and the
//               main thread checks after the run() threads have joined.
//
//               Declaration order in each case: recorders, context, strands,
//               I/O objects, so I/O objects are destroyed first.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-08
// ============================================================================

#include "spanio/strand.h"
#include "spanio/tcp.h"
#include "spanio/timer.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::steady_timer;
namespace tcp = spanio::tcp;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;

namespace {

// Strand type used by every case
using strand_type = spanio::strand<io_context::executor_type>;

// Threads calling run() in the multi-threaded cases
constexpr int kRunThreads = 4;

// Threads posting to one strand at the same time in the mutual-exclusion case
constexpr int kProducers = 4;

// Handlers each producer posts in that case
constexpr int kHandlersPerProducer = 5000;

// Handlers posted in the ordering case
constexpr int kOrderedHandlers = 1000;

// Handlers posted alternately to a strand and its copy
constexpr int kCopyHandlers = 10000;

// Limit for anything a case waits for in a spin loop, so a broken strand
// fails the case instead of hanging it
constexpr std::chrono::seconds kWaitLimit(5);

// Handlers queued in the exception case
constexpr int kExceptionHandlers = 5;

// Position (1-based) of the handler that throws in that case
constexpr int kThrowingHandler = 2;

// Length of the chain of handlers that each post the next one
constexpr int kChainLength = 50;

// Handlers queued behind a slow one in the work-counting case
constexpr int kQueuedBehindSlow = 5;

// How long the slow handler of the work-counting case takes
constexpr std::chrono::milliseconds kSlowHandler(100);

// Handlers that capture the token in the destruction case
constexpr int kPendingHandlers = 3;

// Timers bound to the strand in the timer case
constexpr int kTimers = 200;

// Timer delays in that case spread over this many milliseconds
constexpr int kTimerSpreadMs = 50;

// Delay of the timer that is cancelled before it can fire
constexpr std::chrono::seconds kLongDelay(60);

// Bytes echoed through the strand-bound TCP session
constexpr std::size_t kEchoBytes = 1024 * 1024;

// Size of each write and of each receive buffer of that session
constexpr std::size_t kEchoChunk = 64 * 1024;

// The byte pattern repeats with this period; a prime, so it does not line up
// with chunk boundaries
constexpr std::size_t kPatternPeriod = 251;

// Receive buffer of the move-only / cancel case
constexpr std::size_t kSmallBuffer = 16;

// Records handlers that run at the same time as another one
class overlap_detector
{
public:
    // A handler starts
    void enter()
    {
        if (m_inside.fetch_add(1) != 0)
        {
            m_violations.fetch_add(1);
        }
    }

    // A handler ends
    void leave()
    {
        m_inside.fetch_sub(1);
    }

    // Number of handlers that started while another one was running
    // Return value: the count
    int violations() const
    {
        return m_violations.load();
    }

private:
    // Handlers running right now
    std::atomic<int> m_inside{0};
    // Handlers that found another one running when they started
    std::atomic<int> m_violations{0};
};

// Marks the body of a handler for an overlap_detector, also when it throws
class overlap_scope
{
public:
    // Parameter detector: detector to report to; must outlive the scope
    explicit overlap_scope(overlap_detector& detector)
        : m_detector(detector)
    {
        m_detector.enter();
        // Give another thread the chance to enter at the same time, which a
        // broken strand would allow
        std::this_thread::yield();
    }

    ~overlap_scope()
    {
        m_detector.leave();
    }

    overlap_scope(const overlap_scope&) = delete;
    overlap_scope& operator=(const overlap_scope&) = delete;

private:
    // Detector the handler reports to
    overlap_detector& m_detector;
};

// Call run() of a context from several threads and wait for all of them
// Parameter ctx:   context to run
// Parameter count: number of threads
void run_in_threads(io_context& ctx, int count)
{
    std::vector<std::thread> threads;
    auto runner = [&ctx]()
    {
        ctx.run();
    };
    for (int i = 0; i < count; ++i)
    {
        threads.emplace_back(runner);
    }
    for (std::thread& thread : threads)
    {
        thread.join();
    }
}

// Spin until a flag is set or kWaitLimit passes
// Parameter flag: flag to wait for
// Return value: true if the flag was set in time
bool wait_for(const std::atomic<bool>& flag)
{
    const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
    while (!flag.load())
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

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

// Connect a client to an acceptor through the async API on one thread;
// restarts the context afterwards
// Parameter ctx:    context of all three objects
// Parameter acc:    listening acceptor
// Parameter client: socket that is not open yet
// Parameter server: receives the accepted socket
void connect_pair(io_context& ctx, tcp::acceptor& acc, tcp::socket& client, tcp::socket& server)
{
    std::error_code accept_ec;
    std::error_code connect_ec;
    auto on_accept = [&](std::error_code ec, tcp::socket peer)
    {
        accept_ec = ec;
        server = std::move(peer);
    };
    auto on_connect = [&](std::error_code ec)
    {
        connect_ec = ec;
    };
    acc.async_accept(on_accept);
    client.async_connect(listening_endpoint(acc), on_connect);
    ctx.run();
    ctx.restart();
    SPANIO_REQUIRE(!accept_ec);
    SPANIO_REQUIRE(!connect_ec);
    SPANIO_REQUIRE(server.is_open());
}

// Echo over one TCP connection in which every handler (reads, writes and
// extra posted handlers) is bound to one strand. Its state is deliberately
// unsynchronized: the strand alone must keep the handlers apart, and TSan
// checks that on Linux.
class echo_session
{
public:
    // Parameter strand: strand every handler is bound to
    // Parameter client: connected client side
    // Parameter server: connected server side
    echo_session(strand_type& strand, tcp::socket& client, tcp::socket& server)
        : m_strand(strand)
        , m_client(client)
        , m_server(server)
        , m_sent(kEchoBytes)
        , m_server_buffer(kEchoChunk)
        , m_client_buffer(kEchoChunk)
    {
        for (std::size_t i = 0; i < kEchoBytes; ++i)
        {
            m_sent[i] = static_cast<char>(i % kPatternPeriod);
        }
    }

    // Start the server read, the client writes and the client read
    void start()
    {
        server_read();
        client_write_next();
        client_read();
    }

    // Detector of overlapping handlers
    overlap_detector m_detector;
    // Handlers that ran while running_in_this_thread() was false
    std::atomic<int> m_outside{0};
    // Bytes the client received back
    std::vector<char> m_received;
    // First unexpected error of any operation
    std::error_code m_error;
    // Whether the client saw the end of the echoed stream
    bool m_client_eof = false;
    // Extra handlers posted to the strand by the I/O handlers
    int m_plain_posted = 0;
    // Extra handlers that ran
    int m_plain_run = 0;

private:
    // Handler with (error, bytes) that calls a member function
    class io_call
    {
    public:
        // Signature of the member functions it calls
        using member = void (echo_session::*)(std::error_code, std::size_t);

        // Parameter session: session to call; must outlive the operation
        // Parameter function: member to call
        io_call(echo_session* session, member function)
            : m_session(session)
            , m_function(function)
        {
        }

        // Parameter ec:    operation result
        // Parameter bytes: bytes transferred
        void operator()(std::error_code ec, std::size_t bytes)
        {
            (m_session->*m_function)(ec, bytes);
        }

    private:
        // Session to call
        echo_session* m_session;
        // Member function to call
        member m_function;
    };

    // Extra handler posted to the strand from an I/O handler
    class plain_call
    {
    public:
        // Parameter session: session to record in
        explicit plain_call(echo_session* session)
            : m_session(session)
        {
        }

        // Record one run, checked like an I/O handler
        void operator()()
        {
            overlap_scope scope(m_session->m_detector);
            m_session->check_in_strand();
            ++m_session->m_plain_run;
        }

    private:
        // Session to record in
        echo_session* m_session;
    };

    // Handler bound to the strand that calls a member function
    // Parameter function: member to call
    // Return value: the bound handler
    spanio::executor_binder<io_call, strand_type> bind(io_call::member function)
    {
        return spanio::bind_executor(m_strand, io_call(this, function));
    }

    // Record a handler that runs outside the strand, and post an extra
    // handler to the strand
    void note_io_handler()
    {
        check_in_strand();
        ++m_plain_posted;
        m_strand.post(plain_call(this));
    }

    // Record a handler that runs outside the strand
    void check_in_strand()
    {
        if (!m_strand.running_in_this_thread())
        {
            m_outside.fetch_add(1);
        }
    }

    // Remember the first unexpected error
    // Parameter ec: the error
    void fail(std::error_code ec)
    {
        if (!m_error)
        {
            m_error = ec;
        }
    }

    // Start a server read
    void server_read()
    {
        m_server.async_read_some(spanio::buffer(m_server_buffer), bind(&echo_session::on_server_read));
    }

    // Server read done: echo the bytes, or end the stream at EOF
    // Parameter ec:    read result
    // Parameter bytes: bytes read
    void on_server_read(std::error_code ec, std::size_t bytes)
    {
        overlap_scope scope(m_detector);
        note_io_handler();
        if (ec == spanio::error::misc_errors::eof)
        {
            m_server.shutdown(tcp::shutdown_type::send);
            return;
        }
        if (ec)
        {
            fail(ec);
            return;
        }
        m_server.async_write(spanio::buffer(m_server_buffer.data(), bytes),
                             bind(&echo_session::on_server_write));
    }

    // Server write done: read the next part
    // Parameter ec:    write result
    // Parameter bytes: bytes written (unused)
    void on_server_write(std::error_code ec, std::size_t /*bytes*/)
    {
        overlap_scope scope(m_detector);
        note_io_handler();
        if (ec)
        {
            fail(ec);
            return;
        }
        server_read();
    }

    // Send the next chunk, or shut down sending after the last one
    void client_write_next()
    {
        if (m_write_offset == kEchoBytes)
        {
            m_client.shutdown(tcp::shutdown_type::send);
            return;
        }
        const std::size_t size = std::min(kEchoChunk, kEchoBytes - m_write_offset);
        m_client.async_write(spanio::buffer(m_sent.data() + m_write_offset, size),
                             bind(&echo_session::on_client_write));
    }

    // Client write done: send the next chunk
    // Parameter ec:    write result
    // Parameter bytes: bytes written
    void on_client_write(std::error_code ec, std::size_t bytes)
    {
        overlap_scope scope(m_detector);
        note_io_handler();
        if (ec)
        {
            fail(ec);
            return;
        }
        m_write_offset += bytes;
        client_write_next();
    }

    // Start a client read
    void client_read()
    {
        m_client.async_read_some(spanio::buffer(m_client_buffer), bind(&echo_session::on_client_read));
    }

    // Client read done: keep the bytes and read again, until EOF
    // Parameter ec:    read result
    // Parameter bytes: bytes read
    void on_client_read(std::error_code ec, std::size_t bytes)
    {
        overlap_scope scope(m_detector);
        note_io_handler();
        if (ec == spanio::error::misc_errors::eof)
        {
            m_client_eof = true;
            return;
        }
        if (ec)
        {
            fail(ec);
            return;
        }
        m_received.insert(m_received.end(), m_client_buffer.begin(),
                          m_client_buffer.begin() + static_cast<std::ptrdiff_t>(bytes));
        client_read();
    }

    // Strand of every handler
    strand_type& m_strand;
    // Client side of the connection
    tcp::socket& m_client;
    // Server side of the connection
    tcp::socket& m_server;
    // Bytes the client sends
    std::vector<char> m_sent;
    // Bytes of m_sent already written
    std::size_t m_write_offset = 0;
    // Receive buffer of the server, also the source of its echo
    std::vector<char> m_server_buffer;
    // Receive buffer of the client
    std::vector<char> m_client_buffer;
};

}

// Case 1: post() never runs inline; run() runs each handler and counts it
SPANIO_TEST(post_not_inline)
{
    int calls = 0;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    auto handler = [&calls]()
    {
        ++calls;
    };
    s.post(handler);
    s.post(handler);
    s.post(handler);
    SPANIO_CHECK(calls == 0);
    const std::size_t ran = ctx.run();
    SPANIO_CHECK(calls == 3);
    SPANIO_CHECK(ran == 3);
}

// Case 2: handlers posted from one thread run in the order of posting
SPANIO_TEST(fifo_order)
{
    std::vector<int> order;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    for (int i = 0; i < kOrderedHandlers; ++i)
    {
        auto handler = [&order, i]()
        {
            order.push_back(i);
        };
        s.post(handler);
    }
    ctx.run();
    SPANIO_REQUIRE(order.size() == static_cast<std::size_t>(kOrderedHandlers));
    for (int i = 0; i < kOrderedHandlers; ++i)
    {
        SPANIO_CHECK(order[static_cast<std::size_t>(i)] == i);
    }
}

// Case 3: several threads post while several threads run; handlers never
// overlap, a plain counter stays exact, and each producer's order is kept
SPANIO_TEST(no_concurrency)
{
    overlap_detector detector;
    int counter = 0;
    std::vector<int> last_seen(kProducers, -1);
    std::atomic<int> out_of_order{0};
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    spanio::executor_work_guard guard = spanio::make_work_guard(ctx);

    std::vector<std::thread> runners;
    auto runner = [&ctx]()
    {
        ctx.run();
    };
    for (int i = 0; i < kRunThreads; ++i)
    {
        runners.emplace_back(runner);
    }

    auto producer = [&](int id)
    {
        for (int seq = 0; seq < kHandlersPerProducer; ++seq)
        {
            auto handler = [&detector, &counter, &last_seen, &out_of_order, id, seq]()
            {
                overlap_scope scope(detector);
                ++counter;
                int& last = last_seen[static_cast<std::size_t>(id)];
                if (last + 1 != seq)
                {
                    out_of_order.fetch_add(1);
                }
                last = seq;
            };
            s.post(handler);
        }
    };
    std::vector<std::thread> producers;
    for (int id = 0; id < kProducers; ++id)
    {
        producers.emplace_back(producer, id);
    }
    for (std::thread& thread : producers)
    {
        thread.join();
    }
    guard.reset();
    for (std::thread& thread : runners)
    {
        thread.join();
    }

    SPANIO_CHECK(detector.violations() == 0);
    SPANIO_CHECK(counter == kProducers * kHandlersPerProducer);
    SPANIO_CHECK(out_of_order.load() == 0);
}

// Case 4: two strands run in parallel: a handler of A waits for a handler
// of B, which only works if B is not held up by A
SPANIO_TEST(strands_run_in_parallel)
{
    std::atomic<bool> b_ran{false};
    bool a_saw_b = false;
    io_context ctx;
    strand_type a = spanio::make_strand(ctx);
    strand_type b = spanio::make_strand(ctx);
    auto on_a = [&]()
    {
        a_saw_b = wait_for(b_ran);
    };
    auto on_b = [&]()
    {
        b_ran.store(true);
    };
    a.post(on_a);
    b.post(on_b);
    run_in_threads(ctx, 2);
    SPANIO_CHECK(a_saw_b);
}

// Case 5: copies share the domain (equal, ordered, never overlapping);
// separate make_strand calls give different domains
SPANIO_TEST(copies_share_domain)
{
    overlap_detector detector;
    std::vector<int> order;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    const strand_type copy = s;
    const strand_type other = spanio::make_strand(ctx);
    SPANIO_CHECK(s == copy);
    SPANIO_CHECK(!(s != copy));
    SPANIO_CHECK(s != other);
    SPANIO_CHECK(s.get_inner_executor() == ctx.get_executor());
    SPANIO_CHECK(&s.context() == &ctx);

    for (int i = 0; i < kCopyHandlers; ++i)
    {
        auto handler = [&detector, &order, i]()
        {
            overlap_scope scope(detector);
            order.push_back(i);
        };
        if (i % 2 == 0)
        {
            s.post(handler);
        }
        else
        {
            copy.post(handler);
        }
    }
    run_in_threads(ctx, kRunThreads);

    SPANIO_CHECK(detector.violations() == 0);
    SPANIO_REQUIRE(order.size() == static_cast<std::size_t>(kCopyHandlers));
    bool in_order = true;
    for (int i = 0; i < kCopyHandlers; ++i)
    {
        if (order[static_cast<std::size_t>(i)] != i)
        {
            in_order = false;
        }
    }
    SPANIO_CHECK(in_order);
}

// Case 6: dispatch() inside a strand handler runs inline;
// running_in_this_thread() is true only inside the strand's handlers
SPANIO_TEST(dispatch_inline_inside_strand)
{
    bool outer_in_strand = false;
    bool inner_ran_inline = false;
    bool inner_in_strand = false;
    bool plain_in_context = false;
    bool plain_in_strand = true;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    SPANIO_CHECK(!s.running_in_this_thread());

    auto inner = [&]()
    {
        inner_ran_inline = true;
        inner_in_strand = s.running_in_this_thread();
    };
    bool ran_before_return = false;
    auto outer = [&]()
    {
        outer_in_strand = s.running_in_this_thread();
        s.dispatch(inner);
        // inner must already have run when dispatch() returns
        ran_before_return = inner_ran_inline;
    };
    auto plain = [&]()
    {
        plain_in_context = ctx.running_in_this_thread();
        plain_in_strand = s.running_in_this_thread();
    };
    s.post(outer);
    ctx.post(plain);
    ctx.run();

    SPANIO_CHECK(outer_in_strand);
    SPANIO_CHECK(ran_before_return);
    SPANIO_CHECK(inner_in_strand);
    SPANIO_CHECK(plain_in_context);
    SPANIO_CHECK(!plain_in_strand);
    SPANIO_CHECK(!s.running_in_this_thread());
}

// Case 7: dispatch() from a thread that does not run the context queues
SPANIO_TEST(dispatch_outside_run_queues)
{
    bool ran = false;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    auto handler = [&ran]()
    {
        ran = true;
    };
    s.dispatch(handler);
    SPANIO_CHECK(!ran);
    ctx.run();
    SPANIO_CHECK(ran);
}

// Case 8: dispatch() from a plain handler of the context runs inline when
// the strand is idle, and queues behind the running handler when it is busy
SPANIO_TEST(dispatch_from_context_handler)
{
    // Idle strand: inline
    {
        bool ran = false;
        bool ran_before_return = false;
        io_context ctx;
        strand_type s = spanio::make_strand(ctx);
        auto handler = [&ran]()
        {
            ran = true;
        };
        auto plain = [&]()
        {
            s.dispatch(handler);
            ran_before_return = ran;
        };
        ctx.post(plain);
        ctx.run();
        SPANIO_CHECK(ran_before_return);
    }

    // Busy strand on another thread: queued until the busy handler returns
    {
        std::atomic<bool> busy_started{false};
        std::atomic<bool> release{false};
        std::atomic<bool> busy_done{false};
        bool started_in_time = false;
        bool released_in_time = false;
        bool ran_inline = true;
        bool ran_after_busy = false;
        std::atomic<bool> dispatched_ran{false};
        io_context ctx;
        strand_type s = spanio::make_strand(ctx);
        auto busy = [&]()
        {
            busy_started.store(true);
            released_in_time = wait_for(release);
            busy_done.store(true);
        };
        auto dispatched = [&]()
        {
            ran_after_busy = busy_done.load();
            dispatched_ran.store(true);
        };
        auto plain = [&]()
        {
            started_in_time = wait_for(busy_started);
            s.dispatch(dispatched);
            ran_inline = dispatched_ran.load();
            release.store(true);
        };
        s.post(busy);
        ctx.post(plain);
        run_in_threads(ctx, 2);
        SPANIO_CHECK(started_in_time);
        SPANIO_CHECK(released_in_time);
        SPANIO_CHECK(!ran_inline);
        SPANIO_CHECK(dispatched_ran.load());
        SPANIO_CHECK(ran_after_busy);
    }
}

// Case 9: post() inside a strand handler runs after the current handler and
// after the handlers already queued
SPANIO_TEST(post_inside_strand_runs_after)
{
    std::vector<std::string> order;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    auto third = [&order]()
    {
        order.push_back("c");
    };
    auto first = [&]()
    {
        order.push_back("a");
        s.post(third);
        order.push_back("a-end");
    };
    auto second = [&order]()
    {
        order.push_back("b");
    };
    s.post(first);
    s.post(second);
    ctx.run();
    const std::vector<std::string> expected{"a", "a-end", "b", "c"};
    SPANIO_CHECK(order == expected);
}

// Case 10: a handler of A dispatches to idle B: B's handler runs inline and
// both strands report running_in_this_thread() inside it
SPANIO_TEST(nested_strands)
{
    bool inner_ran_inline = false;
    bool a_inside_b = false;
    bool b_inside_b = false;
    bool b_after_inner = true;
    bool a_after_inner = false;
    io_context ctx;
    strand_type a = spanio::make_strand(ctx);
    strand_type b = spanio::make_strand(ctx);
    bool inner_ran = false;
    auto inner = [&]()
    {
        inner_ran = true;
        a_inside_b = a.running_in_this_thread();
        b_inside_b = b.running_in_this_thread();
    };
    auto outer = [&]()
    {
        b.dispatch(inner);
        inner_ran_inline = inner_ran;
        b_after_inner = b.running_in_this_thread();
        a_after_inner = a.running_in_this_thread();
    };
    a.post(outer);
    ctx.run();
    SPANIO_CHECK(inner_ran_inline);
    SPANIO_CHECK(a_inside_b);
    SPANIO_CHECK(b_inside_b);
    SPANIO_CHECK(!b_after_inner);
    SPANIO_CHECK(a_after_inner);
}

// Case 11: queued strand handlers keep run() going; an unused strand is no
// work
SPANIO_TEST(work_counting)
{
    // A strand alone does not keep run() alive
    {
        io_context ctx;
        strand_type s = spanio::make_strand(ctx);
        SPANIO_CHECK(ctx.run() == 0);
    }

    // A chain of handlers that each post the next one
    {
        int links = 0;
        io_context ctx;
        strand_type s = spanio::make_strand(ctx);
        std::function<void()> link;
        link = [&]()
        {
            ++links;
            if (links < kChainLength)
            {
                s.post(link);
            }
        };
        s.post(link);
        run_in_threads(ctx, 2);
        SPANIO_CHECK(links == kChainLength);
    }

    // Handlers waiting in the strand behind a slow one, with an idle second
    // thread: run() returns only after all of them
    {
        std::atomic<int> quick_ran{0};
        bool slow_ran = false;
        io_context ctx;
        strand_type s = spanio::make_strand(ctx);
        auto slow = [&slow_ran]()
        {
            std::this_thread::sleep_for(kSlowHandler);
            slow_ran = true;
        };
        auto quick = [&quick_ran]()
        {
            quick_ran.fetch_add(1);
        };
        s.post(slow);
        for (int i = 0; i < kQueuedBehindSlow; ++i)
        {
            s.post(quick);
        }
        run_in_threads(ctx, 2);
        SPANIO_CHECK(slow_ran);
        SPANIO_CHECK(quick_ran.load() == kQueuedBehindSlow);
    }
}

// Case 12: an exception leaves run(); the rest of the queue runs in order on
// the next run(), and the strand keeps working afterwards
SPANIO_TEST(handler_exception)
{
    std::vector<int> order;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    for (int id = 1; id <= kExceptionHandlers; ++id)
    {
        auto handler = [&order, id]()
        {
            order.push_back(id);
            if (id == kThrowingHandler)
            {
                throw std::runtime_error("test handler failure");
            }
        };
        s.post(handler);
    }

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
    SPANIO_CHECK(order.size() == static_cast<std::size_t>(kThrowingHandler));
    SPANIO_CHECK(!s.running_in_this_thread());

    ctx.run();
    const std::vector<int> expected{1, 2, 3, 4, 5};
    SPANIO_CHECK(order == expected);

    // The strand is not stuck in its "running" state
    bool later_ran = false;
    auto later = [&later_ran]()
    {
        later_ran = true;
    };
    ctx.restart();
    s.post(later);
    ctx.run();
    SPANIO_CHECK(later_ran);
}

// Case 13: destroying the context destroys queued strand handlers without
// calling them; a strand copy that outlives the context can be destroyed
SPANIO_TEST(destroy_with_pending)
{
    std::shared_ptr<int> token = std::make_shared<int>(0);
    int calls = 0;
    std::unique_ptr<strand_type> survivor;
    {
        std::unique_ptr<io_context> ctx(new io_context());
        strand_type s = spanio::make_strand(*ctx);
        survivor.reset(new strand_type(s));
        for (int i = 0; i < kPendingHandlers; ++i)
        {
            auto handler = [&calls, held = token]()
            {
                ++calls;
                ++*held;
            };
            s.post(std::move(handler));
        }
        SPANIO_CHECK(token.use_count() == 1 + kPendingHandlers);
        ctx.reset();
        SPANIO_CHECK(token.use_count() == 1);
        SPANIO_CHECK(calls == 0);
    }
    survivor.reset();
    SPANIO_CHECK(token.use_count() == 1);
    SPANIO_CHECK(*token == 0);
}

// Case 14: timer handlers bound to one strand under several run() threads
// never overlap; a cancelled wait reaches its bound handler once
SPANIO_TEST(bind_timer)
{
    overlap_detector detector;
    int fired = 0;
    std::atomic<int> outside{0};
    int cancel_calls = 0;
    std::error_code cancel_ec;
    io_context ctx;
    strand_type s = spanio::make_strand(ctx);
    std::vector<std::unique_ptr<steady_timer>> timers;
    steady_timer long_timer(ctx, kLongDelay);

    auto on_timer = [&](std::error_code ec)
    {
        overlap_scope scope(detector);
        if (!s.running_in_this_thread())
        {
            outside.fetch_add(1);
        }
        if (!ec)
        {
            ++fired;
        }
    };
    for (int i = 0; i < kTimers; ++i)
    {
        timers.emplace_back(new steady_timer(ctx, std::chrono::milliseconds(i % kTimerSpreadMs)));
        timers.back()->async_wait(spanio::bind_executor(s, on_timer));
    }

    auto on_cancel = [&](std::error_code ec)
    {
        overlap_scope scope(detector);
        if (!s.running_in_this_thread())
        {
            outside.fetch_add(1);
        }
        ++cancel_calls;
        cancel_ec = ec;
    };
    long_timer.async_wait(spanio::bind_executor(s, on_cancel));
    SPANIO_CHECK(long_timer.cancel() == 1);

    run_in_threads(ctx, kRunThreads);

    SPANIO_CHECK(detector.violations() == 0);
    SPANIO_CHECK(outside.load() == 0);
    SPANIO_CHECK(fired == kTimers);
    SPANIO_CHECK(cancel_calls == 1);
    SPANIO_CHECK(cancel_ec == std::errc::operation_canceled);
}

// Case 15: TCP echo whose read, write and extra posted handlers are all
// bound to one strand, under several run() threads, on every backend
SPANIO_TEST(bind_tcp_io)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        strand_type s = spanio::make_strand(*ctx);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        echo_session session(s, client, server);
        session.start();
        run_in_threads(*ctx, kRunThreads);

        SPANIO_CHECK(!session.m_error);
        SPANIO_CHECK(session.m_client_eof);
        SPANIO_CHECK(session.m_received.size() == kEchoBytes);
        bool same = session.m_received.size() == kEchoBytes;
        for (std::size_t i = 0; same && i < kEchoBytes; ++i)
        {
            same = session.m_received[i] == static_cast<char>(i % kPatternPeriod);
        }
        SPANIO_CHECK(same);
        SPANIO_CHECK(session.m_detector.violations() == 0);
        SPANIO_CHECK(session.m_outside.load() == 0);
        SPANIO_CHECK(session.m_plain_posted > 0);
        SPANIO_CHECK(session.m_plain_run == session.m_plain_posted);
    }
}

// Case 16: a bound accept handler receives the move-only socket, which then
// works; a bound read on a closed socket reports operation_canceled once
SPANIO_TEST(bind_move_only_and_cancel)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        int accept_calls = 0;
        std::error_code accept_ec;
        bool accept_in_strand = false;
        std::error_code connect_ec;
        std::error_code write_ec;
        int read_calls = 0;
        std::error_code read_ec;
        std::size_t read_bytes = 0;
        bool read_in_strand = false;
        int cancel_calls = 0;
        std::error_code cancel_ec;
        bool cancel_in_strand = false;
        char receive[kSmallBuffer] = {};
        const std::string ping("ping");

        std::unique_ptr<io_context> ctx = make_context(backend);
        strand_type s = spanio::make_strand(*ctx);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);

        auto on_accept = [&](std::error_code ec, tcp::socket peer)
        {
            ++accept_calls;
            accept_ec = ec;
            accept_in_strand = s.running_in_this_thread();
            server = std::move(peer);
        };
        auto on_connect = [&](std::error_code ec)
        {
            connect_ec = ec;
        };
        acc.async_accept(spanio::bind_executor(s, on_accept));
        client.async_connect(listening_endpoint(acc), on_connect);
        ctx->run();
        ctx->restart();
        SPANIO_REQUIRE(accept_calls == 1);
        SPANIO_REQUIRE(!accept_ec);
        SPANIO_REQUIRE(!connect_ec);
        SPANIO_CHECK(accept_in_strand);
        SPANIO_REQUIRE(server.is_open());

        // The moved-in socket carries data
        auto on_write = [&](std::error_code ec, std::size_t /*bytes*/)
        {
            write_ec = ec;
        };
        auto on_read = [&](std::error_code ec, std::size_t bytes)
        {
            ++read_calls;
            read_ec = ec;
            read_bytes = bytes;
            read_in_strand = s.running_in_this_thread();
        };
        client.async_write(spanio::buffer(ping), on_write);
        server.async_read_some(spanio::buffer(receive, sizeof(receive)), spanio::bind_executor(s, on_read));
        ctx->run();
        ctx->restart();
        SPANIO_CHECK(!write_ec);
        SPANIO_CHECK(read_calls == 1);
        SPANIO_CHECK(!read_ec);
        SPANIO_CHECK(read_in_strand);
        SPANIO_CHECK(std::string(receive, read_bytes) == ping);

        // Closing cancels the pending bound read
        auto on_cancel = [&](std::error_code ec, std::size_t /*bytes*/)
        {
            ++cancel_calls;
            cancel_ec = ec;
            cancel_in_strand = s.running_in_this_thread();
        };
        server.async_read_some(spanio::buffer(receive, sizeof(receive)), spanio::bind_executor(s, on_cancel));
        server.close();
        ctx->run();
        SPANIO_CHECK(cancel_calls == 1);
        SPANIO_CHECK(cancel_ec == std::errc::operation_canceled);
        SPANIO_CHECK(cancel_in_strand);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
