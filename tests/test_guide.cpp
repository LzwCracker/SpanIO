// ============================================================================
// File purpose: Every code snippet of the user guide docs/guide.md, compiled
//               and run. Each snippet sits between the markers
//                 // [snippet <name>]
//                 // [/snippet]
//               and docs/guide.md shows the same text after a
//               <!-- snippet: <name> --> comment. The CTest test
//               guide_snippets_match (tests/check_guide_snippets.cmake)
//               fails when the two differ, so the guide cannot drift from
//               code that builds and works.
// Usage:        Built as the test_guide executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_guide strand`.
//
//               The code around a snippet sets up what the guide's text
//               assumes (a context, a connected socket, a port) and checks
//               what the snippet did. Snippets use fully qualified names, as
//               a reader's program would.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

#include "spanio/spanio.h"

#include "test_framework.h"
#include "test_net_helpers.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

using spanio::test::connect_client;
using spanio::test::recv_exact;
using spanio::test::send_all;
using spanio::test::socket_guard;

namespace {

// Port 0 asks the system for a free port; the cases read the real one back
constexpr std::uint16_t kAnyPort = 0;

// Longest a case waits for a server to reach a state
constexpr std::chrono::seconds kWaitLimit(5);

// Pause between checks while waiting
constexpr std::chrono::milliseconds kPollInterval(10);

// Threads the quick-start server runs on in its test
constexpr unsigned kEchoThreads = 2;

// Line every echo test sends
const std::string kHelloLine = "hello\n";

// Greeting the tcp_server snippet sends to a new client
const std::string kWelcome = "welcome\n";

// Datagram the udp_server test sends
const std::string kPing = "ping";

// Room for a reply datagram in the udp_server test
constexpr std::size_t kReplyBufferSize = 16;

// Lines the tcp_server snippet counts, the client's quit included
constexpr int kLinesWithQuit = 2;

// Ticks the periodic timer snippet waits for
constexpr int kTicks = 3;

// Port the address and resolver snippets use
constexpr std::uint16_t kSnippetPort = 8080;

// Bytes of "abc\r\n", consumed by the line framer in its snippet
constexpr std::size_t kFirstLineBytes = 5;

// Spin until a condition holds or kWaitLimit passes
// Parameter condition: function returning true when done
// Return value: true if the condition held in time
bool wait_until(const std::function<bool()>& condition)
{
    const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
    while (!condition())
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            return false;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
    return true;
}

// Receive a fixed number of bytes as text on a blocking test client
// Parameter client: connected socket
// Parameter size:   bytes to receive
// Return value: the text, or an empty string on failure
std::string receive_text(const socket_guard& client, std::size_t size)
{
    std::string text(size, '\0');
    if (!recv_exact(client.get(), &text[0], size))
    {
        return std::string();
    }
    return text;
}

// Send text on a blocking test client
// Parameter client: connected socket
// Parameter text:   bytes to send
// Return value: true if everything was sent
bool send_text(const socket_guard& client, const std::string& text)
{
    return send_all(client.get(), text.data(), text.size());
}

// Connect a pair of tcp::sockets through the async API; restarts the context
// Parameter context: context of both sockets and the acceptor
// Parameter client:  socket that is not open yet
// Parameter server:  receives the accepted socket
void connect_pair(spanio::io_context& context, spanio::tcp::socket& client, spanio::tcp::socket& server)
{
    spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    std::error_code ec;
    const spanio::tcp::endpoint target = acceptor.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    auto on_accept = [&server](std::error_code accept_ec, spanio::tcp::socket peer)
    {
        if (!accept_ec)
        {
            server = std::move(peer);
        }
    };
    auto on_connect = [](std::error_code)
    {
    };
    acceptor.async_accept(on_accept);
    client.async_connect(target, on_connect);
    context.run();
    context.restart();
    SPANIO_REQUIRE(client.is_open());
    SPANIO_REQUIRE(server.is_open());
}

}

// ---------------------------------------------------------------------------
// Guide 1.2: the first program
// ---------------------------------------------------------------------------

// [snippet quick_start]
// Echo every byte back to the client that sent it
void serve_echo(spanio::tcp_server& server, unsigned threads)
{
    auto on_message = [](const spanio::tcp_connection_ptr& connection, std::string_view data)
    {
        connection->send(data);
    };
    server.on_message(on_message);
    server.run(threads);    // blocks until server.stop()
}
// [/snippet]

// Case 1: the quick-start server echoes a line
SPANIO_TEST(quick_start_echo)
{
    spanio::tcp_server server(kAnyPort);
    std::error_code ec;
    const std::uint16_t port = server.local_endpoint(ec).port();
    SPANIO_REQUIRE(!ec);
    auto serve = [&server]()
    {
        serve_echo(server, kEchoThreads);
    };
    std::thread runner(serve);

    socket_guard client;
    const bool connected = connect_client(port, client);
    const bool sent = connected && send_text(client, kHelloLine);
    const std::string echoed = sent ? receive_text(client, kHelloLine.size()) : std::string();
    client.reset();
    server.stop();
    runner.join();
    SPANIO_CHECK(connected);
    SPANIO_CHECK(echoed == kHelloLine);
}

// ---------------------------------------------------------------------------
// Guide 2: io_context, handlers, executors
// ---------------------------------------------------------------------------

// Case 2: run() returns without work, a work guard keeps it running, and a
// stopped context needs restart()
SPANIO_TEST(run_and_work)
{
    spanio::io_context idle;
    SPANIO_CHECK(idle.run() == 0);
    SPANIO_CHECK(idle.stopped());

    std::atomic<bool> handled{false};
    auto handler = [&handled]()
    {
        handled = true;
    };

    // [snippet work_guard]
    spanio::io_context context;
    spanio::executor_work_guard work = spanio::make_work_guard(context);
    auto run_context = [&context]()
    {
        context.run();      // keeps waiting for handlers while the guard lives
    };
    std::thread worker(run_context);

    context.post(handler);  // any thread may post; the worker runs it

    work.reset();           // no more work coming: run() returns when idle
    worker.join();
    // [/snippet]

    SPANIO_CHECK(handled.load());
    SPANIO_CHECK(context.stopped());

    // [snippet restart]
    context.restart();      // a context that ran out of work, or was stopped,
    context.post(handler);  // needs restart() before run() executes handlers again
    context.run();
    // [/snippet]
}

// Case 3: a posted handler runs inside run(), never inside post()
SPANIO_TEST(handlers_never_inline)
{
    spanio::io_context context;

    // [snippet never_inline]
    bool called = false;
    auto handler = [&called]()
    {
        called = true;
    };
    context.post(handler);
    // called is still false here: the handler runs inside run()
    // [/snippet]

    SPANIO_CHECK(!called);
    context.run();
    SPANIO_CHECK(called);
}

// Case 4: dispatch() inside a handler runs at once, post() queues
SPANIO_TEST(post_vs_dispatch)
{
    spanio::io_context context;

    // [snippet post_dispatch]
    std::vector<std::string> order;
    auto inner = [&order]()
    {
        order.push_back("inner");
    };
    auto outer = [&]()
    {
        context.dispatch(inner);    // inside run(): runs here, at once
        context.post(inner);        // queued: runs after outer returns
        order.push_back("outer");
    };
    context.post(outer);
    context.run();
    // order is: inner, outer, inner
    // [/snippet]

    const std::vector<std::string> expected{"inner", "outer", "inner"};
    SPANIO_CHECK(order == expected);
}

// Case 5: checking errors of asynchronous and synchronous calls
SPANIO_TEST(error_handling)
{
    spanio::io_context context;
    spanio::tcp::socket socket(context);
    spanio::tcp::socket peer(context);
    connect_pair(context, socket, peer);
    char data[64] = {};
    bool peer_closed = false;
    std::size_t received = 0;
    std::error_code failure;
    peer.close();

    // [snippet error_async]
    auto on_read = [&](std::error_code ec, std::size_t bytes)
    {
        if (ec == spanio::error::misc_errors::eof)
        {
            peer_closed = true;     // the peer closed the connection normally
        }
        else if (ec == std::errc::operation_canceled)
        {
            // we closed or cancelled the socket ourselves
        }
        else if (ec)
        {
            failure = ec;           // a real error, e.g. std::errc::connection_reset
        }
        else
        {
            received += bytes;
        }
    };
    socket.async_read_some(spanio::buffer(data), on_read);
    // [/snippet]

    context.run();
    SPANIO_CHECK(peer_closed);
    SPANIO_CHECK(received == 0);
    SPANIO_CHECK(!failure);

    bool thrown = false;

    // [snippet error_sync]
    std::error_code ec;
    spanio::ip::address bad = spanio::ip::make_address("not an address", ec);
    // ec == std::errc::invalid_argument; nothing was thrown

    try
    {
        spanio::ip::make_address(std::string("300.1.1.1"));
    }
    catch (const std::system_error& e)
    {
        // the overload without ec throws; e.code() == std::errc::invalid_argument
        thrown = e.code() == std::errc::invalid_argument;
    }
    // [/snippet]

    SPANIO_CHECK(ec == std::errc::invalid_argument);
    SPANIO_CHECK(bad == spanio::ip::address());
    SPANIO_CHECK(thrown);
}

// ---------------------------------------------------------------------------
// Guide 3: TCP
// ---------------------------------------------------------------------------

// Case 6: addresses and endpoints
SPANIO_TEST(addresses)
{
    // [snippet addresses]
    spanio::ip::address v4 = spanio::ip::make_address("192.168.1.10");
    spanio::ip::address v6 = spanio::ip::make_address("fe80::1");
    spanio::tcp::endpoint local(spanio::ipv4::loopback(), 8080);    // 127.0.0.1:8080
    spanio::tcp::endpoint all_v6(spanio::ipv6::any(), 8080);        // [::]:8080
    std::string text = local.address().to_string();                 // "127.0.0.1"
    std::uint16_t port = local.port();                               // 8080
    // [/snippet]

    SPANIO_CHECK(v4.is_v4());
    SPANIO_CHECK(v4.to_string() == "192.168.1.10");
    SPANIO_CHECK(v6.is_v6());
    SPANIO_CHECK(v6.to_string() == "fe80::1");
    SPANIO_CHECK(all_v6.family() == AF_INET6);
    SPANIO_CHECK(text == "127.0.0.1");
    SPANIO_CHECK(port == kSnippetPort);
}

// [snippet accept_loop]
// One client: echoes what it reads until the client closes
class session : public std::enable_shared_from_this<session>
{
public:
    explicit session(spanio::tcp::socket socket)
        : m_socket(std::move(socket))
    {
    }

    void start()
    {
        read();
    }

private:
    void read()
    {
        auto self = shared_from_this();     // keeps the session alive while waiting
        auto on_read = [self](std::error_code ec, std::size_t bytes)
        {
            if (!ec)
            {
                self->write(bytes);
            }
        };
        m_socket.async_read_some(spanio::buffer(m_data), on_read);
    }

    void write(std::size_t bytes)
    {
        auto self = shared_from_this();
        auto on_written = [self](std::error_code ec, std::size_t)
        {
            if (!ec)
            {
                self->read();
            }
        };
        m_socket.async_write(spanio::buffer(m_data, bytes), on_written);
    }

    // The connection
    spanio::tcp::socket m_socket;
    // Bytes read, then written back
    char m_data[4096];
};

// Accept clients one after another, each into its own session
void accept_next(spanio::tcp::acceptor& acceptor)
{
    auto on_accept = [&acceptor](std::error_code ec, spanio::tcp::socket peer)
    {
        if (ec)
        {
            return;     // e.g. operation_canceled after acceptor.close()
        }
        std::make_shared<session>(std::move(peer))->start();
        accept_next(acceptor);
    };
    acceptor.async_accept(on_accept);
}
// [/snippet]

// Case 7: the accept loop serves two clients at once
SPANIO_TEST(accept_loop)
{
    spanio::io_context context;
    spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    std::error_code ec;
    const std::uint16_t port = acceptor.local_endpoint(ec).port();
    SPANIO_REQUIRE(!ec);
    accept_next(acceptor);
    auto run_context = [&context]()
    {
        context.run();
    };
    std::thread runner(run_context);

    socket_guard first;
    socket_guard second;
    const bool connected = connect_client(port, first) && connect_client(port, second);
    const bool sent = connected && send_text(first, "one") && send_text(second, "two");
    const std::string first_echo = sent ? receive_text(first, 3) : std::string();
    const std::string second_echo = sent ? receive_text(second, 3) : std::string();
    // Closing the clients ends their sessions; closing the acceptor (on the
    // context's thread) ends the loop, so run() returns
    first.reset();
    second.reset();
    auto close_acceptor = [&acceptor]()
    {
        acceptor.close();
    };
    context.post(close_acceptor);
    runner.join();
    SPANIO_CHECK(connected);
    SPANIO_CHECK(first_echo == "one");
    SPANIO_CHECK(second_echo == "two");
}

// ---------------------------------------------------------------------------
// Guide 5: high-level servers
// ---------------------------------------------------------------------------

// Case 8: tcp_server callbacks, user data, options and a line framer
SPANIO_TEST(tcp_server_callbacks)
{
    spanio::io_context context;
    const std::uint16_t port = kAnyPort;
    int messages = 0;
    std::error_code close_reason = std::make_error_code(std::errc::io_error);

    // [snippet tcp_server]
    spanio::tcp_server_options options;
    options.m_max_connections = 1000;
    options.m_idle_timeout = std::chrono::seconds(60);    // close silent clients
    spanio::tcp_server server(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), port), options);

    auto on_connect = [](const spanio::tcp_connection_ptr& connection)
    {
        connection->user_data() = 0;    // per-connection state: lines seen
        connection->send("welcome\n");
    };
    auto on_message = [](const spanio::tcp_connection_ptr& connection, std::string_view line)
    {
        ++std::any_cast<int&>(connection->user_data());
        if (line == "quit")
        {
            connection->close();        // sends what is queued, then closes
            return;
        }
        connection->send(std::string(line) + "\n");
    };
    auto on_close = [&](const spanio::tcp_connection_ptr& connection, std::error_code reason)
    {
        messages = std::any_cast<int>(connection->user_data());
        close_reason = reason;          // empty after close(); eof if the client left
    };
    server.on_connect(on_connect);
    server.on_message(on_message);
    server.on_close(on_close);
    server.set_framer(spanio::line_framer());   // one on_message per line
    server.start();     // the caller runs the context; server.run(n) runs its own
    // [/snippet]

    std::error_code ec;
    const std::uint16_t real_port = server.local_endpoint(ec).port();
    SPANIO_REQUIRE(!ec);
    auto run_context = [&context]()
    {
        context.run();
    };
    std::thread runner(run_context);

    socket_guard client;
    const bool connected = connect_client(real_port, client);
    const std::string welcome = connected ? receive_text(client, kWelcome.size()) : std::string();
    const bool sent = connected && send_text(client, "hello\nquit\n");
    const std::string reply = sent ? receive_text(client, kHelloLine.size()) : std::string();
    // The server closes after "quit": the next receive sees the end
    char extra = 0;
    const bool ended = sent && !recv_exact(client.get(), &extra, 1);
    client.reset();
    auto no_connections = [&server]()
    {
        return server.connection_count() == 0;
    };
    const bool closed = wait_until(no_connections);
    server.stop();
    runner.join();

    SPANIO_CHECK(welcome == kWelcome);
    SPANIO_CHECK(reply == kHelloLine);
    SPANIO_CHECK(ended);
    SPANIO_CHECK(closed);
    SPANIO_CHECK(messages == kLinesWithQuit);
    SPANIO_CHECK(!close_reason);
}

// Case 9: a custom framer and the line framer
SPANIO_TEST(custom_framer)
{
    // [snippet custom_framer]
    // Every message is a record of exactly 8 bytes
    constexpr std::size_t kRecordSize = 8;
    auto fixed_records = [](std::string_view input)
    {
        spanio::frame_result result;            // m_consumed == 0: need more data
        if (input.size() >= kRecordSize)
        {
            result.m_payload = input.substr(0, kRecordSize);
            result.m_consumed = kRecordSize;
        }
        return result;
    };
    // server.set_framer(fixed_records);
    // [/snippet]

    const spanio::frame_result whole = fixed_records("12345678abc");
    SPANIO_CHECK(whole.m_payload == "12345678");
    SPANIO_CHECK(whole.m_consumed == kRecordSize);
    SPANIO_CHECK(!whole.m_error);
    const spanio::frame_result partial = fixed_records("123");
    SPANIO_CHECK(partial.m_consumed == 0);

    // [snippet line_framer]
    spanio::framer lines = spanio::line_framer();
    spanio::frame_result first = lines("abc\r\ndef");
    // first.m_payload == "abc" (the line ending is removed), first.m_consumed == 5
    // [/snippet]

    SPANIO_CHECK(first.m_payload == "abc");
    SPANIO_CHECK(first.m_consumed == kFirstLineBytes);
}

// Case 10: udp_server replies to the sender and counts what it did
SPANIO_TEST(udp_server_stats)
{
    spanio::io_context context;
    const std::uint16_t port = kAnyPort;

    // [snippet udp_server]
    spanio::udp_server server(context, spanio::udp::endpoint(spanio::ipv4::loopback(), port));
    spanio::udp_server* self = &server;
    auto on_datagram = [self](const spanio::udp::endpoint& from, std::string_view data)
    {
        self->send_to(from, data);      // queue a reply to the sender
    };
    server.on_datagram(on_datagram);
    server.start();
    // [/snippet]

    std::error_code ec;
    const std::uint16_t real_port = server.local_endpoint(ec).port();
    SPANIO_REQUIRE(!ec);
    auto run_context = [&context]()
    {
        context.run();
    };
    std::thread runner(run_context);

    socket_guard client(spanio::detail::create_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, ec));
    const bool created = !ec && spanio::test::set_receive_timeout(client.get());
    const sockaddr_in target = spanio::test::loopback_address(real_port);
    const int ping_size = static_cast<int>(kPing.size());
    const bool sent = created
        && ::sendto(client.get(), kPing.data(), ping_size, 0, reinterpret_cast<const sockaddr*>(&target),
                    static_cast<spanio::detail::socklen_type>(sizeof(target)))
               == ping_size;
    char reply[kReplyBufferSize] = {};
    const int got = sent ? ::recv(client.get(), reply, static_cast<int>(sizeof(reply)), 0) : -1;
    auto replied = [&server]()
    {
        return server.stats().m_sent == 1;
    };
    const bool counted = wait_until(replied);

    // [snippet udp_stats]
    const spanio::udp_server_stats stats = server.stats();
    // stats.m_received, m_sent, m_queue_dropped, m_send_failed, m_truncated
    // [/snippet]

    server.stop();
    runner.join();
    SPANIO_CHECK(sent);
    SPANIO_CHECK(got == ping_size);
    SPANIO_CHECK(std::string(reply, kPing.size()) == kPing);
    SPANIO_CHECK(counted);
    SPANIO_CHECK(stats.m_received == 1);
    SPANIO_CHECK(stats.m_sent == 1);
    SPANIO_CHECK(stats.m_queue_dropped == 0);
}

// ---------------------------------------------------------------------------
// Guide 6: timers
// ---------------------------------------------------------------------------

// Case 11: a periodic timer, and a timeout for a read
SPANIO_TEST(timers)
{
    spanio::io_context context;
    const auto started = spanio::steady_timer::clock::now();

    // [snippet periodic_timer]
    const std::chrono::milliseconds kPeriod(20);
    spanio::steady_timer timer(context);
    int ticks = 0;
    std::function<void(std::error_code)> on_tick;
    on_tick = [&](std::error_code ec)
    {
        if (ec || ++ticks == 3)
        {
            return;             // cancelled, or done: no new wait, run() can return
        }
        timer.expires_at(timer.expiry() + kPeriod);     // from the last expiry: no drift
        timer.async_wait(on_tick);
    };
    timer.expires_after(kPeriod);
    timer.async_wait(on_tick);
    context.run();
    // [/snippet]

    SPANIO_CHECK(ticks == kTicks);
    SPANIO_CHECK(spanio::steady_timer::clock::now() - started >= kTicks * kPeriod);

    context.restart();
    spanio::tcp::socket socket(context);
    spanio::tcp::socket silent_peer(context);
    connect_pair(context, socket, silent_peer);
    char data[64] = {};
    std::error_code read_result;

    // [snippet read_timeout]
    spanio::steady_timer deadline(context, std::chrono::milliseconds(100));
    auto on_deadline = [&socket](std::error_code ec)
    {
        if (!ec)
        {
            socket.cancel();    // too late: the read ends with operation_canceled
        }
    };
    auto on_read = [&](std::error_code ec, std::size_t)
    {
        deadline.cancel();      // the read ended first: stop the timer
        read_result = ec;
    };
    deadline.async_wait(on_deadline);
    socket.async_read_some(spanio::buffer(data), on_read);
    context.run();
    // [/snippet]

    SPANIO_CHECK(read_result == std::errc::operation_canceled);
}

// ---------------------------------------------------------------------------
// Guide 7: resolving names
// ---------------------------------------------------------------------------

// Case 12: synchronous and asynchronous lookups
SPANIO_TEST(resolving)
{
    spanio::io_context context;

    // [snippet resolve_sync]
    spanio::tcp::resolver resolver(context);
    std::error_code ec;
    std::vector<spanio::tcp::endpoint> endpoints = resolver.resolve("localhost", "8080", ec);
    // on success: e.g. [::1]:8080 and 127.0.0.1:8080, in the order to try them
    // [/snippet]

    SPANIO_CHECK(!ec);
    SPANIO_CHECK(!endpoints.empty());
    for (const spanio::tcp::endpoint& endpoint : endpoints)
    {
        const std::string text = endpoint.address().to_string();
        SPANIO_CHECK(text == "127.0.0.1" || text == "::1");
        SPANIO_CHECK(endpoint.port() == kSnippetPort);
    }

    std::size_t found = 0;

    // [snippet resolve_async]
    auto on_resolved = [&found](std::error_code ec, std::vector<spanio::tcp::endpoint> endpoints)
    {
        if (!ec)
        {
            found = endpoints.size();
        }
    };
    spanio::resolve_options options;
    options.m_family = spanio::resolve_family::v4;  // IPv4 addresses only
    options.m_numeric_host = true;                  // no DNS: the host is an address
    resolver.async_resolve("127.0.0.1", "80", on_resolved, options);
    context.run();
    // [/snippet]

    SPANIO_CHECK(found == 1);
}

// ---------------------------------------------------------------------------
// Guide 8: threads and strands
// ---------------------------------------------------------------------------

// Case 13: four threads, a counter without a lock
SPANIO_TEST(strand_counter)
{
    // [snippet strand]
    constexpr int kIncrements = 10000;
    constexpr int kThreads = 4;
    spanio::io_context context;
    auto counter_strand = spanio::make_strand(context);
    long counter = 0;               // shared, but only touched on the strand
    auto increment = [&counter]()
    {
        ++counter;
    };
    for (int i = 0; i < kIncrements; ++i)
    {
        counter_strand.post(increment);
    }

    // Completion handlers join the strand through bind_executor
    spanio::steady_timer timer(context, std::chrono::milliseconds(10));
    auto on_timer = [&counter](std::error_code)
    {
        ++counter;
    };
    timer.async_wait(spanio::bind_executor(counter_strand, on_timer));

    auto run_context = [&context]()
    {
        context.run();
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i)
    {
        threads.emplace_back(run_context);
    }
    for (std::thread& thread : threads)
    {
        thread.join();
    }
    // counter == kIncrements + 1, with no lock
    // [/snippet]

    SPANIO_CHECK(counter == kIncrements + 1);
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
