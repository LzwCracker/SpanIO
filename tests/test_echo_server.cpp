// ============================================================================
// File purpose: Tests of the echo_server example (examples/echo_server.cpp).
//               The example is included with SPANIO_ECHO_SERVER_NO_MAIN
//               defined, so the tests run exactly the shipped code. The
//               server runs on its own run() threads; clients are plain
//               blocking sockets, as an external program would use. Cases
//               that do I/O run once per proactor backend of the platform.
//               All traffic stays on 127.0.0.1.
// Usage:        Built as the test_echo_server executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_echo_server concurrent`.
//
//               Declaration order in each case: context, server, runner,
//               clients. Clients close first, which ends their sessions;
//               then the runner stops the server and joins its threads.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05
// ============================================================================

#define SPANIO_ECHO_SERVER_NO_MAIN
#include "../examples/echo_server.cpp"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using spanio::io_context;
using spanio::detail::native_handle_t;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::connect_client;
using spanio::test::connect_to;
using spanio::test::loopback_address;
using spanio::test::make_context;
using spanio::test::recv_exact;
using spanio::test::send_all;
using spanio::test::socket_guard;

namespace {

// Port 0: let the server's bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Threads running the server in the concurrency case
constexpr int kServerThreads = 4;

// Threads running the server in the other cases
constexpr int kSingleThread = 1;

// Clients connecting at the same time in the concurrency case
constexpr int kConcurrentClients = 8;

// Messages each client sends in the concurrency case
constexpr int kMessagesPerClient = 5;

// Size of each message in the concurrency case
constexpr std::size_t kMessageSize = 100;

// Size of the large payload
constexpr std::size_t kLargePayloadSize = 1024 * 1024;

// Payload bytes repeat with this prime period, so misplaced data shows up
constexpr std::size_t kPatternModulo = 251;

// Multiplier of the client id in the message pattern
constexpr std::size_t kClientStride = 31;

// Multiplier of the message number in the message pattern
constexpr std::size_t kMessageStride = 7;

// Short message of the single-client case
constexpr char kHello[] = "hello, echo";

// Length of kHello without the terminating zero
constexpr std::size_t kHelloLength = sizeof(kHello) - 1;

// Longest wait for the server's session count to change
constexpr int kSessionWaitMs = 5000;

// Pause between two checks of a condition being waited for
constexpr int kPollIntervalMs = 5;

// Upper bound for run() to return after stop(); covers Windows' ~2 s
// refused-connect retries
constexpr long long kStopLimitMs = 5000;

// Port given on the command line in the argument case ("9000")
constexpr std::uint16_t kExplicitPort = 9000;

// Thread count given on the command line in the argument case ("3")
constexpr unsigned kExplicitThreads = 3;

// Message of the log cases
constexpr char kLogMessage[] = "hello";

// Its length without the terminating zero
constexpr std::size_t kLogMessageLength = sizeof(kLogMessage) - 1;

// Message with control characters: "a\r\nb" and the byte 0x01
constexpr char kControlMessage[] = "a\r\nb\x01";

// Its length without the terminating zero
constexpr std::size_t kControlMessageLength = sizeof(kControlMessage) - 1;

// How the log shows kControlMessage
constexpr char kControlEscaped[] = "a\\r\\nb\\x01";

// Size of the message that the log truncates
constexpr std::size_t kLongMessageSize = 200;

// Messages each client sends in the interleaving case
constexpr int kLogMessagesPerClient = 50;

// Port used by the address formatting case
constexpr std::uint16_t kFormatPort = 8080;

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Number of entries of an argv-style array, as main() would get in argc
// Parameter args: the array
// Return value: N
template <std::size_t N>
int argc_of(const char* const (&args)[N])
{
    static_cast<void>(args);
    return static_cast<int>(N);
}

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Byte of a client message
// Parameter client:  client id
// Parameter message: message number of that client
// Parameter index:   position in the message
// Return value: the expected byte
char message_byte(int client, int message, std::size_t index)
{
    const std::size_t value = static_cast<std::size_t>(client) * kClientStride
                            + static_cast<std::size_t>(message) * kMessageStride + index;
    return static_cast<char>(value % kPatternModulo);
}

// Wait until the server serves a given number of sessions
// Parameter server:   server to watch
// Parameter expected: session count to wait for
// Return value: true if the count was reached within kSessionWaitMs
bool wait_for_sessions(const echo::echo_server& server, std::size_t expected)
{
    const test_clock::time_point start = test_clock::now();
    while (server.active_sessions() != expected)
    {
        if (elapsed_ms(start) > kSessionWaitMs)
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
    }
    return true;
}

// Port the server listens on, or fail the case
// Parameter server: started server
// Return value: the port
std::uint16_t server_port(const echo::echo_server& server)
{
    std::error_code ec;
    const spanio::tcp::endpoint local = server.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return local.port();
}

// Runs an io_context on several threads for the lifetime of a case.
// stop_and_join() (or the destructor) stops the server and waits until
// every run() has returned.
class server_runner
{
public:
    // Start the server and the run() threads
    // Parameter context: context of the server
    // Parameter server:  server to start; must outlive the runner
    // Parameter threads: number of run() threads
    server_runner(io_context& context, echo::echo_server& server, int threads)
        : m_server(&server)
    {
        server.start();
        auto run_context = [&context]()
        {
            context.run();
        };
        for (int i = 0; i < threads; ++i)
        {
            m_threads.emplace_back(run_context);
        }
    }

    // Stop the server and join the threads
    ~server_runner()
    {
        stop_and_join();
    }

    server_runner(const server_runner&) = delete;
    server_runner& operator=(const server_runner&) = delete;

    // Stop the server, which disconnects any clients still connected, and
    // wait for every run() to return
    void stop_and_join()
    {
        m_server->stop();
        for (std::thread& thread : m_threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }

private:
    // Server being run
    echo::echo_server* m_server;
    // Threads calling run()
    std::vector<std::thread> m_threads;
};

// Create a server on 127.0.0.1 with a system-picked port, or fail the case
// Parameter context: context for the server
// Parameter sink:    where its log lines go; empty (the default) for none
// Return value: the server, not started yet
std::unique_ptr<echo::echo_server> make_server(io_context& context, echo::log_sink sink = echo::log_sink())
{
    return std::unique_ptr<echo::echo_server>(new echo::echo_server(
        context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort), std::move(sink)));
}

// Collects the server's log lines; safe to read while the server writes
class log_capture
{
public:
    // A sink that appends to this capture; the capture must outlive it. It
    // also counts calls that overlap: the server must hand lines to its sink
    // one at a time (stdout has no lock of its own), so a capture that only
    // relied on its own mutex would hide a missing lock in the server.
    // Return value: the sink
    echo::log_sink sink()
    {
        auto add = [this](const std::string& line)
        {
            if (m_inside.fetch_add(1) != 0)
            {
                m_overlaps.fetch_add(1);
            }
            // Widen the window in which an unserialized second call would
            // overlap this one
            std::this_thread::yield();
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_lines.push_back(line);
            }
            m_inside.fetch_sub(1);
        };
        return echo::log_sink(add);
    }

    // Number of sink calls that started while another was still running
    // Return value: the count; 0 when the server serializes its lines
    int overlaps() const
    {
        return m_overlaps.load();
    }

    // The lines so far
    // Return value: a copy
    std::vector<std::string> lines() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_lines;
    }

    // Lines that contain a text
    // Parameter text: text to look for, e.g. an event tag
    // Return value: the matching lines, in order
    std::vector<std::string> with(const std::string& text) const
    {
        std::vector<std::string> found;
        for (const std::string& line : lines())
        {
            if (line.find(text) != std::string::npos)
            {
                found.push_back(line);
            }
        }
        return found;
    }

    // Wait until at least a number of lines contain a text
    // Parameter text:  text to look for
    // Parameter count: lines needed
    // Return value: true if they appeared within kSessionWaitMs
    bool wait_for(const std::string& text, std::size_t count) const
    {
        const test_clock::time_point start = test_clock::now();
        while (with(text).size() < count)
        {
            if (elapsed_ms(start) > kSessionWaitMs)
            {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
        }
        return true;
    }

private:
    // Guards m_lines
    mutable std::mutex m_mutex;
    // Lines in the order they were written
    std::vector<std::string> m_lines;
    // Sink calls running right now
    std::atomic<int> m_inside{0};
    // Sink calls that found another one running
    std::atomic<int> m_overlaps{0};
};

// Address text the server logs for a connected test client
// Parameter client: connected blocking client
// Return value: "127.0.0.1:<client port>"
std::string client_peer(const socket_guard& client)
{
    const sockaddr_in local = spanio::test::local_address(client.get());
    return "127.0.0.1:" + std::to_string(static_cast<unsigned>(ntohs(local.sin_port)));
}

// Whether a line has the log format: time, tag, peer, two spaces, detail
// Parameter line: the line
// Return value: true when well-formed and free of line breaks
bool well_formed(const std::string& line)
{
    static const std::regex format(R"(^\d{2}:\d{2}:\d{2}\.\d{3} \[(connect|recv|close)\] +[^ ]+  \S.*$)");
    return line.find_first_of("\r\n") == std::string::npos && std::regex_match(line, format);
}

}

// Case 1: one client sends a message and gets exactly the same bytes back
SPANIO_TEST(echo_single_client)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx);
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));

        SPANIO_CHECK(send_all(client.get(), kHello, kHelloLength));
        char reply[kHelloLength] = {};
        SPANIO_CHECK(recv_exact(client.get(), reply, kHelloLength));
        SPANIO_CHECK(std::memcmp(reply, kHello, kHelloLength) == 0);
    }
}

// Case 2: a server on four threads serves eight clients at once, five
// messages each; every reply matches its message
SPANIO_TEST(echo_concurrent_clients)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx);
        server_runner runner(*ctx, *server, kServerThreads);
        const std::uint16_t port = server_port(*server);

        // Client threads count problems here; the check macros are not
        // thread-safe
        std::atomic<int> failures(0);
        std::atomic<int> echoed(0);
        auto run_client = [&](int id)
        {
            socket_guard client;
            if (!connect_client(port, client))
            {
                ++failures;
                return;
            }
            std::vector<char> message(kMessageSize);
            std::vector<char> reply(kMessageSize);
            for (int m = 0; m < kMessagesPerClient; ++m)
            {
                for (std::size_t i = 0; i < kMessageSize; ++i)
                {
                    message[i] = message_byte(id, m, i);
                }
                if (!send_all(client.get(), message.data(), kMessageSize)
                    || !recv_exact(client.get(), reply.data(), kMessageSize) || reply != message)
                {
                    ++failures;
                    return;
                }
                ++echoed;
            }
        };
        std::vector<std::thread> clients;
        for (int id = 0; id < kConcurrentClients; ++id)
        {
            clients.emplace_back(run_client, id);
        }
        for (std::thread& client : clients)
        {
            client.join();
        }
        SPANIO_CHECK(failures.load() == 0);
        SPANIO_CHECK(echoed.load() == kConcurrentClients * kMessagesPerClient);
    }
}

// Case 3: a 1 MB payload, sent and received at the same time, comes back
// intact
SPANIO_TEST(echo_large_payload)
{
    std::vector<char> payload(kLargePayloadSize);
    for (std::size_t i = 0; i < payload.size(); ++i)
    {
        payload[i] = static_cast<char>(i % kPatternModulo);
    }
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx);
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));

        // Sending in another thread while this one receives: the echo flows
        // back while the payload is still being sent, so neither side's
        // buffers can fill up and block the transfer
        bool sent = false;
        const native_handle_t handle = client.get();
        auto send_payload = [&sent, &payload, handle]()
        {
            sent = send_all(handle, payload.data(), payload.size());
        };
        std::thread sender(send_payload);
        std::vector<char> reply(kLargePayloadSize);
        const bool received = recv_exact(handle, reply.data(), reply.size());
        sender.join();

        SPANIO_CHECK(sent);
        SPANIO_CHECK(received);
        SPANIO_CHECK(reply == payload);
    }
}

// Case 4: a connected client is counted as one live session; after it
// disconnects the count drops back to zero
SPANIO_TEST(session_released_after_disconnect)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx);
        server_runner runner(*ctx, *server, kSingleThread);
        SPANIO_CHECK(server->active_sessions() == 0);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));
        SPANIO_CHECK(wait_for_sessions(*server, 1));

        client.reset();
        SPANIO_CHECK(wait_for_sessions(*server, 0));
    }
}

// Case 5: after stop(), run() returns and the port refuses connections
SPANIO_TEST(stop_refuses_new_connections)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx);
        server_runner runner(*ctx, *server, kSingleThread);
        const std::uint16_t port = server_port(*server);

        const test_clock::time_point start = test_clock::now();
        runner.stop_and_join();
        SPANIO_CHECK(elapsed_ms(start) < kStopLimitMs);
        SPANIO_CHECK(ctx->stopped());

        std::error_code ec;
        socket_guard late(spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);
        SPANIO_CHECK(connect_to(late.get(), loopback_address(port)) != 0);
        SPANIO_CHECK(spanio::detail::last_error() == std::errc::connection_refused);
    }
}

// Case 6: the command line gives defaults, honours explicit values, and
// rejects bad ports, bad thread counts and extra arguments
SPANIO_TEST(parse_arguments)
{
    echo::options opts;
    std::string error;

    const char* const defaults[] = {"echo_server"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(defaults), defaults, opts, error));
    SPANIO_CHECK(opts.m_port == echo::kDefaultPort);
    SPANIO_CHECK(opts.m_threads >= echo::kMinThreads);

    const char* const explicit_values[] = {"echo_server", "9000", "3"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(explicit_values), explicit_values, opts, error));
    SPANIO_CHECK(opts.m_port == kExplicitPort);
    SPANIO_CHECK(opts.m_threads == kExplicitThreads);

    const char* const bad_ports[] = {"0", "70000", "abc", "8080x", "-1", "", " 80"};
    for (const char* port : bad_ports)
    {
        const char* const argv[] = {"echo_server", port};
        error.clear();
        SPANIO_CHECK(!echo::parse_arguments(argc_of(argv), argv, opts, error));
        SPANIO_CHECK(!error.empty());
    }

    const char* const bad_threads[] = {"0", "x", "257"};
    for (const char* threads : bad_threads)
    {
        const char* const argv[] = {"echo_server", "8080", threads};
        error.clear();
        SPANIO_CHECK(!echo::parse_arguments(argc_of(argv), argv, opts, error));
        SPANIO_CHECK(!error.empty());
    }

    const char* const too_many[] = {"echo_server", "8080", "2", "extra"};
    error.clear();
    SPANIO_CHECK(!echo::parse_arguments(argc_of(too_many), too_many, opts, error));
    SPANIO_CHECK(!error.empty());
}

// Case 7: a client that connects is logged with its address and the online
// count
SPANIO_TEST(log_connect)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        log_capture log;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));

        SPANIO_REQUIRE(log.wait_for("[connect]", 1));
        const std::string line = log.with("[connect]").front();
        SPANIO_CHECK(well_formed(line));
        SPANIO_CHECK(line.find(" [connect] " + client_peer(client) + "  online 1") != std::string::npos);
    }
}

// Case 8: received data is logged with its size and text, and still echoed
SPANIO_TEST(log_message)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        log_capture log;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));

        SPANIO_CHECK(send_all(client.get(), kLogMessage, kLogMessageLength));
        char reply[kLogMessageLength] = {};
        SPANIO_CHECK(recv_exact(client.get(), reply, kLogMessageLength));
        SPANIO_CHECK(std::memcmp(reply, kLogMessage, kLogMessageLength) == 0);

        SPANIO_REQUIRE(log.wait_for("[recv]", 1));
        const std::string line = log.with("[recv]").front();
        SPANIO_CHECK(well_formed(line));
        SPANIO_CHECK(line.find(" [recv]    " + client_peer(client) + "  5 bytes: hello") != std::string::npos);
    }
}

// Case 9: control characters are escaped, so one message is one line;
// other bytes, such as UTF-8 text, are kept
SPANIO_TEST(log_escapes)
{
    log_capture log;
    std::unique_ptr<io_context> ctx = make_context(backends().front());
    std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
    server_runner runner(*ctx, *server, kSingleThread);
    socket_guard client;
    SPANIO_REQUIRE(connect_client(server_port(*server), client));

    SPANIO_CHECK(send_all(client.get(), kControlMessage, kControlMessageLength));
    char reply[kControlMessageLength] = {};
    SPANIO_CHECK(recv_exact(client.get(), reply, kControlMessageLength));

    SPANIO_REQUIRE(log.wait_for("[recv]", 1));
    for (const std::string& line : log.lines())
    {
        SPANIO_CHECK(well_formed(line));
    }
    const std::string line = log.with("[recv]").front();
    SPANIO_CHECK(line.find("5 bytes: " + std::string(kControlEscaped)) != std::string::npos);

    // The escaping rules themselves
    SPANIO_CHECK(echo::escape_bytes(std::string_view("\t\\\x7f", 3)) == "\\t\\\\\\x7F");
    SPANIO_CHECK(echo::escape_bytes("\xE4\xB8\xAD\xE6\x96\x87") == "\xE4\xB8\xAD\xE6\x96\x87");
    SPANIO_CHECK(echo::escape_bytes("") == "");
}

// Case 10: a long message shows only its first 64 bytes and its size
SPANIO_TEST(log_long_message_truncated)
{
    log_capture log;
    std::unique_ptr<io_context> ctx = make_context(backends().front());
    std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
    server_runner runner(*ctx, *server, kSingleThread);
    socket_guard client;
    SPANIO_REQUIRE(connect_client(server_port(*server), client));

    const std::string message(kLongMessageSize, 'x');
    SPANIO_CHECK(send_all(client.get(), message.data(), message.size()));
    std::string reply(kLongMessageSize, '\0');
    SPANIO_CHECK(recv_exact(client.get(), &reply[0], reply.size()));

    SPANIO_REQUIRE(log.wait_for("[recv]", 1));
    const std::string line = log.with("[recv]").front();
    const std::string expected = "200 bytes: " + std::string(echo::kShownBytes, 'x') + "... (200 bytes)";
    SPANIO_CHECK(line.size() >= expected.size());
    SPANIO_CHECK(line.compare(line.size() - expected.size(), expected.size(), expected) == 0);

    // A cut never splits a UTF-8 character: 63 ASCII bytes and a 3-byte
    // character show only the 63 bytes
    const std::string utf8 = std::string(echo::kShownBytes - 1, 'a') + "\xE4\xB8\xAD";
    SPANIO_CHECK(echo::escape_bytes(utf8) == std::string(echo::kShownBytes - 1, 'a') + "... (66 bytes)");
}

// Case 11: a client that disconnects is logged as "client closed", with the
// bytes it sent and the online count back at 0
SPANIO_TEST(log_client_close)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        log_capture log;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));
        const std::string peer = client_peer(client);

        SPANIO_CHECK(send_all(client.get(), kLogMessage, kLogMessageLength));
        char reply[kLogMessageLength] = {};
        SPANIO_CHECK(recv_exact(client.get(), reply, kLogMessageLength));
        client.reset();

        SPANIO_REQUIRE(log.wait_for("[close]", 1));
        const std::string line = log.with("[close]").front();
        SPANIO_CHECK(well_formed(line));
        SPANIO_CHECK(line.find(" [close]   " + peer + "  client closed, 5 bytes received, connected ")
                     != std::string::npos);
        SPANIO_CHECK(line.find(" s, online 0") != std::string::npos);
    }
}

// Case 12: stopping the server with a client connected logs "server closed"
SPANIO_TEST(log_server_stop)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        log_capture log;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
        server_runner runner(*ctx, *server, kSingleThread);
        socket_guard client;
        SPANIO_REQUIRE(connect_client(server_port(*server), client));
        SPANIO_REQUIRE(log.wait_for("[connect]", 1));

        runner.stop_and_join();
        const std::vector<std::string> closes = log.with("[close]");
        SPANIO_REQUIRE(closes.size() == 1);
        SPANIO_CHECK(well_formed(closes.front()));
        SPANIO_CHECK(closes.front().find("server closed, 0 bytes received") != std::string::npos);
    }
}

// Case 13: four threads, eight clients, fifty messages each: every line is
// whole and well-formed, each client has one [connect] and one [close], and
// the [recv] lines add up to every byte sent
SPANIO_TEST(log_lines_not_interleaved)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        log_capture log;
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::unique_ptr<echo::echo_server> server = make_server(*ctx, log.sink());
        server_runner runner(*ctx, *server, kServerThreads);
        const std::uint16_t port = server_port(*server);

        std::atomic<int> failures(0);
        auto run_client = [&](int id)
        {
            socket_guard client;
            if (!connect_client(port, client))
            {
                ++failures;
                return;
            }
            std::vector<char> message(kMessageSize);
            std::vector<char> reply(kMessageSize);
            for (int m = 0; m < kLogMessagesPerClient; ++m)
            {
                for (std::size_t i = 0; i < kMessageSize; ++i)
                {
                    message[i] = message_byte(id, m, i);
                }
                if (!send_all(client.get(), message.data(), kMessageSize)
                    || !recv_exact(client.get(), reply.data(), kMessageSize) || reply != message)
                {
                    ++failures;
                    return;
                }
            }
        };
        std::vector<std::thread> clients;
        for (int id = 0; id < kConcurrentClients; ++id)
        {
            clients.emplace_back(run_client, id);
        }
        for (std::thread& client : clients)
        {
            client.join();
        }
        SPANIO_CHECK(failures.load() == 0);
        SPANIO_CHECK(log.wait_for("[close]", static_cast<std::size_t>(kConcurrentClients)));
        runner.stop_and_join();

        std::size_t malformed = 0;
        std::size_t received = 0;
        for (const std::string& line : log.lines())
        {
            if (!well_formed(line))
            {
                ++malformed;
            }
        }
        for (const std::string& line : log.with("[recv]"))
        {
            // "<peer>  <N> bytes: ...": N is the word just before " bytes: "
            const std::size_t bytes_word = line.find(" bytes: ");
            const std::size_t number = line.rfind(' ', bytes_word - 1) + 1;
            received += std::strtoull(line.c_str() + number, nullptr, echo::kDecimalBase);
        }
        SPANIO_CHECK(malformed == 0);
        SPANIO_CHECK(log.overlaps() == 0);
        SPANIO_CHECK(log.with("[connect]").size() == static_cast<std::size_t>(kConcurrentClients));
        SPANIO_CHECK(log.with("[close]").size() == static_cast<std::size_t>(kConcurrentClients));
        SPANIO_CHECK(received == static_cast<std::size_t>(kConcurrentClients * kLogMessagesPerClient) * kMessageSize);
    }
}

// Case 14: --quiet is accepted anywhere and turns the log off; unknown
// options and extra values are rejected; a quiet server still echoes
SPANIO_TEST(quiet_and_arguments)
{
    echo::options opts;
    std::string error;

    const char* const quiet_only[] = {"echo_server", "--quiet"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(quiet_only), quiet_only, opts, error));
    SPANIO_CHECK(opts.m_quiet);
    SPANIO_CHECK(opts.m_port == echo::kDefaultPort);

    const char* const quiet_middle[] = {"echo_server", "9000", "--quiet", "3"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(quiet_middle), quiet_middle, opts, error));
    SPANIO_CHECK(opts.m_quiet);
    SPANIO_CHECK(opts.m_port == kExplicitPort);
    SPANIO_CHECK(opts.m_threads == kExplicitThreads);

    const char* const quiet_last[] = {"echo_server", "9000", "3", "--quiet"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(quiet_last), quiet_last, opts, error));
    SPANIO_CHECK(opts.m_quiet);
    SPANIO_CHECK(opts.m_threads == kExplicitThreads);

    const char* const not_quiet[] = {"echo_server", "9000"};
    SPANIO_CHECK(echo::parse_arguments(argc_of(not_quiet), not_quiet, opts, error));
    SPANIO_CHECK(!opts.m_quiet);

    const char* const unknown[] = {"echo_server", "--verbose"};
    error.clear();
    SPANIO_CHECK(!echo::parse_arguments(argc_of(unknown), unknown, opts, error));
    SPANIO_CHECK(error.find("--verbose") != std::string::npos);

    const char* const too_many[] = {"echo_server", "9000", "3", "--quiet", "4"};
    error.clear();
    SPANIO_CHECK(!echo::parse_arguments(argc_of(too_many), too_many, opts, error));
    SPANIO_CHECK(!error.empty());

    // Without a sink the log is off, and the server still echoes
    SPANIO_CHECK(!echo::event_log(echo::log_sink()).enabled());
    std::unique_ptr<io_context> ctx = make_context(backends().front());
    std::unique_ptr<echo::echo_server> server = make_server(*ctx);
    server_runner runner(*ctx, *server, kSingleThread);
    socket_guard client;
    SPANIO_REQUIRE(connect_client(server_port(*server), client));
    SPANIO_CHECK(send_all(client.get(), kLogMessage, kLogMessageLength));
    char reply[kLogMessageLength] = {};
    SPANIO_CHECK(recv_exact(client.get(), reply, kLogMessageLength));
    SPANIO_CHECK(std::memcmp(reply, kLogMessage, kLogMessageLength) == 0);
}

// Case 15: addresses are shown as a.b.c.d:port and [v6]:port
SPANIO_TEST(format_endpoint)
{
    SPANIO_CHECK(echo::format_endpoint(spanio::tcp::endpoint(spanio::ipv4::loopback(), kFormatPort))
                 == "127.0.0.1:8080");
    SPANIO_CHECK(echo::format_endpoint(spanio::tcp::endpoint(spanio::ipv6::loopback(), kFormatPort))
                 == "[::1]:8080");
    SPANIO_CHECK(echo::format_endpoint(spanio::tcp::endpoint(spanio::ip::make_address("fe80::1"), kFormatPort))
                 == "[fe80::1]:8080");
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
