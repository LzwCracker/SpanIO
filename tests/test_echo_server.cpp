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
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::detail::native_handle_t;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::connect_to;
using spanio::test::loopback_address;
using spanio::test::make_context;
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

// Receive timeout of the blocking clients, so a server bug fails the case
// instead of hanging it
constexpr int kReceiveTimeoutMs = 5000;

// Milliseconds per second, for the Linux SO_RCVTIMEO timeval
constexpr int kMillisPerSecond = 1000;

// Microseconds per millisecond, for the Linux SO_RCVTIMEO timeval
constexpr int kMicrosPerMilli = 1000;

// Longest wait for the server's session count to change
constexpr int kSessionWaitMs = 5000;

// Pause between two checks of a condition being waited for
constexpr int kPollIntervalMs = 5;

// Upper bound for run() to return after stop(); covers Windows' ~2 s
// refused-connect retries
constexpr long long kStopLimitMs = 5000;

// recv/send flags for the blocking clients: none
constexpr int kNoFlags = 0;

// recv/send result that means the connection is closed
constexpr int kConnectionClosed = 0;

// Port given on the command line in the argument case ("9000")
constexpr std::uint16_t kExplicitPort = 9000;

// Thread count given on the command line in the argument case ("3")
constexpr unsigned kExplicitThreads = 3;

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

// Give a blocking socket a receive timeout
// Parameter handle: socket to configure
// Return value: true on success
bool set_receive_timeout(native_handle_t handle)
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    const DWORD timeout = kReceiveTimeoutMs;
#else
    timeval timeout{};
    timeout.tv_sec = kReceiveTimeoutMs / kMillisPerSecond;
    timeout.tv_usec = (kReceiveTimeoutMs % kMillisPerSecond) * kMicrosPerMilli;
#endif
    return ::setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout),
                        static_cast<spanio::detail::socklen_type>(sizeof(timeout)))
        != spanio::detail::kSocketCallFailed;
}

// Open a blocking client connected to the server on 127.0.0.1
// Parameter port:   server port
// Parameter client: receives the connected socket
// Return value: true on success; never calls the check macros, so it is
//               safe in helper threads
bool connect_client(std::uint16_t port, socket_guard& client)
{
    std::error_code ec;
    client.reset(spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    if (ec)
    {
        return false;
    }
    return set_receive_timeout(client.get()) && connect_to(client.get(), loopback_address(port)) == 0;
}

// Send a whole buffer on a blocking socket
// Parameter handle: connected socket
// Parameter data:   bytes to send
// Parameter size:   number of bytes
// Return value: true if every byte was sent
bool send_all(native_handle_t handle, const char* data, std::size_t size)
{
    std::size_t sent = 0;
    while (sent < size)
    {
        const int result = ::send(handle, data + sent, static_cast<int>(size - sent), kNoFlags);
        if (result <= kConnectionClosed)
        {
            return false;
        }
        sent += static_cast<std::size_t>(result);
    }
    return true;
}

// Receive exactly a number of bytes on a blocking socket
// Parameter handle: connected socket
// Parameter data:   destination
// Parameter size:   number of bytes to receive
// Return value: true if all of them arrived before a close, error or timeout
bool recv_exact(native_handle_t handle, char* data, std::size_t size)
{
    std::size_t received = 0;
    while (received < size)
    {
        const int result = ::recv(handle, data + received, static_cast<int>(size - received), kNoFlags);
        if (result <= kConnectionClosed)
        {
            return false;
        }
        received += static_cast<std::size_t>(result);
    }
    return true;
}

// Wait until the server serves a given number of sessions
// Parameter server:   server to watch
// Parameter expected: session count to wait for
// Return value: true if the count was reached within kSessionWaitMs
bool wait_for_sessions(const echo::echo_server& server, int expected)
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

    // Stop accepting and wait for every run() to return. run() returns once
    // the remaining sessions have ended, so clients must be closed first.
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
// Return value: the server, not started yet
std::unique_ptr<echo::echo_server> make_server(io_context& context)
{
    return std::unique_ptr<echo::echo_server>(
        new echo::echo_server(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort)));
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

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
