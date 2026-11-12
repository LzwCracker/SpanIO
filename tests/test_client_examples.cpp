// ============================================================================
// File purpose: Unit tests for the client examples examples/tcp_client.cpp
//               and examples/udp_client.cpp. The examples are included with
//               their main() switched off, together with the echo servers
//               they talk to, so the tests run exactly the shipped code: a
//               server runs on its own context and thread, the client's
//               run_xxx_client() on the test thread.
// Usage:        Built as the test_client_examples executable by CMake. Run it
//               with no arguments for every case, or with a substring of a
//               case name, e.g. `test_client_examples udp`.
//
//               The I/O cases run once per proactor backend of the platform.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

#define SPANIO_ECHO_SERVER_NO_MAIN
#define SPANIO_UDP_ECHO_SERVER_NO_MAIN
#define SPANIO_TCP_CLIENT_NO_MAIN
#define SPANIO_UDP_CLIENT_NO_MAIN
#include "../examples/echo_server.cpp"
#include "../examples/tcp_client.cpp"
#include "../examples/udp_client.cpp"
#include "../examples/udp_echo_server.cpp"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;
using spanio::test::socket_guard;

namespace {

// Clock for measuring how long a client took
using test_clock = std::chrono::steady_clock;

// Limit for exchanges that are expected to succeed
constexpr std::chrono::milliseconds kGenerousLimit(10000);

// Limit for exchanges that are expected to time out
constexpr std::chrono::milliseconds kShortLimit(200);

// A timed-out exchange must return well before this
constexpr std::chrono::seconds kTimeoutUpperBound(2);

// Hosts every client case is tried with: numeric, and a name the resolver
// must look up (it may list ::1 before 127.0.0.1)
const char* const kHosts[] = {"127.0.0.1", "localhost"};

// Port as the decimal text the clients take
// Parameter port: port number
// Return value: the text
std::string port_text(std::uint16_t port)
{
    return std::to_string(static_cast<unsigned>(port));
}

// The TCP echo example on its own context and thread, listening on
// 127.0.0.1 at a port the system picks
class tcp_echo_fixture
{
public:
    // Parameter backend: proactor backend of the server's context
    explicit tcp_echo_fixture(const backend_factory& backend)
        : m_context(make_context(backend))
        , m_server(*m_context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort), echo::log_sink())
    {
        m_server.start();
        spanio::io_context& context = *m_context;
        auto run_context = [&context]()
        {
            context.run();
        };
        m_thread = std::thread(run_context);
    }

    // Stop the server and join its thread
    ~tcp_echo_fixture()
    {
        m_server.stop();
        m_thread.join();
    }

    tcp_echo_fixture(const tcp_echo_fixture&) = delete;
    tcp_echo_fixture& operator=(const tcp_echo_fixture&) = delete;

    // Port the server listens on
    // Return value: the port as text
    std::string port() const
    {
        std::error_code ec;
        const spanio::tcp::endpoint local = m_server.local_endpoint(ec);
        return ec ? std::string() : port_text(local.port());
    }

private:
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The example server
    echo::echo_server m_server;
    // Thread running m_context
    std::thread m_thread;
};

// The UDP echo example on its own context and thread, bound to 127.0.0.1 at
// a port the system picks
class udp_echo_fixture
{
public:
    // Parameter backend: proactor backend of the server's context
    explicit udp_echo_fixture(const backend_factory& backend)
        : m_context(make_context(backend))
        , m_server(*m_context, spanio::udp::endpoint(spanio::ipv4::loopback(), kAnyPort))
    {
        m_server.start();
        spanio::io_context& context = *m_context;
        auto run_context = [&context]()
        {
            context.run();
        };
        m_thread = std::thread(run_context);
    }

    // Stop the server and join its thread
    ~udp_echo_fixture()
    {
        m_server.stop();
        m_thread.join();
    }

    udp_echo_fixture(const udp_echo_fixture&) = delete;
    udp_echo_fixture& operator=(const udp_echo_fixture&) = delete;

    // Port the server is bound to
    // Return value: the port as text
    std::string port() const
    {
        std::error_code ec;
        const spanio::udp::endpoint local = m_server.local_endpoint(ec);
        return ec ? std::string() : port_text(local.port());
    }

private:
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The example server
    udp_echo::udp_echo_server m_server;
    // Thread running m_context
    std::thread m_thread;
};

// A TCP port on 127.0.0.1 where nothing listens: one the system just handed
// out, closed again
// Return value: the port as text
std::string closed_tcp_port()
{
    socket_guard listener(spanio::test::make_loopback_listener());
    return port_text(ntohs(spanio::test::local_address(listener.get()).sin_port));
}

// A UDP port on 127.0.0.1 where nothing is bound: one the system just
// handed out, closed again
// Parameter ctx: context for the temporary socket
// Return value: the port as text
std::string closed_udp_port(io_context& ctx)
{
    spanio::udp::socket temporary(ctx);
    SPANIO_REQUIRE(!temporary.open(AF_INET));
    SPANIO_REQUIRE(!temporary.bind(spanio::udp::endpoint(spanio::ipv4::loopback(), kAnyPort)));
    std::error_code ec;
    const spanio::udp::endpoint local = temporary.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return port_text(local.port());
}

// Command lines that each client must reject
// Return value: argument vectors, argv[0] included
std::vector<std::vector<const char*>> bad_command_lines()
{
    return {
        {"client"},
        {"client", "127.0.0.1", "80"},
        {"client", "127.0.0.1", "80", "m", "extra"},
        {"client", "127.0.0.1", "abc", "m"},
        {"client", "127.0.0.1", "-80", "m"},
        {"client", "127.0.0.1", "0", "m"},
        {"client", "127.0.0.1", "70000", "m"},
        {"client", "127.0.0.1", "80x", "m"},
        {"client", "", "80", "m"},
    };
}

}

// Case 1: the TCP client gets the echo back, with a numeric host and with a
// name the resolver looks up
SPANIO_TEST(tcp_client_echo)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        tcp_echo_fixture server(backend);
        SPANIO_REQUIRE(!server.port().empty());
        std::unique_ptr<io_context> ctx = make_context(backend);
        for (const char* host : kHosts)
        {
            std::string reply;
            const std::error_code ec = tcp_client_example::run_tcp_client(*ctx, host, server.port(), "hello", reply,
                                                                         kGenerousLimit);
            SPANIO_CHECK(!ec);
            SPANIO_CHECK(reply == "hello");
        }
    }
}

// Case 2: no server on the port: connection_refused
SPANIO_TEST(tcp_client_refused)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::string reply;
        const std::error_code ec = tcp_client_example::run_tcp_client(*ctx, "127.0.0.1", closed_tcp_port(), "hello",
                                                                     reply, kGenerousLimit);
        SPANIO_CHECK(ec == std::errc::connection_refused);
    }
}

// Case 3: the server accepts but never answers: timed_out, soon after the
// limit
SPANIO_TEST(tcp_client_timeout)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        // A listening socket nobody accepts on: the system completes the
        // connection and buffers the data, but no reply ever comes
        socket_guard listener(spanio::test::make_loopback_listener());
        const std::string port = port_text(ntohs(spanio::test::local_address(listener.get()).sin_port));
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::string reply;
        const test_clock::time_point start = test_clock::now();
        const std::error_code ec = tcp_client_example::run_tcp_client(*ctx, "127.0.0.1", port, "hello", reply,
                                                                     kShortLimit);
        const test_clock::duration took = test_clock::now() - start;
        SPANIO_CHECK(ec == std::errc::timed_out);
        SPANIO_CHECK(took >= kShortLimit);
        SPANIO_CHECK(took < kTimeoutUpperBound);
    }
}

// Case 4: the TCP client rejects bad command lines and accepts a good one
SPANIO_TEST(tcp_client_bad_args)
{
    for (const std::vector<const char*>& args : bad_command_lines())
    {
        tcp_client_example::options opts;
        std::string error;
        SPANIO_CHECK(!tcp_client_example::parse_arguments(static_cast<int>(args.size()), args.data(), opts, error));
        SPANIO_CHECK(!error.empty());
    }
    const std::vector<const char*> empty_message{"client", "127.0.0.1", "80", ""};
    tcp_client_example::options opts;
    std::string error;
    SPANIO_CHECK(!tcp_client_example::parse_arguments(static_cast<int>(empty_message.size()), empty_message.data(),
                                                      opts, error));

    const std::vector<const char*> good{"client", "localhost", "65535", "hi"};
    error.clear();
    SPANIO_REQUIRE(tcp_client_example::parse_arguments(static_cast<int>(good.size()), good.data(), opts, error));
    SPANIO_CHECK(opts.m_host == "localhost");
    SPANIO_CHECK(opts.m_port == "65535");
    SPANIO_CHECK(opts.m_message == "hi");
}

// Case 5: the UDP client gets the datagram back, with a numeric host and
// with a name the resolver looks up
SPANIO_TEST(udp_client_echo)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        udp_echo_fixture server(backend);
        SPANIO_REQUIRE(!server.port().empty());
        std::unique_ptr<io_context> ctx = make_context(backend);
        for (const char* host : kHosts)
        {
            std::string reply;
            const std::error_code ec = udp_client_example::run_udp_client(*ctx, host, server.port(), "hello", reply,
                                                                         kGenerousLimit);
            SPANIO_CHECK(!ec);
            SPANIO_CHECK(reply == "hello");
        }
    }
}

// Case 6: nobody answers on the port: timed_out, soon after the limit
SPANIO_TEST(udp_client_timeout)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        const std::string port = closed_udp_port(*ctx);
        std::string reply;
        const test_clock::time_point start = test_clock::now();
        const std::error_code ec = udp_client_example::run_udp_client(*ctx, "127.0.0.1", port, "hello", reply,
                                                                     kShortLimit);
        const test_clock::duration took = test_clock::now() - start;
        SPANIO_CHECK(ec == std::errc::timed_out);
        SPANIO_CHECK(took >= kShortLimit);
        SPANIO_CHECK(took < kTimeoutUpperBound);
    }
}

// Case 7: the UDP client rejects bad command lines and accepts a good one,
// including an empty message (an empty datagram is valid)
SPANIO_TEST(udp_client_bad_args)
{
    for (const std::vector<const char*>& args : bad_command_lines())
    {
        udp_client_example::options opts;
        std::string error;
        SPANIO_CHECK(!udp_client_example::parse_arguments(static_cast<int>(args.size()), args.data(), opts, error));
        SPANIO_CHECK(!error.empty());
    }
    const std::vector<const char*> good{"client", "127.0.0.1", "9000", ""};
    udp_client_example::options opts;
    std::string error;
    SPANIO_REQUIRE(udp_client_example::parse_arguments(static_cast<int>(good.size()), good.data(), opts, error));
    SPANIO_CHECK(opts.m_host == "127.0.0.1");
    SPANIO_CHECK(opts.m_port == "9000");
    SPANIO_CHECK(opts.m_message.empty());
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
