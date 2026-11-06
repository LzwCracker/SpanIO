// ============================================================================
// File purpose: UDP echo server: every datagram goes back to its sender. It
//               is built on udp_server, so the service itself is one
//               callback.
// Usage:        udp_echo_server [port]
//                 port  port to use, 1-65535 (default 9000)
//               The server binds 0.0.0.0 and runs until the process is
//               killed; Ctrl+C handling needs a signal wrapper (planned).
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/udp_echo_server.cpp -o udp_echo_server -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\udp_echo_server.cpp ws2_32.lib
//               CMake builds it as the udp_echo_server target when
//               SPANIO_BUILD_EXAMPLES is on (the default at top level).
//
//               The unit tests include this file with
//               SPANIO_UDP_ECHO_SERVER_NO_MAIN defined, so they exercise
//               exactly this code without its main().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#include "spanio/udp_server.h"

#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <system_error>

namespace udp_echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9000;

// Largest port number
constexpr unsigned long kMaxPort = 65535;

// Most command-line arguments, including the program name
constexpr int kMaxArguments = 2;

// Position of the port argument in argv
constexpr int kPortArgument = 1;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Read the command line: udp_echo_server [port]
// Parameter argc: argument count, as passed to main()
// Parameter argv: arguments, as passed to main()
// Parameter port: receives the port on success
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, std::uint16_t& port)
{
    if (argc > kMaxArguments)
    {
        return false;
    }
    if (argc <= kPortArgument)
    {
        port = kDefaultPort;
        return true;
    }
    const char* text = argv[kPortArgument];
    if (!std::isdigit(static_cast<unsigned char>(text[0])))
    {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, kDecimalBase);
    if (errno != 0 || *end != '\0' || parsed == 0 || parsed > kMaxPort)
    {
        return false;
    }
    port = static_cast<std::uint16_t>(parsed);
    return true;
}

// The echo service: a udp_server whose callback sends each datagram back
class udp_echo_server
{
public:
    // Bind 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port: port to use
    explicit udp_echo_server(std::uint16_t port)
        : m_server(port)
    {
        install_callback();
    }

    // Bind an address on a context the caller runs; use start(), not run().
    // Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to bind; port 0 picks one
    udp_echo_server(spanio::io_context& context, const spanio::udp::endpoint& local)
        : m_server(context, local)
    {
        install_callback();
    }

    // Start receiving, without blocking
    void start()
    {
        m_server.start();
    }

    // Run on the owned context until stop()
    void run()
    {
        m_server.run();
    }

    // Stop the server; safe from any thread
    void stop()
    {
        m_server.stop();
    }

    // Address the server is bound to
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    spanio::udp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_server.local_endpoint(ec);
    }

    // What the server has done so far
    // Return value: a snapshot of the counters
    spanio::udp_server_stats stats() const noexcept
    {
        return m_server.stats();
    }

    // Name of the I/O backend, for the startup message
    // Return value: static string
    const char* backend_name()
    {
        return m_server.context().backend_name();
    }

private:
    // Send each datagram back to where it came from
    void install_callback()
    {
        spanio::udp_server* server = &m_server;
        auto echo = [server](const spanio::udp::endpoint& from, std::string_view data)
        {
            server->send_to(from, data);
        };
        m_server.on_datagram(echo);
    }

    // Receive loop, send queue and thread
    spanio::udp_server m_server;
};

}

#if !defined(SPANIO_UDP_ECHO_SERVER_NO_MAIN)

// Parse the command line and run the server
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               server could not start
int main(int argc, char** argv)
{
    std::uint16_t port = udp_echo::kDefaultPort;
    if (!udp_echo::parse_arguments(argc, argv, port))
    {
        std::fprintf(stderr, "usage: udp_echo_server [port]\n");
        return udp_echo::kExitUsage;
    }
    try
    {
        udp_echo::udp_echo_server server(port);
        std::printf("udp_echo_server on port %u, backend %s\n", static_cast<unsigned>(port),
                    server.backend_name());
        std::fflush(stdout);
        server.run();
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "udp_echo_server: %s\n", e.what());
        return udp_echo::kExitFailure;
    }
    return udp_echo::kExitSuccess;
}

#endif
