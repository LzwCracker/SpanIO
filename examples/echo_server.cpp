// ============================================================================
// File purpose: TCP echo server, the end-to-end example of SpanIO M1. Every
//               connection gets back exactly the bytes it sends. It is built
//               on tcp_server, which owns the connections, send queues and
//               threads, so the service itself is one callback.
// Usage:        echo_server [port] [threads]
//                 port     listening port, 1-65535 (default 8080)
//                 threads  threads running the server, 1-256
//                          (default: number of CPU cores)
//               The server listens on 0.0.0.0 and runs until the process is
//               killed; Ctrl+C handling needs a signal wrapper (planned).
//               A client that sends far more than it reads back is
//               disconnected once 16 MiB of echoes are queued for it (the
//               tcp_server default).
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/echo_server.cpp -o echo_server -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\echo_server.cpp ws2_32.lib
//               CMake builds it as the echo_server target when
//               SPANIO_BUILD_EXAMPLES is on (the default at top level).
//
//               The unit tests include this file with
//               SPANIO_ECHO_SERVER_NO_MAIN defined, so they exercise exactly
//               this code without its main().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05
// ============================================================================

#include "spanio/tcp_server.h"

#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

namespace echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 8080;

// Smallest port a server may listen on (0 would mean "any port")
constexpr unsigned long kMinPort = 1;

// Largest TCP port
constexpr unsigned long kMaxPort = 65535;

// Fewest threads that can run the context
constexpr unsigned long kMinThreads = 1;

// Most threads accepted on the command line; a sanity limit
constexpr unsigned long kMaxThreads = 256;

// Thread count used when the number of CPU cores cannot be determined
constexpr unsigned kFallbackThreads = 1;

// Most command-line arguments, including the program name
constexpr int kMaxArguments = 3;

// Position of the port argument in argv
constexpr int kPortArgument = 1;

// Position of the thread count argument in argv
constexpr int kThreadsArgument = 2;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Settings taken from the command line
struct options
{
    // Port to listen on
    std::uint16_t m_port = kDefaultPort;
    // Threads running the server
    unsigned m_threads = kFallbackThreads;
};

// Parse an unsigned decimal number within a range. Signs, spaces and
// trailing characters are rejected.
// Parameter text:  the argument
// Parameter low:   smallest accepted value
// Parameter high:  largest accepted value
// Parameter value: receives the number on success
// Return value: true if text is a number in [low, high]
inline bool parse_number(const char* text, unsigned long low, unsigned long high, unsigned long& value)
{
    if (text == nullptr || !std::isdigit(static_cast<unsigned char>(text[0])))
    {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, kDecimalBase);
    if (errno != 0 || end == text || *end != '\0' || parsed < low || parsed > high)
    {
        return false;
    }
    value = parsed;
    return true;
}

// Number of threads to use when none is given: one per CPU core
// Return value: at least kFallbackThreads
inline unsigned default_threads()
{
    const unsigned cores = std::thread::hardware_concurrency();
    return cores == 0 ? kFallbackThreads : cores;
}

// Read the command line: echo_server [port] [threads]
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    options parsed;
    parsed.m_threads = default_threads();
    if (argc > kMaxArguments)
    {
        error = "too many arguments";
        return false;
    }
    if (argc > kPortArgument)
    {
        unsigned long port = 0;
        if (!parse_number(argv[kPortArgument], kMinPort, kMaxPort, port))
        {
            error = "port must be a number from 1 to 65535";
            return false;
        }
        parsed.m_port = static_cast<std::uint16_t>(port);
    }
    if (argc > kThreadsArgument)
    {
        unsigned long threads = 0;
        if (!parse_number(argv[kThreadsArgument], kMinThreads, kMaxThreads, threads))
        {
            error = "threads must be a number from 1 to 256";
            return false;
        }
        parsed.m_threads = static_cast<unsigned>(threads);
    }
    out = parsed;
    return true;
}


// The echo service: a tcp_server whose only callback sends received bytes
// back. No framer is set, so each read is echoed as it arrives.
class echo_server
{
public:
    // Listen on 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port: port to listen on
    explicit echo_server(std::uint16_t port)
        : m_server(port)
    {
        m_server.on_message(&echo_server::on_message);
    }

    // Listen on an address, on a context the caller runs; use start(), not
    // run(). Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to listen on; port 0 picks one
    echo_server(spanio::io_context& context, const spanio::tcp::endpoint& local)
        : m_server(context, local)
    {
        m_server.on_message(&echo_server::on_message);
    }

    // Start accepting, without blocking
    void start()
    {
        m_server.start();
    }

    // Run on the owned context until stop()
    // Parameter threads: threads to run on, including the calling one
    void run(unsigned threads)
    {
        m_server.run(threads);
    }

    // Disconnect every client and stop accepting; safe from any thread
    void stop()
    {
        m_server.stop();
    }

    // Address the server listens on, e.g. to learn the port picked for 0
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    spanio::tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_server.local_endpoint(ec);
    }

    // Number of client connections currently being served
    // Return value: live connections
    std::size_t active_sessions() const
    {
        return m_server.connection_count();
    }

    // Name of the I/O backend, for the startup message
    // Return value: static string
    const char* backend_name()
    {
        return m_server.context().backend_name();
    }

private:
    // Send received bytes back
    // Parameter connection: the client
    // Parameter data:       the bytes, valid during this call; send() copies
    static void on_message(const spanio::tcp_connection_ptr& connection, std::string_view data)
    {
        connection->send(data);
    }

    // Connections, send queues and threads
    spanio::tcp_server m_server;
};

}

#if !defined(SPANIO_ECHO_SERVER_NO_MAIN)

// Parse the command line and run the server
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               server could not start
int main(int argc, char** argv)
{
    echo::options opts;
    std::string error;
    if (!echo::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "echo_server: %s\nusage: echo_server [port] [threads]\n", error.c_str());
        return echo::kExitUsage;
    }
    try
    {
        echo::echo_server server(opts.m_port);
        std::printf("echo_server listening on port %u with %u thread(s), backend %s\n",
                    static_cast<unsigned>(opts.m_port), opts.m_threads, server.backend_name());
        std::fflush(stdout);
        server.run(opts.m_threads);
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "echo_server: %s\n", e.what());
        return echo::kExitFailure;
    }
    return echo::kExitSuccess;
}

#endif
