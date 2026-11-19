// ============================================================================
// File purpose: TCP echo server written with C++20 coroutines: one coroutine
//               accepts connections, and each connection is served by its
//               own coroutine that reads and writes back in a plain loop.
//               Compare with echo_server.cpp, the callback version built on
//               tcp_server.
// Usage:        coro_echo_server [port] [threads]
//                 port     port to use, 1-65535 (default 9000)
//                 threads  threads calling run(), 1-64 (default 1)
//               The server binds 0.0.0.0 and runs until the process is
//               killed.
//
//               Single-file build (C++20), from the repository root:
//                 Linux:   g++ -std=c++20 -Iinclude examples/coro_echo_server.cpp -o coro_echo_server -lpthread
//                 Windows: cl /std:c++20 /EHsc /Iinclude examples\coro_echo_server.cpp ws2_32.lib
//               CMake builds it as the coro_echo_server target when
//               SPANIO_BUILD_EXAMPLES is on (the default at top level).
//
//               The unit tests include this file with
//               SPANIO_CORO_ECHO_SERVER_NO_MAIN defined, so they exercise
//               exactly this code without its main().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19
// ============================================================================

#include "spanio/coroutine.h"
#include "spanio/tcp.h"

#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace coro_echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9000;

// Threads used when none is given on the command line
constexpr unsigned long kDefaultThreads = 1;

// Largest port number
constexpr unsigned long kMaxPort = 65535;

// Most threads accepted on the command line
constexpr unsigned long kMaxThreads = 64;

// Most command-line arguments, including the program name
constexpr int kMaxArguments = 3;

// Position of the port argument in argv
constexpr int kPortArgument = 1;

// Position of the thread-count argument in argv
constexpr int kThreadsArgument = 2;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Bytes read from a connection at a time
constexpr std::size_t kChunkSize = 4096;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Parse one positive decimal number
// Parameter text:  the argument
// Parameter limit: largest accepted value
// Parameter value: receives the number on success
// Return value: true if text is a number in 1..limit
inline bool parse_number(const char* text, unsigned long limit, unsigned long& value)
{
    if (!std::isdigit(static_cast<unsigned char>(text[0])))
    {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, kDecimalBase);
    if (errno != 0 || *end != '\0' || parsed == 0 || parsed > limit)
    {
        return false;
    }
    value = parsed;
    return true;
}

// Read the command line: coro_echo_server [port] [threads]
// Parameter argc:    argument count, as passed to main()
// Parameter argv:    arguments, as passed to main()
// Parameter port:    receives the port on success
// Parameter threads: receives the thread count on success
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, std::uint16_t& port, unsigned long& threads)
{
    if (argc > kMaxArguments)
    {
        return false;
    }
    unsigned long parsed_port = kDefaultPort;
    if (argc > kPortArgument && !parse_number(argv[kPortArgument], kMaxPort, parsed_port))
    {
        return false;
    }
    unsigned long parsed_threads = kDefaultThreads;
    if (argc > kThreadsArgument && !parse_number(argv[kThreadsArgument], kMaxThreads, parsed_threads))
    {
        return false;
    }
    port = static_cast<std::uint16_t>(parsed_port);
    threads = parsed_threads;
    return true;
}

// Serve one connection: send back everything it sends, until it closes or
// fails. Errors are values here (as_tuple), so the loop needs no try/catch.
// Parameter sock: the connection; owned by this coroutine
inline spanio::awaitable<void> session(spanio::tcp::socket sock)
{
    char data[kChunkSize];
    for (;;)
    {
        auto [read_ec, bytes] =
            co_await sock.async_read_some(spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
        if (read_ec)
        {
            // End of file is the normal end; anything else is logged
            if (read_ec != spanio::error::misc_errors::eof)
            {
                spanio::detail::log_error("coro_echo_server: read", read_ec);
            }
            co_return;
        }
        auto [write_ec, written] = co_await sock.async_write(spanio::buffer(data, bytes),
                                                             spanio::as_tuple(spanio::use_awaitable));
        static_cast<void>(written);
        if (write_ec)
        {
            spanio::detail::log_error("coro_echo_server: write", write_ec);
            co_return;
        }
    }
}

// Accept connections until the acceptor is closed, starting a session
// coroutine for each
// Parameter acceptor: listening acceptor; must outlive the coroutine
inline spanio::awaitable<void> accept_loop(spanio::tcp::acceptor& acceptor)
{
    for (;;)
    {
        auto [ec, peer] = co_await acceptor.async_accept(spanio::as_tuple(spanio::use_awaitable));
        if (ec)
        {
            // operation_canceled: the acceptor was closed, so the server stops
            if (ec != std::errc::operation_canceled)
            {
                spanio::detail::log_error("coro_echo_server: accept", ec);
            }
            co_return;
        }
        spanio::io_context& context = peer.context();
        spanio::co_spawn(context, session(std::move(peer)), spanio::detached);
    }
}

// Start serving on an open, listening acceptor; the caller runs the context
// Parameter context:  context of the acceptor
// Parameter acceptor: listening acceptor; must outlive the server
inline void start(spanio::io_context& context, spanio::tcp::acceptor& acceptor)
{
    spanio::co_spawn(context, accept_loop(acceptor), spanio::detached);
}

}

#if !defined(SPANIO_CORO_ECHO_SERVER_NO_MAIN)

// Parse the command line and run the server
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               server could not start
int main(int argc, char** argv)
{
    std::uint16_t port = coro_echo::kDefaultPort;
    unsigned long threads = coro_echo::kDefaultThreads;
    if (!coro_echo::parse_arguments(argc, argv, port, threads))
    {
        std::fprintf(stderr, "usage: coro_echo_server [port] [threads]\n");
        return coro_echo::kExitUsage;
    }
    try
    {
        spanio::io_context context;
        spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::any(), port));
        coro_echo::start(context, acceptor);
        std::printf("coro_echo_server on port %u, %lu thread(s), backend %s\n", static_cast<unsigned>(port),
                    threads, context.backend_name());
        std::fflush(stdout);

        auto runner = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> pool;
        for (unsigned long i = 1; i < threads; ++i)
        {
            pool.emplace_back(runner);
        }
        context.run();
        for (std::thread& thread : pool)
        {
            thread.join();
        }
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "coro_echo_server: %s\n", e.what());
        return coro_echo::kExitFailure;
    }
    return coro_echo::kExitSuccess;
}

#endif
