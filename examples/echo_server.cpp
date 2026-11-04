// ============================================================================
// File purpose: TCP echo server, the end-to-end example of SpanIO M1. Every
//               connection gets an echo_session that reads whatever arrives
//               and writes exactly those bytes back, until the client
//               disconnects. Several threads can run the io_context at once.
// Usage:        echo_server [port] [threads]
//                 port     listening port, 1-65535 (default 8080)
//                 threads  threads calling io_context::run(), 1-256
//                          (default: number of CPU cores)
//               The server listens on 0.0.0.0 and runs until the process is
//               killed; Ctrl+C handling needs a signal wrapper (planned).
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

#include "spanio/io_context.h"
#include "spanio/tcp.h"

#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

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

// Bytes each session reads at a time
constexpr std::size_t kSessionBufferSize = 4096;

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
    // Threads that call io_context::run()
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

// Number of live sessions, shared between the server and its sessions. It
// is held through a shared_ptr because sessions can outlive the server
// object (their handlers may still be queued in the io_context).
using session_counter = std::shared_ptr<std::atomic<int>>;

// One client connection: reads whatever arrives and writes it back, until
// the client disconnects or an error occurs. It is kept alive by the
// shared_ptr each pending handler holds, and closes its socket when the
// last one is gone.
class echo_session : public std::enable_shared_from_this<echo_session>
{
public:
    // Parameter socket: the accepted connection
    // Parameter active: live-session counter, incremented here and
    //                   decremented by the destructor
    echo_session(spanio::tcp::socket socket, session_counter active)
        : m_socket(std::move(socket))
        , m_active(std::move(active))
        , m_buffer{}
    {
        ++*m_active;
    }

    // Closes the connection (through the socket's destructor)
    ~echo_session()
    {
        --*m_active;
    }

    echo_session(const echo_session&) = delete;
    echo_session& operator=(const echo_session&) = delete;

    // Start echoing
    void start()
    {
        read();
    }

private:
    // Read the next piece of data
    void read()
    {
        std::shared_ptr<echo_session> self = shared_from_this();
        auto read_done = [self](std::error_code ec, std::size_t bytes)
        {
            self->on_read(ec, bytes);
        };
        m_socket.async_read_some(spanio::buffer(m_buffer.data(), m_buffer.size()), read_done);
    }

    // Write a piece back, or end the session
    // Parameter ec:    read result; spanio::error::eof when the client closed
    // Parameter bytes: bytes read into m_buffer
    void on_read(std::error_code ec, std::size_t bytes)
    {
        if (ec)
        {
            if (ec != spanio::error::eof && ec != std::errc::operation_canceled)
            {
                std::fprintf(stderr, "echo_server: read failed: %s\n", ec.message().c_str());
            }
            return;
        }
        std::shared_ptr<echo_session> self = shared_from_this();
        auto write_done = [self](std::error_code write_ec, std::size_t)
        {
            self->on_written(write_ec);
        };
        m_socket.async_write(spanio::buffer(m_buffer.data(), bytes), write_done);
    }

    // Continue with the next read once the echo was sent
    // Parameter ec: write result
    void on_written(std::error_code ec)
    {
        if (ec)
        {
            if (ec != std::errc::operation_canceled)
            {
                std::fprintf(stderr, "echo_server: write failed: %s\n", ec.message().c_str());
            }
            return;
        }
        read();
    }

    // The client connection
    spanio::tcp::socket m_socket;
    // Live-session counter shared with the server
    session_counter m_active;
    // Data read from the client, written back unchanged
    std::array<char, kSessionBufferSize> m_buffer;
};

// Accepts connections and starts an echo_session for each. The acceptor is
// guarded by a mutex because stop() may be called from any thread while an
// accept handler re-arms the acceptor on a run() thread.
class echo_server
{
public:
    // Open, bind and listen; throws std::system_error on failure
    // Parameter context: context that runs the server; must outlive it
    // Parameter local:   address and port to listen on; port 0 picks one
    echo_server(spanio::io_context& context, const spanio::tcp::endpoint& local)
        : m_acceptor(context, local)
        , m_active(std::make_shared<std::atomic<int>>(0))
    {
    }

    echo_server(const echo_server&) = delete;
    echo_server& operator=(const echo_server&) = delete;

    // Start accepting connections
    void start()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        accept_next_locked();
    }

    // Stop accepting: closes the acceptor, so new connections are refused.
    // Sessions already running continue until their clients disconnect.
    // Safe from any thread.
    void stop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_acceptor.close();
    }

    // Address the server listens on, e.g. to learn the port picked for 0
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    spanio::tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_acceptor.local_endpoint(ec);
    }

    // Number of client connections currently being served
    // Return value: live sessions
    int active_sessions() const
    {
        return m_active->load();
    }

private:
    // Start the next accept, unless the server was stopped. Caller holds
    // m_mutex.
    void accept_next_locked()
    {
        if (!m_acceptor.is_open())
        {
            return;
        }
        auto accept_done = [this](std::error_code ec, spanio::tcp::socket peer)
        {
            this->on_accept(ec, std::move(peer));
        };
        m_acceptor.async_accept(accept_done);
    }

    // Start a session for a new connection and accept the next one
    // Parameter ec:   accept result
    // Parameter peer: the new connection on success
    void on_accept(std::error_code ec, spanio::tcp::socket peer)
    {
        if (ec == std::errc::operation_canceled)
        {
            // stop() closed the acceptor
            return;
        }
        if (ec)
        {
            std::fprintf(stderr, "echo_server: accept failed: %s\n", ec.message().c_str());
        }
        else
        {
            std::make_shared<echo_session>(std::move(peer), m_active)->start();
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        accept_next_locked();
    }

    // Guards m_acceptor between stop() and the accept handlers
    mutable std::mutex m_mutex;
    // Listening socket
    spanio::tcp::acceptor m_acceptor;
    // Live-session counter shared with every session
    session_counter m_active;
};

}

#if !defined(SPANIO_ECHO_SERVER_NO_MAIN)

// Parse the command line, start the server, and run the context on the
// requested number of threads
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
        spanio::io_context context;
        echo::echo_server server(context, spanio::tcp::endpoint(spanio::ipv4::any(), opts.m_port));
        server.start();
        std::error_code ec;
        const spanio::tcp::endpoint local = server.local_endpoint(ec);
        std::printf("echo_server listening on %s:%u with %u thread(s), backend %s\n",
                    local.address().to_string().c_str(), static_cast<unsigned>(local.port()),
                    opts.m_threads, context.backend_name());
        std::fflush(stdout);

        auto run_context = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> workers;
        for (unsigned i = 1; i < opts.m_threads; ++i)
        {
            workers.emplace_back(run_context);
        }
        run_context();
        for (std::thread& worker : workers)
        {
            worker.join();
        }
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "echo_server: %s\n", e.what());
        return echo::kExitFailure;
    }
    return echo::kExitSuccess;
}

#endif
