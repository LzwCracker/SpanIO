// ============================================================================
// File purpose: TCP echo server, the end-to-end example of SpanIO M1. Every
//               connection gets back exactly the bytes it sends. It is built
//               on tcp_server, which owns the connections, send queues and
//               threads, so the service itself is a few callbacks.
//               It prints one line per event:
//                 14:03:21.512 [connect] 127.0.0.1:54321  online 1
//                 14:03:23.008 [recv]    127.0.0.1:54321  5 bytes: hello
//                 14:03:30.115 [close]   127.0.0.1:54321  client closed, 5 bytes received, connected 8.6 s, online 0
//               Received data is shown on one line: control characters are
//               escaped (\r \n \t \\ \xNN), other bytes (UTF-8 text) are kept,
//               and only the first 64 bytes are shown, followed by
//               "... (N bytes)".
// Usage:        echo_server [port] [threads] [--quiet]
//                 port     listening port, 1-65535 (default 8080)
//                 threads  threads running the server, 1-256
//                          (default: number of CPU cores)
//                 --quiet  print nothing per connection (for benchmarks;
//                          printing every message costs time)
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
//               this code without its main(); they pass their own log sink
//               to capture the lines.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05 (connection log: 2026-11-11)
// ============================================================================

#include "spanio/tcp_server.h"

#include <any>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
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

// Most positional arguments (port and thread count)
constexpr int kMaxPositional = 2;

// Index of the port among the positional arguments
constexpr int kPortPosition = 0;

// Index of the thread count among the positional arguments
constexpr int kThreadsPosition = 1;

// Option that turns the per-connection log off
constexpr char kQuietOption[] = "--quiet";

// Arguments starting with this are options, not positional values
constexpr char kOptionPrefix[] = "--";

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Received bytes shown per [recv] line; the rest is summarized
constexpr std::size_t kShownBytes = 64;

// First byte value that is not an ASCII control character
constexpr unsigned char kFirstPrintable = 0x20;

// ASCII DEL, a control character
constexpr unsigned char kDelete = 0x7F;

// Bits 7-6 of a UTF-8 continuation byte are 10
constexpr unsigned char kUtf8ContinuationMask = 0xC0;

// Value of those bits in a continuation byte
constexpr unsigned char kUtf8ContinuationBits = 0x80;

// Digits of the escape \xNN
constexpr char kHexDigits[] = "0123456789ABCDEF";

// Bits in a hex digit
constexpr unsigned kBitsPerHexDigit = 4;

// Mask of the low hex digit of a byte
constexpr unsigned kLowNibbleMask = 0x0F;

// Milliseconds per second, for the timestamp
constexpr long long kMillisPerSecond = 1000;

// Size of the "HH:MM:SS" part of a timestamp, including the zero
constexpr std::size_t kClockTextSize = 16;

// Size of the ".mmm" part of a timestamp, including the zero
constexpr std::size_t kMillisTextSize = 8;

// Size of the text of a connection duration, including the zero
constexpr std::size_t kDurationTextSize = 32;

// Settings taken from the command line
struct options
{
    // Port to listen on
    std::uint16_t m_port = kDefaultPort;
    // Threads running the server
    unsigned m_threads = kFallbackThreads;
    // Print nothing per connection
    bool m_quiet = false;
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

// Read the command line: echo_server [port] [threads] [--quiet]. --quiet may
// stand anywhere.
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    options parsed;
    parsed.m_threads = default_threads();
    int positional = 0;
    for (int i = 1; i < argc; ++i)
    {
        const char* argument = argv[i];
        if (std::strcmp(argument, kQuietOption) == 0)
        {
            parsed.m_quiet = true;
            continue;
        }
        if (std::strncmp(argument, kOptionPrefix, std::strlen(kOptionPrefix)) == 0)
        {
            error = std::string("unknown option ") + argument;
            return false;
        }
        if (positional == kMaxPositional)
        {
            error = "too many arguments";
            return false;
        }
        unsigned long value = 0;
        if (positional == kPortPosition)
        {
            if (!parse_number(argument, kMinPort, kMaxPort, value))
            {
                error = "port must be a number from 1 to 65535";
                return false;
            }
            parsed.m_port = static_cast<std::uint16_t>(value);
        }
        else if (positional == kThreadsPosition)
        {
            if (!parse_number(argument, kMinThreads, kMaxThreads, value))
            {
                error = "threads must be a number from 1 to 256";
                return false;
            }
            parsed.m_threads = static_cast<unsigned>(value);
        }
        ++positional;
    }
    out = parsed;
    return true;
}

// ---- Connection log -----------------------------------------------------------

// Receives one finished log line, without its line end
using log_sink = std::function<void(const std::string& line)>;

// Sink that prints each line to stdout
// Return value: the sink
inline log_sink stdout_sink()
{
    auto print = [](const std::string& line)
    {
        std::fputs(line.c_str(), stdout);
        std::fputc('\n', stdout);
        std::fflush(stdout);
    };
    return log_sink(print);
}

// An address and port as text: 127.0.0.1:8080, or [::1]:8080 for IPv6
// Parameter endpoint: the endpoint
// Return value: the text
inline std::string format_endpoint(const spanio::tcp::endpoint& endpoint)
{
    const std::string address = endpoint.address().to_string();
    const std::string port = std::to_string(static_cast<unsigned>(endpoint.port()));
    if (endpoint.family() == AF_INET6)
    {
        return "[" + address + "]:" + port;
    }
    return address + ":" + port;
}

// Received bytes as one line of text: control characters escaped, at most
// kShownBytes bytes shown (never cutting a UTF-8 character), and a size note
// when the rest is left out
// Parameter data: the bytes
// Return value: the text
inline std::string escape_bytes(std::string_view data)
{
    std::size_t shown = data.size();
    if (shown > kShownBytes)
    {
        shown = kShownBytes;
        // Back up to the start of a UTF-8 character
        while (shown > 0
               && (static_cast<unsigned char>(data[shown]) & kUtf8ContinuationMask) == kUtf8ContinuationBits)
        {
            --shown;
        }
    }
    std::string text;
    for (std::size_t i = 0; i < shown; ++i)
    {
        const unsigned char byte = static_cast<unsigned char>(data[i]);
        if (byte == '\n')
        {
            text += "\\n";
        }
        else if (byte == '\r')
        {
            text += "\\r";
        }
        else if (byte == '\t')
        {
            text += "\\t";
        }
        else if (byte == '\\')
        {
            text += "\\\\";
        }
        else if (byte < kFirstPrintable || byte == kDelete)
        {
            text += "\\x";
            text += kHexDigits[byte >> kBitsPerHexDigit];
            text += kHexDigits[byte & kLowNibbleMask];
        }
        else
        {
            text += static_cast<char>(byte);
        }
    }
    if (shown < data.size())
    {
        text += "... (" + std::to_string(data.size()) + " bytes)";
    }
    return text;
}

// Local wall-clock time as HH:MM:SS.mmm
// Return value: the text
inline std::string timestamp()
{
    const std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    const std::time_t seconds = std::chrono::system_clock::to_time_t(now);
    const long long millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % kMillisPerSecond;
    std::tm local{};
#if defined(_WIN32)
    localtime_s(&local, &seconds);
#else
    localtime_r(&seconds, &local);
#endif
    char clock[kClockTextSize] = {};
    std::strftime(clock, sizeof(clock), "%H:%M:%S", &local);
    char fraction[kMillisTextSize] = {};
    std::snprintf(fraction, sizeof(fraction), ".%03lld", millis);
    return std::string(clock) + fraction;
}

// Why a connection ended, in words
// Parameter reason: the error tcp_server passed to on_close
// Return value: "client closed", "server closed", or the error's text
inline std::string close_reason_text(const std::error_code& reason)
{
    if (!reason)
    {
        return "server closed";
    }
    if (reason == spanio::error::misc_errors::eof)
    {
        return "client closed";
    }
    if (reason == std::errc::connection_reset)
    {
        return "connection reset";
    }
    if (reason == spanio::error::misc_errors::send_queue_full)
    {
        return "send queue full";
    }
    return reason.message();
}

// Serializes log lines: each line reaches the sink whole, even when several
// run() threads log at the same time. Without a sink nothing is logged.
class event_log
{
public:
    // Parameter sink: where lines go; empty turns logging off
    explicit event_log(log_sink sink)
        : m_sink(std::move(sink))
    {
    }

    // Whether lines are written at all
    // Return value: true when a sink is set
    bool enabled() const noexcept
    {
        return static_cast<bool>(m_sink);
    }

    // Write one event line, prefixed with the time and the event tag
    // Parameter tag:    event name padded to a common width, e.g. "[recv]   "
    // Parameter peer:   the client's address text
    // Parameter detail: the rest of the line
    void write(const char* tag, const std::string& peer, const std::string& detail)
    {
        const std::string line = timestamp() + " " + tag + " " + peer + "  " + detail;
        std::lock_guard<std::mutex> lock(m_mutex);
        m_sink(line);
    }

private:
    // Destination of the lines; empty when logging is off
    log_sink m_sink;
    // Keeps lines whole
    std::mutex m_mutex;
};

// What the server remembers about one connection, kept in its user_data().
// Only that connection's callbacks touch it, and they never run at the same
// time, so it needs no lock.
struct session_info
{
    // Client address text, computed once
    std::string m_peer;
    // When the connection was accepted
    std::chrono::steady_clock::time_point m_connected;
    // Bytes received so far
    std::uint64_t m_bytes = 0;
};

// State the callbacks share. It lives in a shared_ptr that the callbacks
// hold, because tcp_server may still run a callback while the echo_server
// object is being destroyed.
struct log_state
{
    // Parameter sink: where lines go; empty turns logging off
    explicit log_state(log_sink sink)
        : m_log(std::move(sink))
    {
    }

    // The log
    event_log m_log;
    // Connections between their [connect] and [close] lines
    std::atomic<std::size_t> m_online{0};
};

// The echo service: a tcp_server whose message callback sends received bytes
// back, plus connect / receive / close lines in the log. No framer is set,
// so each read is echoed (and logged) as it arrives.
class echo_server
{
public:
    // Listen on 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port: port to listen on
    // Parameter sink: where log lines go; default stdout, empty for none
    explicit echo_server(std::uint16_t port, log_sink sink = stdout_sink())
        : m_server(port)
        , m_state(std::make_shared<log_state>(std::move(sink)))
    {
        install_callbacks();
    }

    // Listen on an address, on a context the caller runs; use start(), not
    // run(). Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to listen on; port 0 picks one
    // Parameter sink:    where log lines go; default stdout, empty for none
    echo_server(spanio::io_context& context, const spanio::tcp::endpoint& local, log_sink sink = stdout_sink())
        : m_server(context, local)
        , m_state(std::make_shared<log_state>(std::move(sink)))
    {
        install_callbacks();
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
    // Set the callbacks. Without a log only on_message is set, so a quiet
    // server does no per-connection bookkeeping at all.
    void install_callbacks()
    {
        if (!m_state->m_log.enabled())
        {
            m_server.on_message(&echo_server::echo);
            return;
        }
        std::shared_ptr<log_state> state = m_state;
        auto on_connect = [state](const spanio::tcp_connection_ptr& connection)
        {
            session_info info;
            info.m_peer = format_endpoint(connection->remote_endpoint());
            info.m_connected = std::chrono::steady_clock::now();
            const std::size_t online = ++state->m_online;
            state->m_log.write("[connect]", info.m_peer, "online " + std::to_string(online));
            connection->user_data() = std::move(info);
        };
        auto on_message = [state](const spanio::tcp_connection_ptr& connection, std::string_view data)
        {
            session_info& info = std::any_cast<session_info&>(connection->user_data());
            info.m_bytes += data.size();
            state->m_log.write("[recv]   ", info.m_peer,
                               std::to_string(data.size()) + " bytes: " + escape_bytes(data));
            echo(connection, data);
        };
        auto on_close = [state](const spanio::tcp_connection_ptr& connection, std::error_code reason)
        {
            const session_info* info = std::any_cast<session_info>(&connection->user_data());
            if (info == nullptr)
            {
                // Closed before on_connect ran (e.g. over the connection
                // limit): it was never logged as connected
                return;
            }
            const std::chrono::duration<double> lasted = std::chrono::steady_clock::now() - info->m_connected;
            char duration[kDurationTextSize] = {};
            std::snprintf(duration, sizeof(duration), "%.1f s", lasted.count());
            const std::size_t online = --state->m_online;
            state->m_log.write("[close]  ", info->m_peer,
                               close_reason_text(reason) + ", " + std::to_string(info->m_bytes)
                                   + " bytes received, connected " + duration + ", online "
                                   + std::to_string(online));
        };
        m_server.on_connect(on_connect);
        m_server.on_message(on_message);
        m_server.on_close(on_close);
    }

    // Send received bytes back
    // Parameter connection: the client
    // Parameter data:       the bytes, valid during this call; send() copies
    static void echo(const spanio::tcp_connection_ptr& connection, std::string_view data)
    {
        connection->send(data);
    }

    // Connections, send queues and threads
    spanio::tcp_server m_server;
    // Log and online count, shared with the callbacks
    std::shared_ptr<log_state> m_state;
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
        std::fprintf(stderr, "echo_server: %s\nusage: echo_server [port] [threads] [--quiet]\n", error.c_str());
        return echo::kExitUsage;
    }
    try
    {
        echo::echo_server server(opts.m_port, opts.m_quiet ? echo::log_sink() : echo::stdout_sink());
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
