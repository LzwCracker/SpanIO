// ============================================================================
// File purpose: TCP echo server written with C++20 coroutines: one coroutine
//               accepts connections, and each connection is served by its
//               own coroutine that reads and writes back in a plain loop.
//               Compare with echo_server.cpp, the callback version built on
//               tcp_server; both print the same lines, one per event:
//                 14:03:21.512 [connect] 127.0.0.1:54321  online 1
//                 14:03:23.008 [recv]    127.0.0.1:54321  5 bytes: hello
//                 14:03:30.115 [close]   127.0.0.1:54321  client closed, 5 bytes received, connected 8.6 s, online 0
//               Received data is shown on one line: control characters are
//               escaped (\r \n \t \\ \xNN), other bytes (UTF-8 text) are kept,
//               and only the first 64 bytes are shown, followed by
//               "... (N bytes)".
// Usage:        coro_echo_server [port] [threads] [--quiet]
//                 port     port to use, 1-65535 (default 9000)
//                 threads  threads calling run(), 1-64 (default 1)
//                 --quiet  print nothing per connection
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
//               exactly this code without its main(); they pass their own log
//               sink to capture the lines.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-19 (connection log: 2026-11-21)
// ============================================================================

#include "spanio/coroutine.h"
#include "spanio/tcp.h"

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
#include <utility>
#include <vector>

namespace coro_echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9000;

// Threads used when none is given on the command line
constexpr unsigned long kDefaultThreads = 1;

// Smallest port a server may listen on (0 would mean "any port")
constexpr unsigned long kMinPort = 1;

// Largest port number
constexpr unsigned long kMaxPort = 65535;

// Fewest threads that can run the context
constexpr unsigned long kMinThreads = 1;

// Most threads accepted on the command line
constexpr unsigned long kMaxThreads = 64;

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

// Bytes read from a connection at a time
constexpr std::size_t kChunkSize = 4096;

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
    // Threads calling run()
    unsigned long m_threads = kDefaultThreads;
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

// Read the command line: coro_echo_server [port] [threads] [--quiet].
// --quiet may stand anywhere.
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    options parsed;
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
                error = "threads must be a number from 1 to 64";
                return false;
            }
            parsed.m_threads = value;
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

// Why a session ended, in words
// Parameter reason: the read or write error that ended it
// Return value: "client closed", "connection reset", "server closed", or
//               the error's text
inline std::string close_reason_text(const std::error_code& reason)
{
    if (reason == spanio::error::misc_errors::eof)
    {
        return "client closed";
    }
    if (reason == std::errc::connection_reset)
    {
        return "connection reset";
    }
    if (reason == std::errc::operation_canceled)
    {
        return "server closed";
    }
    return reason.message();
}

// State every coroutine of the server shares: the log and the online count.
// It lives in a shared_ptr each coroutine holds, so a session outliving the
// accept loop stays safe.
class log_state
{
public:
    // Parameter sink: where lines go; empty turns logging off
    explicit log_state(log_sink sink)
        : m_sink(std::move(sink))
    {
    }

    // Whether lines are written at all
    // Return value: true when a sink is set
    bool enabled() const noexcept
    {
        return static_cast<bool>(m_sink);
    }

    // Write one event line, prefixed with the time and the event tag; each
    // line reaches the sink whole, even from several run() threads
    // Parameter tag:    event name padded to a common width, e.g. "[recv]   "
    // Parameter peer:   the client's address text
    // Parameter detail: the rest of the line
    void write(const char* tag, const std::string& peer, const std::string& detail)
    {
        const std::string line = timestamp() + " " + tag + " " + peer + "  " + detail;
        std::lock_guard<std::mutex> lock(m_mutex);
        m_sink(line);
    }

    // Connections between their [connect] and [close] lines
    std::atomic<std::size_t> m_online{0};

private:
    // Destination of the lines; empty when logging is off
    log_sink m_sink;
    // Keeps lines whole
    std::mutex m_mutex;
};

// Pointer the coroutines share the log through
using log_state_ptr = std::shared_ptr<log_state>;

// Serve one connection: send back everything it sends, until it closes or
// fails, logging each event. Errors are values here (as_tuple), so the loop
// needs no try/catch. What the log needs about the connection lives in the
// coroutine's locals, so it needs no lock.
// Parameter sock:  the connection; owned by this coroutine
// Parameter state: log and online count
inline spanio::awaitable<void> session(spanio::tcp::socket sock, log_state_ptr state)
{
    const bool logging = state->enabled();
    std::string peer;
    const std::chrono::steady_clock::time_point connected = std::chrono::steady_clock::now();
    std::uint64_t total = 0;
    if (logging)
    {
        std::error_code ec;
        peer = format_endpoint(sock.remote_endpoint(ec));
        const std::size_t online = ++state->m_online;
        state->write("[connect]", peer, "online " + std::to_string(online));
    }

    char data[kChunkSize];
    std::error_code reason;
    for (;;)
    {
        auto [read_ec, bytes] =
            co_await sock.async_read_some(spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
        if (read_ec)
        {
            reason = read_ec;
            break;
        }
        total += bytes;
        if (logging)
        {
            state->write("[recv]   ", peer,
                         std::to_string(bytes) + " bytes: " + escape_bytes(std::string_view(data, bytes)));
        }
        auto [write_ec, written] = co_await sock.async_write(spanio::buffer(data, bytes),
                                                             spanio::as_tuple(spanio::use_awaitable));
        static_cast<void>(written);
        if (write_ec)
        {
            reason = write_ec;
            break;
        }
    }

    // End of file is the normal end; anything else is also logged
    if (reason != spanio::error::misc_errors::eof)
    {
        spanio::detail::log_error("coro_echo_server: session", reason);
    }
    if (logging)
    {
        const std::chrono::duration<double> lasted = std::chrono::steady_clock::now() - connected;
        char duration[kDurationTextSize] = {};
        std::snprintf(duration, sizeof(duration), "%.1f s", lasted.count());
        const std::size_t online = --state->m_online;
        state->write("[close]  ", peer,
                     close_reason_text(reason) + ", " + std::to_string(total) + " bytes received, connected "
                         + duration + ", online " + std::to_string(online));
    }
}

// Accept connections until the acceptor is closed, starting a session
// coroutine for each
// Parameter acceptor: listening acceptor; must outlive the coroutine
// Parameter state:    log and online count, shared with the sessions
inline spanio::awaitable<void> accept_loop(spanio::tcp::acceptor& acceptor, log_state_ptr state)
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
        spanio::co_spawn(context, session(std::move(peer), state), spanio::detached);
    }
}

// Start serving on an open, listening acceptor; the caller runs the context
// Parameter context:  context of the acceptor
// Parameter acceptor: listening acceptor; must outlive the server
// Parameter sink:     where log lines go; default stdout, empty for none
inline void start(spanio::io_context& context, spanio::tcp::acceptor& acceptor, log_sink sink = stdout_sink())
{
    log_state_ptr state = std::make_shared<log_state>(std::move(sink));
    spanio::co_spawn(context, accept_loop(acceptor, std::move(state)), spanio::detached);
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
    coro_echo::options opts;
    std::string error;
    if (!coro_echo::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "coro_echo_server: %s\nusage: coro_echo_server [port] [threads] [--quiet]\n",
                     error.c_str());
        return coro_echo::kExitUsage;
    }
    try
    {
        spanio::io_context context;
        spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::any(), opts.m_port));
        coro_echo::start(context, acceptor, opts.m_quiet ? coro_echo::log_sink() : coro_echo::stdout_sink());
        std::printf("coro_echo_server on port %u, %lu thread(s), backend %s\n", static_cast<unsigned>(opts.m_port),
                    opts.m_threads, context.backend_name());
        std::fflush(stdout);

        auto runner = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> pool;
        for (unsigned long i = 1; i < opts.m_threads; ++i)
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
