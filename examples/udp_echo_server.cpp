// ============================================================================
// File purpose: UDP echo server: every datagram goes back to its sender. It
//               is built on udp_server, so the echo itself is one callback.
//               UDP has no connections, so the server tracks peers itself:
//               the first datagram from a new address (IP:port) counts as a
//               connect, and a peer that sends nothing for the idle time (or
//               is still known when the server stops) counts as a close. It
//               prints one line per event, like echo_server:
//                 14:03:21.512 [connect] 127.0.0.1:54321  online 1
//                 14:03:21.512 [recv]    127.0.0.1:54321  5 bytes: hello
//                 14:04:21.600 [close]   127.0.0.1:54321  idle timeout, 3 datagrams, 15 bytes received, active 60.1 s, online 0
//               Received data is shown on one line: control characters are
//               escaped (\r \n \t \\ \xNN), other bytes (UTF-8 text) are kept,
//               and only the first 64 bytes are shown, followed by
//               "... (N bytes)". At most 10000 peers are tracked; datagrams
//               from further addresses are still echoed and get a [recv]
//               line, but no [connect] or [close].
// Usage:        udp_echo_server [port] [--idle seconds] [--quiet]
//                 port            port to use, 1-65535 (default 9000)
//                 --idle seconds  silence after which a peer counts as gone,
//                                 1-86400 (default 60)
//                 --quiet         print nothing per datagram (for benchmarks)
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
//               exactly this code without its main(); they pass their own
//               log sink to capture the lines.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06 (peer log: 2026-11-23)
// ============================================================================

#include "spanio/timer.h"
#include "spanio/udp_server.h"

#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>

namespace udp_echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9000;

// Smallest port the server may bind (0 would mean "any port")
constexpr unsigned long kMinPort = 1;

// Largest port number
constexpr unsigned long kMaxPort = 65535;

// Idle time used when --idle is not given, in seconds
constexpr unsigned long kDefaultIdleSeconds = 60;

// Shortest idle time accepted on the command line, in seconds
constexpr unsigned long kMinIdleSeconds = 1;

// Longest idle time accepted on the command line: one day, in seconds
constexpr unsigned long kMaxIdleSeconds = 86400;

// Most positional arguments (the port)
constexpr int kMaxPositional = 1;

// Option that turns the per-datagram log off
constexpr char kQuietOption[] = "--quiet";

// Option that sets the idle time; the next argument is the number of seconds
constexpr char kIdleOption[] = "--idle";

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

// Most peers tracked at once, so spoofed source addresses cannot use up memory
constexpr std::size_t kMaxTrackedPeers = 10000;

// The idle check runs this many times per idle period, so a silent peer is
// closed at most a quarter of the idle time late
constexpr long long kSweepsPerIdle = 4;

// Longest pause between two idle checks
constexpr std::chrono::milliseconds kMaxSweepInterval(1000);

// Shortest pause between two idle checks
constexpr std::chrono::milliseconds kMinSweepInterval(1);

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

// Size of the text of a duration, including the zero
constexpr std::size_t kDurationTextSize = 32;

// Settings taken from the command line
struct options
{
    // Port to bind
    std::uint16_t m_port = kDefaultPort;
    // Silence after which a peer counts as gone, in seconds
    unsigned long m_idle_seconds = kDefaultIdleSeconds;
    // Print nothing per datagram
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

// Read the command line: udp_echo_server [port] [--idle seconds] [--quiet].
// The options may stand anywhere.
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
        if (std::strcmp(argument, kIdleOption) == 0)
        {
            const char* seconds = (i + 1 < argc) ? argv[i + 1] : nullptr;
            unsigned long value = 0;
            if (!parse_number(seconds, kMinIdleSeconds, kMaxIdleSeconds, value))
            {
                error = "--idle needs a number of seconds from 1 to 86400";
                return false;
            }
            parsed.m_idle_seconds = value;
            ++i;
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
        if (!parse_number(argument, kMinPort, kMaxPort, value))
        {
            error = "port must be a number from 1 to 65535";
            return false;
        }
        parsed.m_port = static_cast<std::uint16_t>(value);
        ++positional;
    }
    out = parsed;
    return true;
}

// ---- Peer log -----------------------------------------------------------------

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

// An address and port as text: 127.0.0.1:9000, or [::1]:9000 for IPv6
// Parameter endpoint: the endpoint
// Return value: the text
inline std::string format_endpoint(const spanio::udp::endpoint& endpoint)
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

// Pause between two idle checks: a quarter of the idle time, kept within
// [kMinSweepInterval, kMaxSweepInterval]
// Parameter idle: the idle time
// Return value: the pause
inline std::chrono::milliseconds sweep_interval(std::chrono::milliseconds idle)
{
    std::chrono::milliseconds interval = idle / kSweepsPerIdle;
    if (interval > kMaxSweepInterval)
    {
        interval = kMaxSweepInterval;
    }
    if (interval < kMinSweepInterval)
    {
        interval = kMinSweepInterval;
    }
    return interval;
}

// What the server remembers about one peer address
struct peer_info
{
    // When its first datagram arrived
    std::chrono::steady_clock::time_point m_first;
    // When its latest datagram arrived
    std::chrono::steady_clock::time_point m_last;
    // Datagrams received from it
    std::uint64_t m_datagrams = 0;
    // Bytes received from it
    std::uint64_t m_bytes = 0;
};

// Tracks peer addresses and writes the [connect] / [recv] / [close] lines.
// The receive callback and the idle timer run on context threads, stop()
// on any thread, so every member is used under m_mutex. Owned through a
// shared_ptr by udp_echo_server; handlers hold only weak_ptrs, so the timer
// inside is destroyed with the server, never later by a leftover handler.
class peer_tracker : public std::enable_shared_from_this<peer_tracker>
{
public:
    // Parameter context: context for the idle timer; must outlive this object
    // Parameter sink:    where lines go; must not be empty
    // Parameter idle:    silence after which a peer counts as gone
    peer_tracker(spanio::io_context& context, log_sink sink, std::chrono::milliseconds idle)
        : m_sink(std::move(sink))
        , m_idle(idle)
        , m_interval(sweep_interval(idle))
        , m_timer(context)
    {
    }

    peer_tracker(const peer_tracker&) = delete;
    peer_tracker& operator=(const peer_tracker&) = delete;

    // Start the idle checks; later calls and calls after stop() do nothing
    void start()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_started || m_stopped)
        {
            return;
        }
        m_started = true;
        arm_locked();
    }

    // Record a datagram: [connect] for a new address, then [recv]. After
    // stop() nothing is recorded.
    // Parameter from: sender
    // Parameter data: the bytes
    void record(const spanio::udp::endpoint& from, std::string_view data)
    {
        const std::string peer = format_endpoint(from);
        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            return;
        }
        std::map<std::string, peer_info>::iterator found = m_peers.find(peer);
        if (found == m_peers.end() && m_peers.size() < kMaxTrackedPeers)
        {
            peer_info info;
            info.m_first = now;
            found = m_peers.emplace(peer, info).first;
            write_locked("[connect]", peer, "online " + std::to_string(m_peers.size()));
        }
        if (found != m_peers.end())
        {
            found->second.m_last = now;
            ++found->second.m_datagrams;
            found->second.m_bytes += data.size();
        }
        write_locked("[recv]   ", peer, std::to_string(data.size()) + " bytes: " + escape_bytes(data));
    }

    // Close every known peer ("server closed") and end the idle checks.
    // Safe from any thread; later calls do nothing.
    void stop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            return;
        }
        m_stopped = true;
        m_timer.cancel();
        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        while (!m_peers.empty())
        {
            close_locked(m_peers.begin(), "server closed", now);
        }
    }

private:
    // Wait for the next idle check; m_mutex must be held
    void arm_locked()
    {
        std::weak_ptr<peer_tracker> weak = weak_from_this();
        auto on_timer = [weak](std::error_code ec)
        {
            std::shared_ptr<peer_tracker> self = weak.lock();
            if (self != nullptr)
            {
                self->sweep(ec);
            }
        };
        m_timer.expires_after(m_interval);
        m_timer.async_wait(on_timer);
    }

    // Idle check: close peers silent for the idle time, then wait again
    // Parameter ec: the timer's result; set when cancelled
    void sweep(std::error_code ec)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (ec || m_stopped)
        {
            return;
        }
        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        std::map<std::string, peer_info>::iterator it = m_peers.begin();
        while (it != m_peers.end())
        {
            if (now - it->second.m_last >= m_idle)
            {
                it = close_locked(it, "idle timeout", now);
            }
            else
            {
                ++it;
            }
        }
        arm_locked();
    }

    // Forget a peer and write its [close] line; m_mutex must be held
    // Parameter it:     the peer
    // Parameter reason: why it is closed
    // Parameter now:    the current time
    // Return value: the next peer
    std::map<std::string, peer_info>::iterator close_locked(std::map<std::string, peer_info>::iterator it,
                                                            const char* reason,
                                                            std::chrono::steady_clock::time_point now)
    {
        const std::string peer = it->first;
        const peer_info info = it->second;
        std::map<std::string, peer_info>::iterator next = m_peers.erase(it);
        const std::chrono::duration<double> lasted = now - info.m_first;
        char duration[kDurationTextSize] = {};
        std::snprintf(duration, sizeof(duration), "%.1f s", lasted.count());
        const char* unit = (info.m_datagrams == 1) ? " datagram, " : " datagrams, ";
        write_locked("[close]  ", peer,
                     std::string(reason) + ", " + std::to_string(info.m_datagrams) + unit
                         + std::to_string(info.m_bytes) + " bytes received, active " + duration + ", online "
                         + std::to_string(m_peers.size()));
        return next;
    }

    // Write one event line, prefixed with the time and the event tag;
    // m_mutex must be held, which keeps lines whole
    // Parameter tag:    event name padded to a common width, e.g. "[recv]   "
    // Parameter peer:   the peer's address text
    // Parameter detail: the rest of the line
    void write_locked(const char* tag, const std::string& peer, const std::string& detail)
    {
        m_sink(timestamp() + " " + tag + " " + peer + "  " + detail);
    }

    // Guards every member below
    std::mutex m_mutex;
    // Destination of the lines
    log_sink m_sink;
    // Silence after which a peer counts as gone
    std::chrono::milliseconds m_idle;
    // Pause between two idle checks
    std::chrono::milliseconds m_interval;
    // Fires the idle checks
    spanio::steady_timer m_timer;
    // Known peers by address text; the size is the online count
    std::map<std::string, peer_info> m_peers;
    // start() ran
    bool m_started = false;
    // stop() ran
    bool m_stopped = false;
};

// The echo service: a udp_server whose callback sends each datagram back,
// plus a peer_tracker for the log unless the sink is empty
class udp_echo_server
{
public:
    // Bind 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port: port to use
    // Parameter sink: where log lines go; default stdout, empty for none
    // Parameter idle: silence after which a peer counts as gone
    explicit udp_echo_server(std::uint16_t port, log_sink sink = stdout_sink(),
                             std::chrono::milliseconds idle = std::chrono::seconds(kDefaultIdleSeconds))
        : m_server(port)
    {
        install_callback(std::move(sink), idle);
    }

    // Bind an address on a context the caller runs; use start(), not run().
    // Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to bind; port 0 picks one
    // Parameter sink:    where log lines go; default stdout, empty for none
    // Parameter idle:    silence after which a peer counts as gone
    udp_echo_server(spanio::io_context& context, const spanio::udp::endpoint& local, log_sink sink = stdout_sink(),
                    std::chrono::milliseconds idle = std::chrono::seconds(kDefaultIdleSeconds))
        : m_server(context, local)
    {
        install_callback(std::move(sink), idle);
    }

    udp_echo_server(const udp_echo_server&) = delete;
    udp_echo_server& operator=(const udp_echo_server&) = delete;

    // Stop the server; known peers get their [close] line
    ~udp_echo_server()
    {
        stop();
    }

    // Start receiving, without blocking
    void start()
    {
        m_server.start();
        if (m_tracker != nullptr)
        {
            m_tracker->start();
        }
    }

    // Run on the owned context until stop()
    void run()
    {
        if (m_tracker != nullptr)
        {
            m_tracker->start();
        }
        m_server.run();
    }

    // Stop the server and close every known peer; safe from any thread
    void stop()
    {
        m_server.stop();
        if (m_tracker != nullptr)
        {
            m_tracker->stop();
        }
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
    // Set the callback. Without a log it only echoes, so a quiet server does
    // no per-datagram bookkeeping at all.
    // Parameter sink: where log lines go; empty for none
    // Parameter idle: silence after which a peer counts as gone
    void install_callback(log_sink sink, std::chrono::milliseconds idle)
    {
        spanio::udp_server* server = &m_server;
        if (!sink)
        {
            auto echo = [server](const spanio::udp::endpoint& from, std::string_view data)
            {
                server->send_to(from, data);
            };
            m_server.on_datagram(echo);
            return;
        }
        m_tracker = std::make_shared<peer_tracker>(m_server.context(), std::move(sink), idle);
        std::weak_ptr<peer_tracker> weak = m_tracker;
        auto log_and_echo = [server, weak](const spanio::udp::endpoint& from, std::string_view data)
        {
            std::shared_ptr<peer_tracker> tracker = weak.lock();
            if (tracker != nullptr)
            {
                tracker->record(from, data);
            }
            server->send_to(from, data);
        };
        m_server.on_datagram(log_and_echo);
    }

    // Receive loop, send queue and thread. Declared first, so it (and the
    // context it may own) outlives the tracker's timer.
    spanio::udp_server m_server;
    // Peer log; null when the sink is empty
    std::shared_ptr<peer_tracker> m_tracker;
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
    udp_echo::options opts;
    std::string error;
    if (!udp_echo::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "udp_echo_server: %s\nusage: udp_echo_server [port] [--idle seconds] [--quiet]\n",
                     error.c_str());
        return udp_echo::kExitUsage;
    }
    try
    {
        udp_echo::udp_echo_server server(opts.m_port, opts.m_quiet ? udp_echo::log_sink() : udp_echo::stdout_sink(),
                                         std::chrono::seconds(opts.m_idle_seconds));
        std::printf("udp_echo_server on port %u, idle %lu s, backend %s\n", static_cast<unsigned>(opts.m_port),
                    opts.m_idle_seconds, server.backend_name());
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
