// ============================================================================
// File purpose: The SpanIO benchmark program. Eight scenarios measure the
//               network paths (TCP throughput and latency, connection rate,
//               tcp_server throughput, UDP throughput) and the execution
//               engine (post, strand, timers). Clients and servers run in
//               one process over 127.0.0.1, each side on its own io_context
//               and threads. Every run warms up, then measures over a fixed
//               window; each configuration is run several times and reported
//               as median, min and max, flagged unstable when max - min
//               exceeds 10% of the median. Echoed data is checked, so a fast
//               but wrong result shows up as data errors.
// Usage:        bench_spanio [scenario] [options]
//                 scenario     one of the names below; all when omitted
//                 --conns N    connections (TCP), senders (UDP), concurrent
//                              connects (connect rate)
//                 --size N     message / datagram size in bytes
//                 --threads N  threads of the server context (engine
//                              scenarios: threads running the context)
//                 --seconds S  measuring window per run (default 5)
//                 --warmup S   warm-up before each window (default 1)
//                 --runs N     runs per configuration (default 5)
//                 --backend B  only this backend (name as printed)
//                 --csv FILE   also write the results as CSV
//               Without --conns / --size / --threads each scenario runs its
//               default set of configurations (see default_configs()).
//               Build it with -DSPANIO_BUILD_BENCHMARKS=ON, in Release.
//               Compare two CSV files with benchmark/compare.cmake.
//
//               The tests include this file with SPANIO_BENCH_NO_MAIN defined
//               and call the scenarios with short windows.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-12
// ============================================================================

#include "bench_common.h"

#include "spanio/spanio.h"
#include "spanio/detail/platform_proactor.h"
#if defined(SPANIO_PLATFORM_WINDOWS)
#include "spanio/detail/iocp_proactor.h"
#endif

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace bench {

// ---- Defaults and limits --------------------------------------------------------

// Warm-up before each measuring window, in seconds
constexpr double kDefaultWarmup = 1.0;

// Measuring window of a run, in seconds
constexpr double kDefaultSeconds = 5.0;

// Runs per configuration
constexpr int kDefaultRuns = 5;

// Largest message size accepted on the command line: 16 MiB
constexpr std::size_t kMaxSize = 16 * 1024 * 1024;

// Most connections / senders accepted on the command line
constexpr std::size_t kMaxConns = 10000;

// Most threads accepted on the command line
constexpr unsigned kMaxThreads = 256;

// Most runs accepted on the command line
constexpr int kMaxRuns = 1000;

// Longest warm-up or window accepted on the command line: one hour
constexpr double kMaxSeconds = 3600.0;

// Data bytes repeat with this prime period, so shifted data does not match
constexpr std::size_t kPatternPeriod = 251;

// Receive buffer of every TCP reader
constexpr std::size_t kReadBufferSize = 64 * 1024;

// Receive buffer of the UDP server: any datagram fits
constexpr std::size_t kDatagramBufferSize = 64 * 1024;

// Bytes at the front of a benchmark datagram that carry its sequence number
constexpr std::size_t kSequenceBytes = sizeof(std::uint64_t);

// Bytes exchanged per connection in the connection-rate scenario
constexpr std::size_t kConnectPayload = 1;

// Post / strand chains started per thread, so no thread idles
constexpr unsigned kChainsPerThread = 4;

// Timers per batch in the mass-expiry part of the timer scenario
constexpr std::size_t kTimerBatch = 10000;

// Batches in the mass-expiry part of the timer scenario
constexpr int kTimerBatches = 5;

// Delay of the timers that the set/cancel part cancels: never reached
constexpr std::chrono::hours kNeverExpires(1);

// Bytes per megabyte in the reported rates
constexpr double kBytesPerMegabyte = 1000.0 * 1000.0;

// Microseconds per second, for latencies
constexpr double kMicrosPerSecond = 1000000.0;

// Percentage factor
constexpr double kPercentFactor = 100.0;

// Percentiles reported by the latency scenario
constexpr double kP50 = 50.0;
constexpr double kP99 = 99.0;
constexpr double kP999 = 99.9;

// Process exit code: everything ran and no data errors were seen
constexpr int kExitSuccess = 0;

// Process exit code: data errors or a failed scenario
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Option prefix
constexpr char kOptionPrefix[] = "--";

// ---- Options and configurations --------------------------------------------------

// Command-line settings
struct options
{
    // Scenario to run; empty runs all of them
    std::string m_scenario;
    // Connections / senders; 0 uses the scenario's default set
    std::size_t m_conns = 0;
    // Message size; 0 uses the scenario's default set
    std::size_t m_size = 0;
    // Threads; 0 uses the scenario's default set
    unsigned m_threads = 0;
    // Warm-up per run, seconds
    double m_warmup = kDefaultWarmup;
    // Measuring window per run, seconds
    double m_seconds = kDefaultSeconds;
    // Runs per configuration
    int m_runs = kDefaultRuns;
    // Only this backend; empty for all
    std::string m_backend;
    // CSV output file; empty for none
    std::string m_csv;
    // Test hook: called by the echo servers on every chunk before echoing it,
    // to prove that corrupted data is detected
    std::function<void(char*, std::size_t)> m_tamper;
};

// One configuration of a scenario
struct config
{
    // Connections / senders / concurrent connects
    std::size_t m_conns = 1;
    // Message or datagram size
    std::size_t m_size = 1;
    // Threads of the server context, or of the engine scenario's context
    unsigned m_threads = 1;
};

// The configuration as text for reports and CSV keys
// Parameter cfg: the configuration
// Return value: e.g. "conns=16;size=1024;threads=1"
inline std::string config_text(const config& cfg)
{
    return "conns=" + std::to_string(cfg.m_conns) + ";size=" + std::to_string(cfg.m_size)
         + ";threads=" + std::to_string(cfg.m_threads);
}

// One measured quantity of one run
struct metric
{
    // Name, e.g. "throughput"
    std::string m_name;
    // Unit, e.g. "MB/s"
    std::string m_unit;
    // Whether a larger value is better (false for latencies and loss)
    bool m_higher_better = true;
    // The value
    double m_value = 0.0;
};

// Everything one run produced
struct run_result
{
    // Measured quantities; the first one is the scenario's main figure
    std::vector<metric> m_metrics;
    // Data that came back different from what was sent
    std::uint64_t m_data_errors = 0;
    // Latency samples recorded (latency scenario only)
    std::size_t m_samples = 0;
    // Round trips completed in the window, counted apart from the samples
    // (latency scenario only); the two may differ by about one per
    // connection, because the window's edges are not atomic
    std::uint64_t m_round_trips = 0;
};

// Parse a positive integer argument
// Parameter text:  the argument
// Parameter limit: largest accepted value
// Parameter value: receives the number
// Return value: true for a number in [1, limit]
inline bool parse_count(const char* text, unsigned long long limit, unsigned long long& value)
{
    if (text == nullptr || text[0] < '0' || text[0] > '9')
    {
        return false;
    }
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, kDecimalBase);
    if (*end != '\0' || parsed == 0 || parsed > limit)
    {
        return false;
    }
    value = parsed;
    return true;
}

// Parse a non-negative number of seconds
// Parameter text:  the argument
// Parameter value: receives the number
// Return value: true for a finite number >= 0
inline bool parse_seconds(const char* text, double& value)
{
    if (text == nullptr || text[0] == '\0' || text[0] == '-')
    {
        return false;
    }
    char* end = nullptr;
    const double parsed = std::strtod(text, &end);
    if (*end != '\0' || !(parsed >= 0.0) || parsed > kMaxSeconds)
    {
        return false;
    }
    value = parsed;
    return true;
}

// Names of all scenarios, in the order they run
// Return value: the names
inline const std::vector<std::string>& scenario_names()
{
    static const std::vector<std::string> names = {"tcp_throughput", "tcp_latency",     "tcp_connect_rate",
                                                   "tcp_server_throughput", "udp_throughput", "post_rate",
                                                   "strand_rate",    "timer_rate"};
    return names;
}

// Whether a scenario name is known
// Parameter name: the name
// Return value: true when it is one of scenario_names()
inline bool known_scenario(const std::string& name)
{
    for (const std::string& known : scenario_names())
    {
        if (known == name)
        {
            return true;
        }
    }
    return false;
}

// Read the command line (see the file header)
// Parameter argc:  argument count
// Parameter argv:  arguments
// Parameter out:   receives the settings
// Parameter error: receives the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    options parsed;
    for (int i = 1; i < argc; ++i)
    {
        const std::string argument = argv[i];
        if (argument.compare(0, std::strlen(kOptionPrefix), kOptionPrefix) != 0)
        {
            if (!parsed.m_scenario.empty())
            {
                error = "only one scenario may be given";
                return false;
            }
            if (!known_scenario(argument))
            {
                error = "unknown scenario " + argument;
                return false;
            }
            parsed.m_scenario = argument;
            continue;
        }
        if (i + 1 >= argc)
        {
            error = argument + " needs a value";
            return false;
        }
        const char* value = argv[++i];
        unsigned long long number = 0;
        bool valid = true;
        if (argument == "--conns")
        {
            valid = parse_count(value, kMaxConns, number);
            parsed.m_conns = static_cast<std::size_t>(number);
        }
        else if (argument == "--size")
        {
            valid = parse_count(value, kMaxSize, number);
            parsed.m_size = static_cast<std::size_t>(number);
        }
        else if (argument == "--threads")
        {
            valid = parse_count(value, kMaxThreads, number);
            parsed.m_threads = static_cast<unsigned>(number);
        }
        else if (argument == "--runs")
        {
            valid = parse_count(value, static_cast<unsigned long long>(kMaxRuns), number);
            parsed.m_runs = static_cast<int>(number);
        }
        else if (argument == "--seconds")
        {
            valid = parse_seconds(value, parsed.m_seconds) && parsed.m_seconds > 0.0;
        }
        else if (argument == "--warmup")
        {
            valid = parse_seconds(value, parsed.m_warmup);
        }
        else if (argument == "--backend")
        {
            parsed.m_backend = value;
        }
        else if (argument == "--csv")
        {
            parsed.m_csv = value;
        }
        else
        {
            error = "unknown option " + argument;
            return false;
        }
        if (!valid)
        {
            error = "bad value for " + argument + ": " + value;
            return false;
        }
    }
    out = std::move(parsed);
    return true;
}

// ---- Backends and threads ---------------------------------------------------------

// One proactor backend to measure
struct backend
{
    // Name, as printed and written to the CSV
    const char* m_name;
    // Creates an instance
    spanio::detail::io_proactor_ptr (*m_create)(std::error_code&);
};

#if defined(SPANIO_PLATFORM_WINDOWS)
// Create the native IOCP proactor through the common pointer type
// Parameter ec: set to the failure reason, or cleared
// Return value: the proactor, or nullptr
inline spanio::detail::io_proactor_ptr create_native_proactor(std::error_code& ec)
{
    return spanio::detail::iocp_proactor::create(ec);
}
#endif

// Every backend of the platform; the first is the platform default
// Return value: the list
inline std::vector<backend> backends()
{
    std::vector<backend> list;
#if defined(SPANIO_PLATFORM_WINDOWS)
    list.push_back(backend{"iocp-proactor", &create_native_proactor});
    list.push_back(backend{"reactor-proactor-afd", &spanio::detail::make_reactor_proactor});
#else
    list.push_back(backend{"reactor-proactor-epoll", &spanio::detail::make_reactor_proactor});
#endif
    return list;
}

// A context on a backend
// Parameter which: the backend
// Return value: the context; throws std::system_error on failure
inline std::unique_ptr<spanio::io_context> make_context(const backend& which)
{
    std::error_code ec;
    spanio::detail::io_proactor_ptr proactor = which.m_create(ec);
    if (ec || proactor == nullptr)
    {
        throw std::system_error(ec, std::string("bench: cannot create backend ") + which.m_name);
    }
    return std::unique_ptr<spanio::io_context>(new spanio::io_context(std::move(proactor)));
}

// Sleep for a number of seconds
// Parameter seconds: how long
inline void sleep_seconds(double seconds)
{
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
}

// Runs a context on threads, kept busy by a work guard until stop()
class context_runner
{
public:
    // Start the threads
    // Parameter context: context to run
    // Parameter threads: number of threads
    context_runner(spanio::io_context& context, unsigned threads)
        : m_context(context)
        , m_guard(spanio::make_work_guard(context))
    {
        auto run_context = [&context]()
        {
            context.run();
        };
        for (unsigned i = 0; i < threads; ++i)
        {
            m_threads.emplace_back(run_context);
        }
    }

    // Stop and join, if not done yet
    ~context_runner()
    {
        stop();
    }

    context_runner(const context_runner&) = delete;
    context_runner& operator=(const context_runner&) = delete;

    // Stop the context and wait for every thread
    void stop()
    {
        m_guard.reset();
        m_context.stop();
        for (std::thread& thread : m_threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }

private:
    // Context being run
    spanio::io_context& m_context;
    // Keeps run() from returning while the scenario sets up
    spanio::executor_work_guard m_guard;
    // Threads calling run()
    std::vector<std::thread> m_threads;
};

// The data pattern: byte i of a stream is i % kPatternPeriod. The buffer is
// long enough that any window of `length` bytes starting at offset % period
// can be compared without wrapping.
// Parameter length: longest window that will be taken from it
// Return value: the buffer
inline std::vector<char> make_pattern(std::size_t length)
{
    std::vector<char> pattern(length + kPatternPeriod);
    for (std::size_t i = 0; i < pattern.size(); ++i)
    {
        pattern[i] = static_cast<char>(i % kPatternPeriod);
    }
    return pattern;
}

// ---- TCP echo server ----------------------------------------------------------------

// One server-side connection that echoes every chunk it reads. It owns
// itself through the shared_ptr its pending handlers hold.
class echo_session : public std::enable_shared_from_this<echo_session>
{
public:
    // Parameter socket: accepted connection
    // Parameter tamper: optional corruption hook; must outlive the session
    echo_session(spanio::tcp::socket socket, const std::function<void(char*, std::size_t)>* tamper)
        : m_socket(std::move(socket))
        , m_buffer(kReadBufferSize)
        , m_tamper(tamper)
    {
    }

    // Start reading
    void start()
    {
        read();
    }

    // Close the socket; only called after every run() thread has stopped
    void close()
    {
        m_socket.close();
    }

private:
    // Read the next chunk
    void read()
    {
        std::shared_ptr<echo_session> self = shared_from_this();
        auto on_read = [self](std::error_code ec, std::size_t bytes)
        {
            if (!ec)
            {
                self->write(bytes);
            }
        };
        m_socket.async_read_some(spanio::buffer(m_buffer), on_read);
    }

    // Echo a chunk, then read again
    // Parameter bytes: size of the chunk in m_buffer
    void write(std::size_t bytes)
    {
        if (m_tamper != nullptr && *m_tamper)
        {
            (*m_tamper)(m_buffer.data(), bytes);
        }
        std::shared_ptr<echo_session> self = shared_from_this();
        auto on_written = [self](std::error_code ec, std::size_t)
        {
            if (!ec)
            {
                self->read();
            }
        };
        m_socket.async_write(spanio::buffer(m_buffer.data(), bytes), on_written);
    }

    // The connection
    spanio::tcp::socket m_socket;
    // Chunk being echoed
    std::vector<char> m_buffer;
    // Corruption hook, or nullptr
    const std::function<void(char*, std::size_t)>* m_tamper;
};

// Accept loop of the raw echo server. Sessions are remembered weakly so
// they can be closed at the end.
class echo_listener
{
public:
    // Listen on 127.0.0.1 at a free port
    // Parameter context: server context
    // Parameter tamper:  optional corruption hook; must outlive the listener
    echo_listener(spanio::io_context& context, const std::function<void(char*, std::size_t)>* tamper)
        : m_acceptor(context, spanio::tcp::endpoint(spanio::ipv4::loopback(), 0))
        , m_tamper(tamper)
    {
        accept_next();
    }

    // Address clients connect to
    // Return value: the endpoint
    spanio::tcp::endpoint endpoint() const
    {
        std::error_code ec;
        return m_acceptor.local_endpoint(ec);
    }

    // Stop accepting and close every session; only after run() stopped
    void close_all()
    {
        m_acceptor.close();
        std::lock_guard<std::mutex> lock(m_mutex);
        for (const std::weak_ptr<echo_session>& weak : m_sessions)
        {
            if (std::shared_ptr<echo_session> session = weak.lock())
            {
                session->close();
            }
        }
        m_sessions.clear();
    }

private:
    // Accept one connection, start its session, accept the next
    void accept_next()
    {
        auto on_accept = [this](std::error_code ec, spanio::tcp::socket peer)
        {
            if (ec)
            {
                return;
            }
            std::shared_ptr<echo_session> session = std::make_shared<echo_session>(std::move(peer), m_tamper);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_sessions.push_back(session);
            }
            session->start();
            accept_next();
        };
        m_acceptor.async_accept(on_accept);
    }

    // The listening socket
    spanio::tcp::acceptor m_acceptor;
    // Corruption hook, or nullptr
    const std::function<void(char*, std::size_t)>* m_tamper;
    // Guards m_sessions
    std::mutex m_mutex;
    // Every session started
    std::vector<std::weak_ptr<echo_session>> m_sessions;
};

// ---- TCP clients --------------------------------------------------------------------------

// A client that writes the pattern continuously and checks the echo it
// reads back, independently of the writes (pipelined)
class stream_client
{
public:
    // Parameter context: client context
    // Parameter pattern: pattern buffer from make_pattern()
    // Parameter size:    bytes per write
    // Parameter stop:    set when the run ends
    stream_client(spanio::io_context& context, const std::vector<char>& pattern, std::size_t size,
                  const std::atomic<bool>& stop)
        : m_socket(context)
        , m_pattern(pattern)
        , m_size(size)
        , m_stop(stop)
        , m_buffer(kReadBufferSize)
    {
    }

    // Connect, then start writing and reading
    // Parameter target: server address
    void start(const spanio::tcp::endpoint& target)
    {
        auto on_connected = [this](std::error_code ec)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            write_next();
            read_next();
        };
        m_socket.async_connect(target, on_connected);
    }

    // Close; only after run() stopped
    void close()
    {
        m_socket.close();
    }

    // Bytes echoed back and checked so far
    std::atomic<std::uint64_t> m_received{0};
    // Echoed bytes that differed from what was sent
    std::atomic<std::uint64_t> m_errors{0};
    // Connections that could not be made
    std::atomic<std::uint64_t> m_failures{0};

private:
    // Write the next block of the pattern
    void write_next()
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        auto on_written = [this](std::error_code ec, std::size_t bytes)
        {
            if (!ec)
            {
                m_written += bytes;
                write_next();
            }
        };
        const char* data = m_pattern.data() + m_written % kPatternPeriod;
        m_socket.async_write(spanio::buffer(data, m_size), on_written);
    }

    // Read whatever came back and compare it with the pattern
    void read_next()
    {
        auto on_read = [this](std::error_code ec, std::size_t bytes)
        {
            if (ec)
            {
                return;
            }
            if (std::memcmp(m_buffer.data(), m_pattern.data() + m_read % kPatternPeriod, bytes) != 0)
            {
                m_errors.fetch_add(1, std::memory_order_relaxed);
            }
            m_read += bytes;
            m_received.fetch_add(bytes, std::memory_order_relaxed);
            read_next();
        };
        m_socket.async_read_some(spanio::buffer(m_buffer), on_read);
    }

    // The connection
    spanio::tcp::socket m_socket;
    // Pattern buffer, shared by all clients
    const std::vector<char>& m_pattern;
    // Bytes per write
    std::size_t m_size;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
    // Receive buffer
    std::vector<char> m_buffer;
    // Stream offset of the next write (writer chain only)
    std::uint64_t m_written = 0;
    // Stream offset of the next read (reader chain only)
    std::uint64_t m_read = 0;
};

// A client that sends one message, waits for the whole echo, and records
// the round-trip time
class pingpong_client
{
public:
    // Parameter context:   client context
    // Parameter size:      message size
    // Parameter stop:      set when the run ends
    // Parameter recording: set while the measuring window is open
    pingpong_client(spanio::io_context& context, std::size_t size, const std::atomic<bool>& stop,
                    const std::atomic<bool>& recording)
        : m_socket(context)
        , m_message(make_pattern(size))
        , m_reply(size)
        , m_stop(stop)
        , m_recording(recording)
    {
        m_message.resize(size);
    }

    // Connect, then start the first round trip
    // Parameter target: server address
    void start(const spanio::tcp::endpoint& target)
    {
        auto on_connected = [this](std::error_code ec)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            send();
        };
        m_socket.async_connect(target, on_connected);
    }

    // Close; only after run() stopped
    void close()
    {
        m_socket.close();
    }

    // Round-trip times recorded in the window, in microseconds; read only
    // after run() stopped
    std::vector<double> m_samples;
    // Round trips completed, in or out of the window; counted apart from
    // the samples so the two can be checked against each other
    std::atomic<std::uint64_t> m_completed{0};
    // Echoes that differed from the message
    std::atomic<std::uint64_t> m_errors{0};
    // Connections that could not be made
    std::atomic<std::uint64_t> m_failures{0};

private:
    // Start one round trip
    void send()
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        m_sent_at = std::chrono::steady_clock::now();
        auto on_written = [this](std::error_code ec, std::size_t)
        {
            if (!ec)
            {
                receive();
            }
        };
        m_socket.async_write(spanio::buffer(m_message), on_written);
    }

    // Wait for the whole echo
    void receive()
    {
        auto on_read = [this](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                return;
            }
            const std::chrono::duration<double> took = std::chrono::steady_clock::now() - m_sent_at;
            if (m_reply != m_message)
            {
                m_errors.fetch_add(1, std::memory_order_relaxed);
            }
            if (m_recording.load(std::memory_order_relaxed))
            {
                m_samples.push_back(took.count() * kMicrosPerSecond);
            }
            m_completed.fetch_add(1, std::memory_order_relaxed);
            send();
        };
        m_socket.async_read(spanio::buffer(m_reply), on_read);
    }

    // The connection
    spanio::tcp::socket m_socket;
    // Message sent each time
    std::vector<char> m_message;
    // Echo of it
    std::vector<char> m_reply;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
    // Set while samples are kept
    const std::atomic<bool>& m_recording;
    // When the current round trip started
    std::chrono::steady_clock::time_point m_sent_at;
};

// One slot of the connection-rate scenario: connect, exchange one byte,
// close with a reset (no TIME_WAIT that would use up ports), repeat
class connect_slot
{
public:
    // Parameter context: client context
    // Parameter target:  server address
    // Parameter stop:    set when the run ends
    connect_slot(spanio::io_context& context, const spanio::tcp::endpoint& target, const std::atomic<bool>& stop)
        : m_socket(context)
        , m_target(target)
        , m_stop(stop)
    {
    }

    // Start the first cycle
    void start()
    {
        cycle();
    }

    // Close; only after run() stopped
    void close()
    {
        m_socket.close();
    }

    // Completed cycles
    std::atomic<std::uint64_t> m_completed{0};
    // Cycles that failed (connect refused, reset, ...)
    std::atomic<std::uint64_t> m_failures{0};
    // Echoed bytes that differed
    std::atomic<std::uint64_t> m_errors{0};

private:
    // Close the current connection with a reset, then start the next cycle
    void next_cycle()
    {
        if (m_socket.is_open())
        {
            linger abort_on_close{};
            abort_on_close.l_onoff = 1;
            abort_on_close.l_linger = 0;
            ::setsockopt(m_socket.native_handle(), SOL_SOCKET, SO_LINGER,
                         reinterpret_cast<const char*>(&abort_on_close),
                         static_cast<spanio::detail::socklen_type>(sizeof(abort_on_close)));
        }
        m_socket.close();
        cycle();
    }

    // Connect, send a byte, read it back
    void cycle()
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        auto on_read = [this](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                if (m_in != m_out)
                {
                    m_errors.fetch_add(1, std::memory_order_relaxed);
                }
                m_completed.fetch_add(1, std::memory_order_relaxed);
            }
            next_cycle();
        };
        auto on_written = [this, on_read](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
                next_cycle();
                return;
            }
            m_socket.async_read(spanio::buffer(&m_in, kConnectPayload), on_read);
        };
        auto on_connected = [this, on_written](std::error_code ec)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
                next_cycle();
                return;
            }
            ++m_out;
            m_socket.async_write(spanio::buffer(&m_out, kConnectPayload), on_written);
        };
        m_socket.async_connect(m_target, on_connected);
    }

    // The connection of the current cycle
    spanio::tcp::socket m_socket;
    // Server address
    spanio::tcp::endpoint m_target;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
    // Byte sent in this cycle
    char m_out = 0;
    // Byte read back
    char m_in = 0;
};

// Sum of a counter over a set of clients
// Parameter clients: the clients
// Parameter member:  the counter
// Return value: the sum
template <typename Client>
std::uint64_t total(const std::vector<std::unique_ptr<Client>>& clients,
                    std::atomic<std::uint64_t> Client::*member)
{
    std::uint64_t sum = 0;
    for (const std::unique_ptr<Client>& client : clients)
    {
        sum += ((*client).*member).load(std::memory_order_relaxed);
    }
    return sum;
}

// The measuring window: warm up, then count a quantity over the window
// Parameter opts:  durations
// Parameter count: reads the current value of the counted quantity
// Parameter delta: receives the increase over the window
// Return value: the window's length in seconds
inline double measure(const options& opts, const std::function<std::uint64_t()>& count, std::uint64_t& delta)
{
    sleep_seconds(opts.m_warmup);
    const auto begin = std::chrono::steady_clock::now();
    const std::uint64_t first = count();
    sleep_seconds(opts.m_seconds);
    const std::uint64_t last = count();
    const std::chrono::duration<double> window = std::chrono::steady_clock::now() - begin;
    delta = last - first;
    return window.count();
}

// ---- Scenarios ---------------------------------------------------------------------------

// Pipelined echo throughput against the raw echo server (async_read_some /
// async_write) or against tcp_server
// Parameter which:       backend
// Parameter cfg:         conns, size, server threads
// Parameter opts:        durations and hooks
// Parameter use_server:  true to serve with tcp_server
// Return value: throughput in MB/s and messages/s
inline run_result run_tcp_throughput(const backend& which, const config& cfg, const options& opts, bool use_server)
{
    std::unique_ptr<spanio::io_context> server_context = make_context(which);
    std::unique_ptr<spanio::io_context> client_context = make_context(which);
    const std::vector<char> pattern = make_pattern(std::max(cfg.m_size, kReadBufferSize));
    std::atomic<bool> stop{false};
    const std::function<void(char*, std::size_t)>* tamper = opts.m_tamper ? &opts.m_tamper : nullptr;

    std::unique_ptr<echo_listener> listener;
    std::unique_ptr<spanio::tcp_server> server;
    spanio::tcp::endpoint target;
    if (use_server)
    {
        server.reset(new spanio::tcp_server(*server_context, spanio::tcp::endpoint(spanio::ipv4::loopback(), 0)));
        auto on_message = [tamper](const spanio::tcp_connection_ptr& connection, std::string_view data)
        {
            if (tamper == nullptr)
            {
                connection->send(data);
                return;
            }
            std::string copy(data);
            (*tamper)(&copy[0], copy.size());
            connection->send(std::move(copy));
        };
        server->on_message(on_message);
        server->start();
        std::error_code ec;
        target = server->local_endpoint(ec);
    }
    else
    {
        listener.reset(new echo_listener(*server_context, tamper));
        target = listener->endpoint();
    }

    std::vector<std::unique_ptr<stream_client>> clients;
    for (std::size_t i = 0; i < cfg.m_conns; ++i)
    {
        clients.emplace_back(new stream_client(*client_context, pattern, cfg.m_size, stop));
        clients.back()->start(target);
    }
    context_runner server_threads(*server_context, cfg.m_threads);
    context_runner client_threads(*client_context, 1);

    auto received = [&clients]()
    {
        return total(clients, &stream_client::m_received);
    };
    std::uint64_t bytes = 0;
    const double seconds = measure(opts, received, bytes);

    stop = true;
    if (server)
    {
        server->stop();
    }
    client_threads.stop();
    server_threads.stop();
    for (const std::unique_ptr<stream_client>& client : clients)
    {
        client->close();
    }
    if (listener)
    {
        listener->close_all();
    }

    run_result result;
    const double per_second = static_cast<double>(bytes) / seconds;
    result.m_metrics.push_back(metric{"throughput", "MB/s", true, per_second / kBytesPerMegabyte});
    result.m_metrics.push_back(
        metric{"messages", "msg/s", true, per_second / static_cast<double>(cfg.m_size)});
    result.m_metrics.push_back(metric{"failed_connects", "count", false,
                                      static_cast<double>(total(clients, &stream_client::m_failures))});
    result.m_data_errors = total(clients, &stream_client::m_errors);
    return result;
}

// Request / response latency: one message in flight per connection
// Parameter which: backend
// Parameter cfg:   conns, size, server threads
// Parameter opts:  durations and hooks
// Return value: p50 / p99 / p999 in microseconds and round trips per second
inline run_result run_tcp_latency(const backend& which, const config& cfg, const options& opts)
{
    std::unique_ptr<spanio::io_context> server_context = make_context(which);
    std::unique_ptr<spanio::io_context> client_context = make_context(which);
    std::atomic<bool> stop{false};
    std::atomic<bool> recording{false};
    const std::function<void(char*, std::size_t)>* tamper = opts.m_tamper ? &opts.m_tamper : nullptr;
    echo_listener listener(*server_context, tamper);

    std::vector<std::unique_ptr<pingpong_client>> clients;
    for (std::size_t i = 0; i < cfg.m_conns; ++i)
    {
        clients.emplace_back(new pingpong_client(*client_context, cfg.m_size, stop, recording));
        clients.back()->start(listener.endpoint());
    }
    context_runner server_threads(*server_context, cfg.m_threads);
    context_runner client_threads(*client_context, 1);

    sleep_seconds(opts.m_warmup);
    const auto begin = std::chrono::steady_clock::now();
    const std::uint64_t completed_first = total(clients, &pingpong_client::m_completed);
    recording = true;
    sleep_seconds(opts.m_seconds);
    recording = false;
    const std::uint64_t round_trips = total(clients, &pingpong_client::m_completed) - completed_first;
    const std::chrono::duration<double> window = std::chrono::steady_clock::now() - begin;

    stop = true;
    client_threads.stop();
    server_threads.stop();
    std::vector<double> samples;
    for (const std::unique_ptr<pingpong_client>& client : clients)
    {
        client->close();
        samples.insert(samples.end(), client->m_samples.begin(), client->m_samples.end());
    }
    listener.close_all();

    std::sort(samples.begin(), samples.end());
    run_result result;
    result.m_metrics.push_back(metric{"p50", "us", false, percentile(samples, kP50)});
    result.m_metrics.push_back(metric{"p99", "us", false, percentile(samples, kP99)});
    result.m_metrics.push_back(metric{"p999", "us", false, percentile(samples, kP999)});
    result.m_metrics.push_back(
        metric{"round_trips", "rt/s", true, static_cast<double>(round_trips) / window.count()});
    result.m_data_errors = total(clients, &pingpong_client::m_errors);
    result.m_samples = samples.size();
    result.m_round_trips = round_trips;
    return result;
}

// Connections opened, used for one byte and closed, per second
// Parameter which: backend
// Parameter cfg:   conns = concurrent slots, server threads
// Parameter opts:  durations
// Return value: connections per second and failures
inline run_result run_tcp_connect_rate(const backend& which, const config& cfg, const options& opts)
{
    std::unique_ptr<spanio::io_context> server_context = make_context(which);
    std::unique_ptr<spanio::io_context> client_context = make_context(which);
    std::atomic<bool> stop{false};
    const std::function<void(char*, std::size_t)>* tamper = opts.m_tamper ? &opts.m_tamper : nullptr;
    echo_listener listener(*server_context, tamper);

    std::vector<std::unique_ptr<connect_slot>> slots;
    for (std::size_t i = 0; i < cfg.m_conns; ++i)
    {
        slots.emplace_back(new connect_slot(*client_context, listener.endpoint(), stop));
        slots.back()->start();
    }
    context_runner server_threads(*server_context, cfg.m_threads);
    context_runner client_threads(*client_context, 1);

    auto completed = [&slots]()
    {
        return total(slots, &connect_slot::m_completed);
    };
    std::uint64_t connections = 0;
    const double seconds = measure(opts, completed, connections);

    stop = true;
    client_threads.stop();
    server_threads.stop();
    for (const std::unique_ptr<connect_slot>& slot : slots)
    {
        slot->close();
    }
    listener.close_all();

    run_result result;
    result.m_metrics.push_back(
        metric{"connections", "conn/s", true, static_cast<double>(connections) / seconds});
    result.m_metrics.push_back(
        metric{"failures", "count", false, static_cast<double>(total(slots, &connect_slot::m_failures))});
    result.m_data_errors = total(slots, &connect_slot::m_errors);
    return result;
}

// One sender of the UDP scenario: sends numbered datagrams as fast as the
// socket takes them
class datagram_sender
{
public:
    // Parameter context: client context
    // Parameter target:  server address
    // Parameter size:    datagram size, at least kSequenceBytes
    // Parameter pattern: pattern buffer from make_pattern()
    // Parameter stop:    set when the run ends
    datagram_sender(spanio::io_context& context, const spanio::udp::endpoint& target, std::size_t size,
                    const std::vector<char>& pattern, const std::atomic<bool>& stop)
        : m_socket(context)
        , m_target(target)
        , m_datagram(size)
        , m_pattern(pattern)
        , m_stop(stop)
    {
    }

    // Start sending
    void start()
    {
        send_next();
    }

    // Close; only after run() stopped
    void close()
    {
        m_socket.close();
    }

    // Datagrams the system accepted
    std::atomic<std::uint64_t> m_sent{0};
    // Sends the system refused (e.g. out of buffer space)
    std::atomic<std::uint64_t> m_failures{0};

private:
    // Build and send the next datagram
    void send_next()
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        // Sequence number first, then the pattern from where it leads
        const std::uint64_t sequence = m_sequence++;
        std::memcpy(m_datagram.data(), &sequence, kSequenceBytes);
        std::memcpy(m_datagram.data() + kSequenceBytes,
                    m_pattern.data() + sequence % kPatternPeriod + kSequenceBytes,
                    m_datagram.size() - kSequenceBytes);
        auto on_sent = [this](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                m_failures.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                m_sent.fetch_add(1, std::memory_order_relaxed);
            }
            send_next();
        };
        m_socket.async_send_to(spanio::buffer(m_datagram), m_target, on_sent);
    }

    // Sending socket, opened by the first send
    spanio::udp::socket m_socket;
    // Server address
    spanio::udp::endpoint m_target;
    // Datagram being sent
    std::vector<char> m_datagram;
    // Pattern buffer, shared
    const std::vector<char>& m_pattern;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
    // Next sequence number
    std::uint64_t m_sequence = 0;
};

// The UDP receiving side: counts datagrams and checks each one's pattern
class datagram_receiver
{
public:
    // Bind 127.0.0.1 at a free port
    // Parameter context: server context
    // Parameter pattern: pattern buffer from make_pattern()
    datagram_receiver(spanio::io_context& context, const std::vector<char>& pattern)
        : m_socket(context)
        , m_buffer(kDatagramBufferSize)
        , m_pattern(pattern)
    {
        std::error_code ec = m_socket.open(AF_INET);
        if (!ec)
        {
            ec = m_socket.bind(spanio::udp::endpoint(spanio::ipv4::loopback(), 0));
        }
        if (ec)
        {
            throw std::system_error(ec, "bench: cannot bind the UDP receiver");
        }
        receive_next();
    }

    // Address senders send to
    // Return value: the endpoint
    spanio::udp::endpoint endpoint() const
    {
        std::error_code ec;
        return m_socket.local_endpoint(ec);
    }

    // Close; only after run() stopped
    void close()
    {
        m_socket.close();
    }

    // Datagrams received
    std::atomic<std::uint64_t> m_received{0};
    // Bytes received
    std::atomic<std::uint64_t> m_bytes{0};
    // Datagrams whose content was wrong
    std::atomic<std::uint64_t> m_errors{0};

private:
    // Receive and check the next datagram
    void receive_next()
    {
        auto on_received = [this](std::error_code ec, std::size_t bytes, const spanio::udp::endpoint&)
        {
            if (ec == std::errc::operation_canceled)
            {
                return;
            }
            if (!ec && bytes >= kSequenceBytes)
            {
                std::uint64_t sequence = 0;
                std::memcpy(&sequence, m_buffer.data(), kSequenceBytes);
                const char* expected = m_pattern.data() + sequence % kPatternPeriod + kSequenceBytes;
                if (std::memcmp(m_buffer.data() + kSequenceBytes, expected, bytes - kSequenceBytes) != 0)
                {
                    m_errors.fetch_add(1, std::memory_order_relaxed);
                }
                m_received.fetch_add(1, std::memory_order_relaxed);
                m_bytes.fetch_add(bytes, std::memory_order_relaxed);
            }
            receive_next();
        };
        m_socket.async_receive_from(spanio::buffer(m_buffer), on_received);
    }

    // Bound socket
    spanio::udp::socket m_socket;
    // Receive buffer
    std::vector<char> m_buffer;
    // Pattern buffer, shared
    const std::vector<char>& m_pattern;
};

// Datagrams per second received and the share lost on the way
// Parameter which: backend
// Parameter cfg:   conns = senders, size = datagram size, server threads
// Parameter opts:  durations
// Return value: received datagrams/s, MB/s and loss %
inline run_result run_udp_throughput(const backend& which, const config& cfg, const options& opts)
{
    if (cfg.m_size < kSequenceBytes || cfg.m_size > spanio::kMaxUdpPayload)
    {
        throw std::invalid_argument("udp_throughput: size must be 8 to 65507 bytes");
    }
    std::unique_ptr<spanio::io_context> server_context = make_context(which);
    std::unique_ptr<spanio::io_context> client_context = make_context(which);
    const std::vector<char> pattern = make_pattern(cfg.m_size);
    std::atomic<bool> stop{false};
    datagram_receiver receiver(*server_context, pattern);

    std::vector<std::unique_ptr<datagram_sender>> senders;
    for (std::size_t i = 0; i < cfg.m_conns; ++i)
    {
        senders.emplace_back(new datagram_sender(*client_context, receiver.endpoint(), cfg.m_size, pattern, stop));
        senders.back()->start();
    }
    context_runner server_threads(*server_context, cfg.m_threads);
    context_runner client_threads(*client_context, 1);

    sleep_seconds(opts.m_warmup);
    const auto begin = std::chrono::steady_clock::now();
    const std::uint64_t sent_first = total(senders, &datagram_sender::m_sent);
    const std::uint64_t received_first = receiver.m_received.load();
    const std::uint64_t bytes_first = receiver.m_bytes.load();
    sleep_seconds(opts.m_seconds);
    const std::uint64_t sent = total(senders, &datagram_sender::m_sent) - sent_first;
    const std::uint64_t received = receiver.m_received.load() - received_first;
    const std::uint64_t bytes = receiver.m_bytes.load() - bytes_first;
    const std::chrono::duration<double> window = std::chrono::steady_clock::now() - begin;

    stop = true;
    client_threads.stop();
    server_threads.stop();
    for (const std::unique_ptr<datagram_sender>& sender : senders)
    {
        sender->close();
    }
    receiver.close();

    run_result result;
    result.m_metrics.push_back(
        metric{"datagrams", "dgram/s", true, static_cast<double>(received) / window.count()});
    result.m_metrics.push_back(
        metric{"throughput", "MB/s", true, static_cast<double>(bytes) / window.count() / kBytesPerMegabyte});
    const double loss = sent == 0 || received >= sent
                          ? 0.0
                          : static_cast<double>(sent - received) / static_cast<double>(sent) * kPercentFactor;
    result.m_metrics.push_back(metric{"loss", "%", false, loss});
    result.m_data_errors = receiver.m_errors.load();
    return result;
}

// A chain of handlers that re-post themselves, through the context or a
// strand. Each chain is its own heap object, so the counters of different
// chains rarely share a cache line.
template <typename Executor>
class post_chain
{
public:
    // Parameter executor: where to post (io_context executor or strand)
    // Parameter stop:     set when the run ends
    post_chain(const Executor& executor, const std::atomic<bool>& stop)
        : m_executor(executor)
        , m_stop(stop)
    {
    }

    // Post the first handler
    void start()
    {
        auto step = [this]()
        {
            m_count.fetch_add(1, std::memory_order_relaxed);
            if (!m_stop.load(std::memory_order_relaxed))
            {
                start();
            }
        };
        m_executor.post(step);
    }

    // Handlers run so far
    std::atomic<std::uint64_t> m_count{0};

private:
    // Where the chain posts
    Executor m_executor;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
};

// Handlers run per second by chains that keep re-posting themselves
// Parameter which:      backend
// Parameter cfg:        threads running the context
// Parameter opts:       durations
// Parameter use_strand: post through one strand per chain
// Return value: handlers per second
inline run_result run_post_rate(const backend& which, const config& cfg, const options& opts, bool use_strand)
{
    std::unique_ptr<spanio::io_context> context = make_context(which);
    std::atomic<bool> stop{false};
    const unsigned chains = cfg.m_threads * kChainsPerThread;
    using strand_type = spanio::strand<spanio::io_context::executor_type>;
    std::vector<std::unique_ptr<post_chain<spanio::io_context::executor_type>>> plain;
    std::vector<std::unique_ptr<post_chain<strand_type>>> stranded;
    for (unsigned i = 0; i < chains; ++i)
    {
        if (use_strand)
        {
            stranded.emplace_back(new post_chain<strand_type>(spanio::make_strand(*context), stop));
            stranded.back()->start();
        }
        else
        {
            plain.emplace_back(new post_chain<spanio::io_context::executor_type>(context->get_executor(), stop));
            plain.back()->start();
        }
    }
    context_runner threads(*context, cfg.m_threads);

    auto handled = [&]()
    {
        return use_strand ? total(stranded, &post_chain<strand_type>::m_count)
                          : total(plain, &post_chain<spanio::io_context::executor_type>::m_count);
    };
    std::uint64_t count = 0;
    const double seconds = measure(opts, handled, count);
    stop = true;
    threads.stop();

    run_result result;
    result.m_metrics.push_back(metric{"handlers", "handler/s", true, static_cast<double>(count) / seconds});
    return result;
}

// A timer that is armed and cancelled over and over
class set_cancel_chain
{
public:
    // Parameter context: context of the timer
    // Parameter stop:    set when the run ends
    set_cancel_chain(spanio::io_context& context, const std::atomic<bool>& stop)
        : m_timer(context)
        , m_stop(stop)
    {
    }

    // Arm the timer and cancel it at once; the cancelled wait starts the
    // next cycle
    void cycle()
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        auto on_wait = [this](std::error_code)
        {
            m_count.fetch_add(1, std::memory_order_relaxed);
            cycle();
        };
        m_timer.expires_after(kNeverExpires);
        m_timer.async_wait(on_wait);
        m_timer.cancel();
    }

    // Cycles completed
    std::atomic<std::uint64_t> m_count{0};

private:
    // The timer
    spanio::steady_timer m_timer;
    // Set when the run ends
    const std::atomic<bool>& m_stop;
};

// Timer arm/cancel cycles per second, and how fast a large batch of timers
// that expire together is handled
// Parameter which: backend
// Parameter cfg:   unused beyond the thread count (one thread)
// Parameter opts:  durations
// Return value: set/cancel cycles per second and expirations per second
inline run_result run_timer_rate(const backend& which, const config& cfg, const options& opts)
{
    run_result result;
    {
        std::unique_ptr<spanio::io_context> context = make_context(which);
        std::atomic<bool> stop{false};
        set_cancel_chain chain(*context, stop);
        auto first_cycle = [&chain]()
        {
            chain.cycle();
        };
        context->post(first_cycle);
        context_runner threads(*context, cfg.m_threads);
        auto cycles = [&chain]()
        {
            return chain.m_count.load(std::memory_order_relaxed);
        };
        std::uint64_t count = 0;
        const double seconds = measure(opts, cycles, count);
        stop = true;
        threads.stop();
        result.m_metrics.push_back(metric{"set_cancel", "cycle/s", true, static_cast<double>(count) / seconds});
    }

    // Mass expiry: kTimerBatch timers due at the same moment, run to the end
    double expired_seconds = 0.0;
    std::size_t fired = 0;
    for (int batch = 0; batch < kTimerBatches; ++batch)
    {
        std::unique_ptr<spanio::io_context> context = make_context(which);
        std::vector<std::unique_ptr<spanio::steady_timer>> timers;
        const auto due = spanio::steady_timer::clock::now();
        auto on_expired = [&fired](std::error_code ec)
        {
            if (!ec)
            {
                ++fired;
            }
        };
        const auto begin = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < kTimerBatch; ++i)
        {
            timers.emplace_back(new spanio::steady_timer(*context, due));
            timers.back()->async_wait(on_expired);
        }
        context->run();
        const std::chrono::duration<double> took = std::chrono::steady_clock::now() - begin;
        expired_seconds += took.count();
    }
    result.m_metrics.push_back(
        metric{"expirations", "timer/s", true, static_cast<double>(fired) / expired_seconds});
    return result;
}

// ---- Running and reporting -----------------------------------------------------------

// Run one scenario once
// Parameter name:  scenario name
// Parameter which: backend
// Parameter cfg:   configuration
// Parameter opts:  durations and hooks
// Return value: what the run produced; throws for an unknown name
inline run_result run_scenario(const std::string& name, const backend& which, const config& cfg,
                               const options& opts)
{
    if (name == "tcp_throughput")
    {
        return run_tcp_throughput(which, cfg, opts, false);
    }
    if (name == "tcp_server_throughput")
    {
        return run_tcp_throughput(which, cfg, opts, true);
    }
    if (name == "tcp_latency")
    {
        return run_tcp_latency(which, cfg, opts);
    }
    if (name == "tcp_connect_rate")
    {
        return run_tcp_connect_rate(which, cfg, opts);
    }
    if (name == "udp_throughput")
    {
        return run_udp_throughput(which, cfg, opts);
    }
    if (name == "post_rate")
    {
        return run_post_rate(which, cfg, opts, false);
    }
    if (name == "strand_rate")
    {
        return run_post_rate(which, cfg, opts, true);
    }
    if (name == "timer_rate")
    {
        return run_timer_rate(which, cfg, opts);
    }
    throw std::invalid_argument("unknown scenario " + name);
}

// Whether a scenario measures the network (and so every backend), as
// opposed to the execution engine (measured on the default backend only)
// Parameter name: scenario name
// Return value: true for the network scenarios
inline bool network_scenario(const std::string& name)
{
    return name.compare(0, 3, "tcp") == 0 || name.compare(0, 3, "udp") == 0;
}

// Combine fixed lists of values into configurations
// Parameter conns:   connection counts
// Parameter sizes:   sizes
// Parameter threads: thread counts
// Return value: every combination
inline std::vector<config> combine(const std::vector<std::size_t>& conns, const std::vector<std::size_t>& sizes,
                                   const std::vector<unsigned>& threads)
{
    std::vector<config> list;
    for (const std::size_t c : conns)
    {
        for (const std::size_t s : sizes)
        {
            for (const unsigned t : threads)
            {
                config cfg;
                cfg.m_conns = c;
                cfg.m_size = s;
                cfg.m_threads = t;
                list.push_back(cfg);
            }
        }
    }
    return list;
}

// The configurations a scenario runs: its default set, with any value given
// on the command line replacing that dimension
// Parameter name: scenario name
// Parameter opts: command line
// Return value: the configurations
inline std::vector<config> default_configs(const std::string& name, const options& opts)
{
    std::vector<std::size_t> conns = {1};
    std::vector<std::size_t> sizes = {1};
    std::vector<unsigned> threads = {1};
    if (name == "tcp_throughput")
    {
        conns = {1, 16, 64};
        sizes = {64, 1024, 16384};
    }
    else if (name == "tcp_server_throughput")
    {
        conns = {16};
        sizes = {64, 1024, 16384};
    }
    else if (name == "tcp_latency")
    {
        conns = {1, 16};
        sizes = {64};
    }
    else if (name == "tcp_connect_rate")
    {
        conns = {8};
        sizes = {kConnectPayload};
    }
    else if (name == "udp_throughput")
    {
        sizes = {64, 1024};
    }
    else if (name == "post_rate")
    {
        threads = {1, 4};
    }
    else if (name == "strand_rate")
    {
        threads = {4};
    }
    if (opts.m_conns != 0)
    {
        conns = {opts.m_conns};
    }
    if (opts.m_size != 0 && name != "tcp_connect_rate")
    {
        sizes = {opts.m_size};
    }
    if (opts.m_threads != 0)
    {
        threads = {opts.m_threads};
    }
    return combine(conns, sizes, threads);
}

// One reported line: one metric of one configuration over all runs
struct report_row
{
    // Backend name
    std::string m_backend;
    // Scenario name
    std::string m_scenario;
    // Configuration text
    std::string m_params;
    // Metric name
    std::string m_metric;
    // Metric unit
    std::string m_unit;
    // Whether larger is better
    bool m_higher_better = true;
    // Median, range, stability over the runs
    spread m_spread;
};

// The CSV line of a row
// Parameter row: the row
// Return value: the line, without its end
inline std::string row_csv(const report_row& row)
{
    return csv_line({SPANIO_VERSION_STRING, platform_name(), cpu_name(), row.m_backend, row.m_scenario,
                     row.m_params, row.m_metric, row.m_unit, row.m_higher_better ? "higher" : "lower",
                     number_text(row.m_spread.m_median), number_text(row.m_spread.m_min),
                     number_text(row.m_spread.m_max), row.m_spread.m_stable ? "yes" : "no"});
}

// Run every selected scenario, print the results and optionally write the
// CSV
// Parameter opts: command line
// Parameter out:  console output
// Return value: kExitSuccess, or kExitFailure after data errors or a
//               scenario that threw
inline int run_benchmarks(const options& opts, std::ostream& out)
{
    print_environment(out);
    out << "warm-up " << opts.m_warmup << " s, window " << opts.m_seconds << " s, " << opts.m_runs
        << " run(s) per configuration\n";
    std::vector<report_row> rows;
    bool failed = false;
    const std::vector<backend> all_backends = backends();
    for (const std::string& name : scenario_names())
    {
        if (!opts.m_scenario.empty() && opts.m_scenario != name)
        {
            continue;
        }
        for (std::size_t b = 0; b < all_backends.size(); ++b)
        {
            const backend& which = all_backends[b];
            if (!opts.m_backend.empty() && opts.m_backend != which.m_name)
            {
                continue;
            }
            if (opts.m_backend.empty() && b != 0 && !network_scenario(name))
            {
                continue;
            }
            for (const config& cfg : default_configs(name, opts))
            {
                std::vector<run_result> runs;
                try
                {
                    for (int r = 0; r < opts.m_runs; ++r)
                    {
                        runs.push_back(run_scenario(name, which, cfg, opts));
                    }
                }
                catch (const std::exception& e)
                {
                    out << name << " " << which.m_name << " " << config_text(cfg) << ": FAILED: " << e.what()
                        << "\n";
                    failed = true;
                    continue;
                }
                std::uint64_t data_errors = 0;
                for (const run_result& run : runs)
                {
                    data_errors += run.m_data_errors;
                }
                for (std::size_t m = 0; m < runs.front().m_metrics.size(); ++m)
                {
                    std::vector<double> values;
                    for (const run_result& run : runs)
                    {
                        values.push_back(run.m_metrics[m].m_value);
                    }
                    report_row row;
                    row.m_backend = which.m_name;
                    row.m_scenario = name;
                    row.m_params = config_text(cfg);
                    row.m_metric = runs.front().m_metrics[m].m_name;
                    row.m_unit = runs.front().m_metrics[m].m_unit;
                    row.m_higher_better = runs.front().m_metrics[m].m_higher_better;
                    row.m_spread = summarize(values);
                    out << name << "  " << which.m_name << "  " << row.m_params << "  " << row.m_metric << " "
                        << number_text(row.m_spread.m_median) << " " << row.m_unit << "  (min "
                        << number_text(row.m_spread.m_min) << ", max " << number_text(row.m_spread.m_max) << ")"
                        << (row.m_spread.m_stable ? "" : "  UNSTABLE") << "\n";
                    rows.push_back(row);
                }
                if (data_errors != 0)
                {
                    out << name << "  " << which.m_name << "  " << config_text(cfg) << "  DATA ERRORS: " << data_errors
                        << "\n";
                    failed = true;
                }
                out.flush();
            }
        }
    }
    if (!opts.m_csv.empty())
    {
        std::ofstream csv(opts.m_csv, std::ios::binary);
        csv << kCsvHeader << "\n";
        for (const report_row& row : rows)
        {
            csv << row_csv(row) << "\n";
        }
        if (!csv)
        {
            out << "cannot write " << opts.m_csv << "\n";
            failed = true;
        }
    }
    return failed ? kExitFailure : kExitSuccess;
}

}

#if !defined(SPANIO_BENCH_NO_MAIN)

// Parse the command line and run the benchmarks
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure after data
//               errors, kExitSuccess otherwise
int main(int argc, char** argv)
{
    bench::options opts;
    std::string error;
    if (!bench::parse_arguments(argc, argv, opts, error))
    {
        std::cerr << "bench_spanio: " << error << "\n"
                  << "usage: bench_spanio [scenario] [--conns N] [--size N] [--threads N] [--seconds S]\n"
                  << "                    [--warmup S] [--runs N] [--backend NAME] [--csv FILE]\n";
        return bench::kExitUsage;
    }
    try
    {
        return bench::run_benchmarks(opts, std::cout);
    }
    catch (const std::exception& e)
    {
        std::cerr << "bench_spanio: " << e.what() << "\n";
        return bench::kExitFailure;
    }
}

#endif
