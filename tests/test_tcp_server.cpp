// ============================================================================
// File purpose: Unit tests for tcp_server (tcp_server.h) and the framers
//               (framing.h). Clients are plain blocking sockets, as an
//               external program would use. Cases with network traffic run
//               once per proactor backend of the platform on an external
//               context; one case covers the owned context on the default
//               backend. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_tcp_server executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_tcp_server framer`.
//
//               Declaration order in each case: recorders, fixture, then
//               client sockets and connection slots. Clients and connection
//               pointers go first, then the fixture stops the server and
//               joins its threads, and the recorders the callbacks write to
//               are destroyed last.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05
// ============================================================================

#include "spanio/framing.h"
#include "spanio/tcp_server.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using spanio::frame_result;
using spanio::io_context;
using spanio::tcp_connection_ptr;
using spanio::tcp_server;
using spanio::tcp_server_options;
using spanio::detail::native_handle_t;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::connect_client;
using spanio::test::connect_to;
using spanio::test::loopback_address;
using spanio::test::make_context;
using spanio::test::recv_exact;
using spanio::test::send_all;
using spanio::test::socket_guard;

namespace {

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Threads running the server in the single-threaded cases
constexpr int kOneThread = 1;

// Threads running the server in the owned context case
constexpr unsigned kTwoThreads = 2;

// Threads running the server in the concurrency cases
constexpr int kFourThreads = 4;

// Longest wait for something the server is expected to do
constexpr int kWaitLimitMs = 5000;

// Pause between two checks of a condition being waited for
constexpr int kPollIntervalMs = 2;

// Pause that makes the first part of a split message arrive on its own
constexpr int kSplitPauseMs = 50;

// Time a callback stays busy in the overlap case, so overlaps get caught
constexpr int kCallbackHoldUs = 200;

// Short message used by the echo checks
constexpr char kPing[] = "ping";

// Length of kPing without the terminating zero
constexpr std::size_t kPingLength = sizeof(kPing) - 1;

// Connections and pieces in the overlap case
constexpr int kOverlapClients = 8;

// Pieces each client sends in the overlap case
constexpr int kOverlapPieces = 50;

// Threads sending on one connection in the many-senders case
constexpr int kSenderThreads = 4;

// Messages each of those threads sends
constexpr int kMessagesPerSender = 200;

// Digits written for each number in a many-senders line; fixed so every line
// has the same length
constexpr std::size_t kLineNumberDigits = 6;

// Decimal base, for writing and reading those numbers
constexpr int kDecimalBase = 10;

// Length of the "s:" that starts a many-senders line
constexpr std::size_t kSenderPrefixLength = 2;

// Length of "s:nnnnnn\n": sender digit, colon, number, line feed
constexpr std::size_t kSenderLineLength = kSenderPrefixLength + kLineNumberDigits + 1;

// Size of the large reply in the flush case
constexpr std::size_t kLargeReplySize = 4 * 1024 * 1024;

// Size of each queued piece of that reply
constexpr std::size_t kFlushChunkSize = 16 * 1024;

// Payload bytes repeat with this prime period, so misplaced data shows up
constexpr std::size_t kPatternModulo = 251;

// Message limit in the too-large case
constexpr std::size_t kSmallMessageLimit = 16;

// Bytes sent with no line end in the too-large case: over the limit
constexpr std::size_t kUnterminatedSize = 64;

// Length of a complete line in the too-large case: over the limit
constexpr std::size_t kLongLineSize = 32;

// Send queue limit in the overflow case
constexpr std::size_t kSmallSendQueue = 1024 * 1024;

// Chunk queued repeatedly in the overflow case
constexpr std::size_t kOverflowChunk = 256 * 1024;

// Connection limit in the max-connections case
constexpr std::size_t kConnectionLimit = 2;

// Clients in the stop case
constexpr int kStopClients = 3;

// Clients in the framed echo case
constexpr int kEchoClients = 16;

// Messages each client sends in the framed echo case
constexpr int kEchoMessages = 20;

// Base message size in the framed echo case; each message adds to it
constexpr std::size_t kEchoBaseSize = 100;

// Extra bytes per client id, so messages differ between connections
constexpr std::size_t kEchoClientStep = 37;

// Extra bytes per message number
constexpr std::size_t kEchoMessageStep = 53;

// SO_LINGER on/off value that turns lingering on
constexpr int kLingerOn = 1;

// SO_LINGER timeout of zero: closing sends RST instead of FIN
constexpr int kLingerAbortSeconds = 0;

// Payload whose length needs every byte of the header: 0x0102
constexpr std::size_t kTwoByteLength = 0x0102;

// Second-lowest header byte of kTwoByteLength
constexpr char kHeaderByteHigh = 0x01;

// Lowest header byte of kTwoByteLength
constexpr char kHeaderByteLow = 0x02;

// Index of the second-lowest header byte
constexpr std::size_t kHeaderIndexHigh = 2;

// Index of the lowest header byte
constexpr std::size_t kHeaderIndexLow = 3;

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Wait until a condition holds
// Parameter condition: function object returning bool
// Return value: true if it held within kWaitLimitMs
template <typename Condition>
bool wait_until(Condition condition)
{
    const test_clock::time_point start = test_clock::now();
    while (!condition())
    {
        if (elapsed_ms(start) > kWaitLimitMs)
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
    }
    return true;
}

// Wait until a server serves a given number of connections
// Parameter server:   server to watch
// Parameter expected: connection count to wait for
// Return value: true if the count was reached in time
bool wait_for_connections(const tcp_server& server, std::size_t expected)
{
    auto reached = [&server, expected]()
    {
        return server.connection_count() == expected;
    };
    return wait_until(reached);
}

// Payload of a given size following the prime-period pattern
// Parameter size: number of bytes
// Return value: the payload
std::string make_payload(std::size_t size)
{
    std::string payload(size, '\0');
    for (std::size_t i = 0; i < size; ++i)
    {
        payload[i] = static_cast<char>(i % kPatternModulo);
    }
    return payload;
}

// Whether the peer has closed a connection: the next read reports it
// Parameter handle: blocking client socket with a receive timeout
// Return value: true if a read ended the connection before the timeout
bool sees_close(native_handle_t handle)
{
    const test_clock::time_point start = test_clock::now();
    char byte = 0;
    const int result = ::recv(handle, &byte, 1, spanio::test::kNoSocketFlags);
    return result <= spanio::test::kSocketClosedResult && elapsed_ms(start) < kWaitLimitMs;
}

// Read everything until the peer closes
// Parameter handle: blocking client socket
// Parameter out:    receives the bytes
// Return value: true if the connection ended in an orderly close
bool recv_until_closed(native_handle_t handle, std::string& out)
{
    std::vector<char> chunk(kOverflowChunk);
    while (true)
    {
        const int result = ::recv(handle, chunk.data(), static_cast<int>(chunk.size()),
                                  spanio::test::kNoSocketFlags);
        if (result == 0)
        {
            return true;
        }
        if (result < 0)
        {
            return false;
        }
        out.append(chunk.data(), static_cast<std::size_t>(result));
    }
}

// Send kPing and check it comes back; for use in the case's own thread
// Parameter handle: connected client socket of an echoing server
// Return value: true if the echo arrived intact
bool ping(native_handle_t handle)
{
    char reply[kPingLength] = {};
    return send_all(handle, kPing, kPingLength) && recv_exact(handle, reply, kPingLength)
        && std::memcmp(reply, kPing, kPingLength) == 0;
}

// Queue a payload as many small sends. Only the first is in flight, and the
// rest wait in the send queue; one big send would not do, because Windows
// takes all of it into the kernel at once and the queue would stay empty.
// Parameter connection: the connection
// Parameter payload:    bytes to send
// Return value: true if every piece was accepted
bool send_in_chunks(const tcp_connection_ptr& connection, std::string_view payload)
{
    bool accepted = true;
    for (std::size_t offset = 0; offset < payload.size(); offset += kFlushChunkSize)
    {
        accepted = connection->send(payload.substr(offset, kFlushChunkSize)) && accepted;
    }
    return accepted;
}

// Callback that echoes what it receives
// Parameter connection: the connection
// Parameter data:       received data
void echo_message(const tcp_connection_ptr& connection, std::string_view data)
{
    connection->send(data);
}

// Thread-safe record of what the on_close callback saw
class close_record
{
public:
    // Store one on_close call
    // Parameter reason: reason passed to on_close
    void add(const std::error_code& reason)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_reasons.push_back(reason);
    }

    // Number of on_close calls so far
    // Return value: the count
    std::size_t count() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_reasons.size();
    }

    // Reason of one on_close call
    // Parameter index: call number, from 0
    // Return value: the reason
    std::error_code reason(std::size_t index) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_reasons.at(index);
    }

    // Wait for a number of on_close calls
    // Parameter expected: count to wait for
    // Return value: true if it was reached in time
    bool wait_for(std::size_t expected) const
    {
        auto reached = [this, expected]()
        {
            return count() == expected;
        };
        return wait_until(reached);
    }

private:
    // Guards m_reasons
    mutable std::mutex m_mutex;
    // Reasons in arrival order
    std::vector<std::error_code> m_reasons;
};

// Holds the connection most recently seen by on_connect
class connection_slot
{
public:
    // Store a connection
    // Parameter connection: the connection
    void set(const tcp_connection_ptr& connection)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_connection = connection;
    }

    // Wait for a connection to arrive
    // Return value: the connection, or nullptr on timeout
    tcp_connection_ptr wait() const
    {
        auto arrived = [this]()
        {
            return get() != nullptr;
        };
        wait_until(arrived);
        return get();
    }

    // The stored connection
    // Return value: the connection, or nullptr
    tcp_connection_ptr get() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_connection;
    }

private:
    // Guards m_connection
    mutable std::mutex m_mutex;
    // Latest connection
    tcp_connection_ptr m_connection;
};

// A tcp_server on an external context of a given backend, with its run()
// threads. The destructor stops the server and joins them.
class server_fixture
{
public:
    // Parameter backend: proactor backend to use
    // Parameter options: server limits
    explicit server_fixture(const backend_factory& backend,
                            const tcp_server_options& options = tcp_server_options())
        : m_context(make_context(backend))
        , m_server(new tcp_server(*m_context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort),
                                  options))
    {
    }

    // Stop and join
    ~server_fixture()
    {
        stop_and_join();
    }

    server_fixture(const server_fixture&) = delete;
    server_fixture& operator=(const server_fixture&) = delete;

    // The server, for setting callbacks
    // Return value: the server
    tcp_server& server()
    {
        return *m_server;
    }

    // Port the server listens on, or fail the case
    // Return value: the port
    std::uint16_t port() const
    {
        std::error_code ec;
        const spanio::tcp::endpoint local = m_server->local_endpoint(ec);
        SPANIO_REQUIRE(!ec);
        return local.port();
    }

    // Start the server and the run() threads
    // Parameter threads: number of threads
    void start(int threads)
    {
        m_server->start();
        io_context& context = *m_context;
        auto run_context = [&context]()
        {
            context.run();
        };
        for (int i = 0; i < threads; ++i)
        {
            m_threads.emplace_back(run_context);
        }
    }

    // Stop the server and wait for every run() to return
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
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The server
    std::unique_ptr<tcp_server> m_server;
    // Threads calling run()
    std::vector<std::thread> m_threads;
};

// Per-connection state of the overlap case, kept in tcp_connection::context
struct overlap_state
{
    // Non-zero while a callback of this connection is running
    std::atomic<int> m_busy{0};
    // on_close has run
    std::atomic<bool> m_closed{false};
};

// Pointer stored in tcp_connection::context by the overlap case
using overlap_state_ptr = std::shared_ptr<overlap_state>;

}

// Case 1: line_framer finds complete lines, waits for partial ones, removes
// "\r", and treats an empty line as a message
SPANIO_TEST(line_framer_cases)
{
    const spanio::framer split = spanio::line_framer();

    frame_result frame = split("hello\nrest");
    SPANIO_CHECK(frame.m_payload == "hello");
    SPANIO_CHECK(frame.m_consumed == std::strlen("hello\n"));
    SPANIO_CHECK(!frame.m_error);

    frame = split("no end yet");
    SPANIO_CHECK(frame.m_consumed == 0);
    SPANIO_CHECK(!frame.m_error);

    frame = split("");
    SPANIO_CHECK(frame.m_consumed == 0);

    frame = split("dos\r\n");
    SPANIO_CHECK(frame.m_payload == "dos");
    SPANIO_CHECK(frame.m_consumed == std::strlen("dos\r\n"));

    frame = split("\n");
    SPANIO_CHECK(frame.m_payload.empty());
    SPANIO_CHECK(frame.m_consumed == 1);

    frame = split("\r\n");
    SPANIO_CHECK(frame.m_payload.empty());
    SPANIO_CHECK(frame.m_consumed == std::strlen("\r\n"));
}

// Case 2: length_prefix_framer waits for the header and the payload, and
// matches encode_length_prefixed
SPANIO_TEST(length_prefix_framer_cases)
{
    const spanio::framer split = spanio::length_prefix_framer();

    const std::string message = spanio::encode_length_prefixed("hello");
    SPANIO_CHECK(message.size() == spanio::kLengthHeaderBytes + std::strlen("hello"));
    frame_result frame = split(message);
    SPANIO_CHECK(frame.m_payload == "hello");
    SPANIO_CHECK(frame.m_consumed == message.size());

    // Every shorter prefix means "need more data"
    for (std::size_t size = 0; size < message.size(); ++size)
    {
        frame = split(std::string_view(message).substr(0, size));
        SPANIO_CHECK(frame.m_consumed == 0);
        SPANIO_CHECK(!frame.m_error);
    }

    const std::string empty = spanio::encode_length_prefixed("");
    SPANIO_CHECK(empty.size() == spanio::kLengthHeaderBytes);
    frame = split(empty);
    SPANIO_CHECK(frame.m_payload.empty());
    SPANIO_CHECK(frame.m_consumed == spanio::kLengthHeaderBytes);

    // The header is big-endian
    const std::string sized = spanio::encode_length_prefixed(std::string(kTwoByteLength, 'x'));
    SPANIO_CHECK(sized[0] == 0);
    SPANIO_CHECK(sized[1] == 0);
    SPANIO_CHECK(sized[kHeaderIndexHigh] == kHeaderByteHigh);
    SPANIO_CHECK(sized[kHeaderIndexLow] == kHeaderByteLow);
    SPANIO_CHECK(split(sized).m_payload.size() == kTwoByteLength);

    // Two messages in a row: only the first is taken
    const std::string both = message + empty;
    frame = split(both);
    SPANIO_CHECK(frame.m_payload == "hello");
    SPANIO_CHECK(frame.m_consumed == message.size());
}

// Case 3: on its own context, run(2) serves clients, stop() from another
// thread ends it, and the server cannot run again
SPANIO_TEST(owned_context_run_and_stop)
{
    tcp_server server(spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    server.on_message(echo_message);
    std::error_code ec;
    const std::uint16_t port = server.local_endpoint(ec).port();
    SPANIO_REQUIRE(!ec);

    auto run_server = [&server]()
    {
        server.run(kTwoThreads);
    };
    std::thread runner(run_server);
    {
        socket_guard client;
        SPANIO_CHECK(connect_client(port, client));
        SPANIO_CHECK(ping(client.get()));
    }
    const test_clock::time_point start = test_clock::now();
    server.stop();
    runner.join();
    SPANIO_CHECK(elapsed_ms(start) < kWaitLimitMs);

    bool run_thrown = false;
    try
    {
        server.run();
    }
    catch (const std::logic_error&)
    {
        run_thrown = true;
    }
    SPANIO_CHECK(run_thrown);
}

// Case 4: on an external context start() works and run() is refused;
// starting twice and changing callbacks after start are refused
SPANIO_TEST(external_context_start)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        fixture.server().on_message(echo_message);

        bool run_thrown = false;
        try
        {
            fixture.server().run();
        }
        catch (const std::logic_error&)
        {
            run_thrown = true;
        }
        SPANIO_CHECK(run_thrown);

        fixture.start(kOneThread);

        bool second_start_thrown = false;
        try
        {
            fixture.server().start();
        }
        catch (const std::logic_error&)
        {
            second_start_thrown = true;
        }
        SPANIO_CHECK(second_start_thrown);

        bool late_callback_thrown = false;
        try
        {
            fixture.server().on_message(echo_message);
        }
        catch (const std::logic_error&)
        {
            late_callback_thrown = true;
        }
        SPANIO_CHECK(late_callback_thrown);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        SPANIO_CHECK(ping(client.get()));
    }
}

// Case 5: callbacks arrive as connect, message, close; on_close runs once,
// and a peer close is reported as eof
SPANIO_TEST(callback_order)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::mutex events_mutex;
        std::vector<std::string> events;
        close_record closes;
        server_fixture fixture(backend);

        auto add_event = [&](const std::string& event)
        {
            std::lock_guard<std::mutex> lock(events_mutex);
            events.push_back(event);
        };
        auto on_connect = [&](const tcp_connection_ptr&)
        {
            add_event("connect");
        };
        auto on_message = [&](const tcp_connection_ptr& connection, std::string_view data)
        {
            add_event("message:" + std::string(data));
            connection->send(data);
        };
        auto on_close = [&](const tcp_connection_ptr&, std::error_code reason)
        {
            add_event("close");
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        SPANIO_CHECK(ping(client.get()));
        client.reset();

        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(wait_for_connections(fixture.server(), 0));
        SPANIO_CHECK(closes.reason(0) == spanio::error::eof);

        std::lock_guard<std::mutex> lock(events_mutex);
        const std::string expected_message = "message:" + std::string(kPing);
        SPANIO_REQUIRE(events.size() == 3);
        SPANIO_CHECK(events[0] == "connect");
        SPANIO_CHECK(events[1] == expected_message);
        SPANIO_CHECK(events[2] == "close");
    }
}

// Case 6: with four run() threads and several connections, the callbacks of
// one connection never overlap, and on_close comes after all messages
SPANIO_TEST(callbacks_never_overlap_per_connection)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> overlaps(0);
        std::atomic<int> after_close(0);
        std::atomic<std::size_t> bytes_seen(0);
        std::atomic<int> closes(0);
        server_fixture fixture(backend);

        // Marks the connection busy for a moment; a second callback entering
        // meanwhile finds the mark
        auto hold_busy = [&](const overlap_state_ptr& state)
        {
            if (state->m_busy.exchange(1) != 0)
            {
                ++overlaps;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(kCallbackHoldUs));
            state->m_busy.store(0);
        };
        auto state_of = [](const tcp_connection_ptr& connection)
        {
            return std::any_cast<overlap_state_ptr>(connection->context());
        };
        auto on_connect = [&](const tcp_connection_ptr& connection)
        {
            overlap_state_ptr state = std::make_shared<overlap_state>();
            connection->context() = state;
            hold_busy(state);
        };
        auto on_message = [&](const tcp_connection_ptr& connection, std::string_view data)
        {
            const overlap_state_ptr state = state_of(connection);
            if (state->m_closed.load())
            {
                ++after_close;
            }
            bytes_seen += data.size();
            hold_busy(state);
        };
        auto on_close = [&](const tcp_connection_ptr& connection, std::error_code)
        {
            const overlap_state_ptr state = state_of(connection);
            hold_busy(state);
            state->m_closed.store(true);
            ++closes;
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kFourThreads);
        const std::uint16_t port = fixture.port();

        std::atomic<int> client_failures(0);
        auto run_client = [&]()
        {
            socket_guard client;
            if (!connect_client(port, client))
            {
                ++client_failures;
                return;
            }
            for (int i = 0; i < kOverlapPieces; ++i)
            {
                if (!send_all(client.get(), kPing, kPingLength))
                {
                    ++client_failures;
                    return;
                }
            }
        };
        std::vector<std::thread> clients;
        for (int i = 0; i < kOverlapClients; ++i)
        {
            clients.emplace_back(run_client);
        }
        for (std::thread& client : clients)
        {
            client.join();
        }

        auto all_closed = [&closes]()
        {
            return closes.load() == kOverlapClients;
        };
        SPANIO_CHECK(wait_until(all_closed));
        SPANIO_CHECK(client_failures.load() == 0);
        SPANIO_CHECK(overlaps.load() == 0);
        SPANIO_CHECK(after_close.load() == 0);
        SPANIO_CHECK(bytes_seen.load()
                     == static_cast<std::size_t>(kOverlapClients * kOverlapPieces) * kPingLength);
    }
}

// Case 7: several threads send on one connection at once; every line
// arrives whole, and each thread's lines stay in order
SPANIO_TEST(send_from_many_threads)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        connection_slot slot;
        auto on_connect = [&slot](const tcp_connection_ptr& connection)
        {
            slot.set(connection);
        };
        fixture.server().on_connect(on_connect);
        fixture.start(kFourThreads);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        const tcp_connection_ptr connection = slot.wait();
        SPANIO_REQUIRE(connection != nullptr);

        auto send_lines = [&connection](int sender)
        {
            for (int i = 0; i < kMessagesPerSender; ++i)
            {
                std::string number = std::to_string(i);
                number.insert(0, kLineNumberDigits - number.size(), '0');
                connection->send(std::to_string(sender) + ":" + number + "\n");
            }
        };
        std::vector<std::thread> senders;
        for (int sender = 0; sender < kSenderThreads; ++sender)
        {
            senders.emplace_back(send_lines, sender);
        }

        const std::size_t total = static_cast<std::size_t>(kSenderThreads * kMessagesPerSender);
        std::string received(total * kSenderLineLength, '\0');
        const bool complete = recv_exact(client.get(), &received[0], received.size());
        for (std::thread& sender : senders)
        {
            sender.join();
        }
        SPANIO_REQUIRE(complete);

        std::vector<int> next(static_cast<std::size_t>(kSenderThreads), 0);
        int bad_lines = 0;
        for (std::size_t line = 0; line < total; ++line)
        {
            const std::string text = received.substr(line * kSenderLineLength, kSenderLineLength);
            const int sender = text[0] - '0';
            const int number = static_cast<int>(std::strtol(text.c_str() + kSenderPrefixLength, nullptr, kDecimalBase));
            if (text[1] != ':' || text.back() != '\n' || sender < 0 || sender >= kSenderThreads
                || number != next[static_cast<std::size_t>(sender)])
            {
                ++bad_lines;
                continue;
            }
            ++next[static_cast<std::size_t>(sender)];
        }
        SPANIO_CHECK(bad_lines == 0);
    }
}

// Case 8: queued data is delivered before a close: both after close() and
// when the peer only shut down its sending side
SPANIO_TEST(close_flushes_pending_sends)
{
    const std::string payload = make_payload(kLargeReplySize);
    for (const backend_factory& backend : backends())
    {
        announce(backend);

        // close() right after queuing a large reply
        {
            close_record closes;
            server_fixture fixture(backend);
            std::atomic<bool> send_accepted(false);
            std::atomic<bool> send_after_close(true);
            auto on_connect = [&](const tcp_connection_ptr& connection)
            {
                send_accepted = send_in_chunks(connection, payload);
                connection->close();
                send_after_close = connection->send(payload);
            };
            auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
            {
                closes.add(reason);
            };
            fixture.server().on_connect(on_connect);
            fixture.server().on_close(on_close);
            fixture.start(kOneThread);

            socket_guard client;
            SPANIO_REQUIRE(connect_client(fixture.port(), client));
            std::string received;
            SPANIO_CHECK(recv_until_closed(client.get(), received));
            SPANIO_CHECK(received == payload);
            SPANIO_CHECK(send_accepted.load());
            SPANIO_CHECK(!send_after_close.load());
            SPANIO_REQUIRE(closes.wait_for(1));
            SPANIO_CHECK(!closes.reason(0));
        }

        // The client asks, shuts down its sending side, then reads the reply
        {
            close_record closes;
            server_fixture fixture(backend);
            auto on_message = [&payload](const tcp_connection_ptr& connection, std::string_view)
            {
                send_in_chunks(connection, payload);
            };
            auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
            {
                closes.add(reason);
            };
            fixture.server().on_message(on_message);
            fixture.server().on_close(on_close);
            fixture.start(kOneThread);

            socket_guard client;
            SPANIO_REQUIRE(connect_client(fixture.port(), client));
            SPANIO_REQUIRE(send_all(client.get(), kPing, kPingLength));
            SPANIO_REQUIRE(::shutdown(client.get(), static_cast<int>(spanio::tcp::shutdown_send)) == 0);
            std::string received;
            SPANIO_CHECK(recv_until_closed(client.get(), received));
            SPANIO_CHECK(received == payload);
            SPANIO_REQUIRE(closes.wait_for(1));
            SPANIO_CHECK(closes.reason(0) == spanio::error::eof);
        }
    }
}

// Case 9: abort() closes at once and later sends are refused
SPANIO_TEST(abort_closes_immediately)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        server_fixture fixture(backend);
        connection_slot slot;
        auto on_connect = [&slot](const tcp_connection_ptr& connection)
        {
            slot.set(connection);
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        const tcp_connection_ptr connection = slot.wait();
        SPANIO_REQUIRE(connection != nullptr);
        SPANIO_CHECK(connection->is_open());

        connection->abort();
        SPANIO_CHECK(!connection->is_open());
        SPANIO_CHECK(!connection->send(kPing));
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(!closes.reason(0));
        SPANIO_CHECK(sees_close(client.get()));
    }
}

// Case 10: with line_framer, split and merged input still gives one
// on_message per line
SPANIO_TEST(framed_messages_over_tcp)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::mutex lines_mutex;
        std::vector<std::string> lines;
        server_fixture fixture(backend);
        auto on_message = [&](const tcp_connection_ptr&, std::string_view data)
        {
            std::lock_guard<std::mutex> lock(lines_mutex);
            lines.emplace_back(data);
        };
        fixture.server().set_framer(spanio::line_framer());
        fixture.server().on_message(on_message);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        const std::string first_part = "he";
        const std::string second_part = "llo\r\nsecond\nthird\n";
        SPANIO_REQUIRE(send_all(client.get(), first_part.data(), first_part.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(kSplitPauseMs));
        SPANIO_REQUIRE(send_all(client.get(), second_part.data(), second_part.size()));

        auto three_lines = [&]()
        {
            std::lock_guard<std::mutex> lock(lines_mutex);
            return lines.size() == 3;
        };
        SPANIO_REQUIRE(wait_until(three_lines));

        // A later read must continue after the consumed input, not repeat it
        const std::string last_part = "fourth\n";
        SPANIO_REQUIRE(send_all(client.get(), last_part.data(), last_part.size()));
        auto four_lines = [&]()
        {
            std::lock_guard<std::mutex> lock(lines_mutex);
            return lines.size() >= 4;
        };
        SPANIO_REQUIRE(wait_until(four_lines));

        std::lock_guard<std::mutex> lock(lines_mutex);
        SPANIO_REQUIRE(lines.size() == 4);
        SPANIO_CHECK(lines[0] == "hello");
        SPANIO_CHECK(lines[1] == "second");
        SPANIO_CHECK(lines[2] == "third");
        SPANIO_CHECK(lines[3] == "fourth");
    }
}

// Case 11: input over the message limit closes that connection with
// message_too_large, both while buffering and for a complete message;
// other connections are not affected
SPANIO_TEST(message_too_large_closes)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_server_options options;
        options.m_max_message_size = kSmallMessageLimit;
        server_fixture fixture(backend, options);
        auto on_message = [](const tcp_connection_ptr& connection, std::string_view data)
        {
            connection->send(std::string(data) + "\n");
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().set_framer(spanio::line_framer());
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard bystander;
        SPANIO_REQUIRE(connect_client(fixture.port(), bystander));

        // No line end: the buffered input passes the limit
        socket_guard unterminated;
        SPANIO_REQUIRE(connect_client(fixture.port(), unterminated));
        const std::string long_input(kUnterminatedSize, 'x');
        SPANIO_REQUIRE(send_all(unterminated.get(), long_input.data(), long_input.size()));
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::message_too_large);
        SPANIO_CHECK(sees_close(unterminated.get()));

        // A complete line that is itself too long
        socket_guard long_line;
        SPANIO_REQUIRE(connect_client(fixture.port(), long_line));
        const std::string line = std::string(kLongLineSize, 'y') + "\n";
        SPANIO_REQUIRE(send_all(long_line.get(), line.data(), line.size()));
        SPANIO_REQUIRE(closes.wait_for(2));
        SPANIO_CHECK(closes.reason(1) == spanio::error::message_too_large);

        const std::string request = std::string(kPing) + "\n";
        char reply[kPingLength + 1] = {};
        SPANIO_CHECK(send_all(bystander.get(), request.data(), request.size()));
        SPANIO_CHECK(recv_exact(bystander.get(), reply, request.size()));
        SPANIO_CHECK(std::memcmp(reply, request.data(), request.size()) == 0);
    }
}

// Case 12: a client that stops reading overflows its send queue: send()
// returns false and the connection closes with send_queue_full; other
// connections keep working
SPANIO_TEST(send_queue_overflow_closes)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_server_options options;
        options.m_max_send_queue_bytes = kSmallSendQueue;
        server_fixture fixture(backend, options);
        connection_slot slot;
        std::atomic<bool> first_taken(false);
        auto on_connect = [&](const tcp_connection_ptr& connection)
        {
            // The first client is the one that stops reading
            if (!first_taken.exchange(true))
            {
                slot.set(connection);
            }
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(echo_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard silent;
        SPANIO_REQUIRE(connect_client(fixture.port(), silent));
        const tcp_connection_ptr connection = slot.wait();
        SPANIO_REQUIRE(connection != nullptr);
        socket_guard bystander;
        SPANIO_REQUIRE(connect_client(fixture.port(), bystander));

        const std::string chunk(kOverflowChunk, 'z');
        const test_clock::time_point start = test_clock::now();
        bool refused = false;
        while (!refused && elapsed_ms(start) < kWaitLimitMs)
        {
            refused = !connection->send(chunk);
        }
        SPANIO_REQUIRE(refused);
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::send_queue_full);
        SPANIO_CHECK(ping(bystander.get()));
    }
}

// Case 13: connections over the limit are closed at once without on_connect;
// when one leaves, a new client gets in
SPANIO_TEST(max_connections_rejects_extra)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<std::size_t> connects(0);
        tcp_server_options options;
        options.m_max_connections = kConnectionLimit;
        server_fixture fixture(backend, options);
        auto on_connect = [&connects](const tcp_connection_ptr&)
        {
            ++connects;
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(echo_message);
        fixture.start(kOneThread);

        socket_guard first;
        socket_guard second;
        SPANIO_REQUIRE(connect_client(fixture.port(), first));
        SPANIO_REQUIRE(connect_client(fixture.port(), second));
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), kConnectionLimit));

        socket_guard extra;
        SPANIO_REQUIRE(connect_client(fixture.port(), extra));
        SPANIO_CHECK(sees_close(extra.get()));
        SPANIO_CHECK(connects.load() == kConnectionLimit);
        SPANIO_CHECK(fixture.server().connection_count() == kConnectionLimit);

        first.reset();
        SPANIO_CHECK(wait_for_connections(fixture.server(), kConnectionLimit - 1));
        socket_guard replacement;
        SPANIO_REQUIRE(connect_client(fixture.port(), replacement));
        SPANIO_CHECK(ping(replacement.get()));
        SPANIO_CHECK(connects.load() == kConnectionLimit + 1);
    }
}

// Case 14: an exception from a callback closes only that connection, with
// handler_failed; the server keeps serving, even if on_close throws too
SPANIO_TEST(handler_exception_closes_one_connection)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        server_fixture fixture(backend);
        auto on_message = [](const tcp_connection_ptr& connection, std::string_view data)
        {
            if (data == "boom")
            {
                throw std::runtime_error("callback failure");
            }
            connection->send(data);
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
            throw std::runtime_error("on_close failure");
        };
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard victim;
        socket_guard bystander;
        SPANIO_REQUIRE(connect_client(fixture.port(), victim));
        SPANIO_REQUIRE(connect_client(fixture.port(), bystander));
        SPANIO_REQUIRE(ping(bystander.get()));

        const std::string boom = "boom";
        SPANIO_REQUIRE(send_all(victim.get(), boom.data(), boom.size()));
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::handler_failed);
        SPANIO_CHECK(sees_close(victim.get()));

        SPANIO_CHECK(ping(bystander.get()));
        socket_guard newcomer;
        SPANIO_REQUIRE(connect_client(fixture.port(), newcomer));
        SPANIO_CHECK(ping(newcomer.get()));
    }
}

// Case 15: stop() with clients connected closes each connection once, run()
// returns, and new connections are refused
SPANIO_TEST(stop_closes_all_connections)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        server_fixture fixture(backend);
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_close(on_close);
        fixture.start(kFourThreads);
        const std::uint16_t port = fixture.port();

        std::vector<socket_guard> clients(static_cast<std::size_t>(kStopClients));
        for (socket_guard& client : clients)
        {
            SPANIO_REQUIRE(connect_client(port, client));
        }
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), static_cast<std::size_t>(kStopClients)));

        const test_clock::time_point start = test_clock::now();
        fixture.stop_and_join();
        SPANIO_CHECK(elapsed_ms(start) < kWaitLimitMs);

        SPANIO_CHECK(closes.count() == static_cast<std::size_t>(kStopClients));
        for (std::size_t i = 0; i < closes.count(); ++i)
        {
            SPANIO_CHECK(!closes.reason(i));
        }
        SPANIO_CHECK(fixture.server().connection_count() == 0);
        for (socket_guard& client : clients)
        {
            SPANIO_CHECK(sees_close(client.get()));
        }

        std::error_code ec;
        socket_guard late(spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);
        SPANIO_CHECK(connect_to(late.get(), loopback_address(port)) != 0);
        SPANIO_CHECK(spanio::detail::last_error() == std::errc::connection_refused);
    }
}

// Case 16: a peer that resets the connection is reported through on_close
SPANIO_TEST(peer_reset_reports_error)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        server_fixture fixture(backend);
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), 1));

        linger abort_on_close{};
        abort_on_close.l_onoff = kLingerOn;
        abort_on_close.l_linger = kLingerAbortSeconds;
        SPANIO_REQUIRE(::setsockopt(client.get(), SOL_SOCKET, SO_LINGER,
                                    reinterpret_cast<const char*>(&abort_on_close),
                                    static_cast<spanio::detail::socklen_type>(sizeof(abort_on_close)))
                       != spanio::detail::kSocketCallFailed);
        client.reset();

        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == std::errc::connection_reset);
    }
}

// Case 17: tcp_connection::context keeps user data across the callbacks of
// one connection
SPANIO_TEST(connection_context_keeps_user_data)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> counted(0);
        close_record closes;
        server_fixture fixture(backend);
        auto on_connect = [](const tcp_connection_ptr& connection)
        {
            connection->context() = 0;
        };
        auto on_message = [](const tcp_connection_ptr& connection, std::string_view data)
        {
            ++std::any_cast<int&>(connection->context());
            connection->send(data);
        };
        auto on_close = [&](const tcp_connection_ptr& connection, std::error_code reason)
        {
            counted = std::any_cast<int>(connection->context());
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        // Waiting for each echo keeps the two sends in separate reads
        SPANIO_CHECK(ping(client.get()));
        SPANIO_CHECK(ping(client.get()));
        client.reset();

        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(counted.load() == 2);
    }
}

// Case 18: four run() threads echo length-prefixed messages for 16 clients;
// every reply is intact
SPANIO_TEST(multi_thread_framed_echo)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        auto on_message = [](const tcp_connection_ptr& connection, std::string_view data)
        {
            connection->send(spanio::encode_length_prefixed(data));
        };
        fixture.server().set_framer(spanio::length_prefix_framer());
        fixture.server().on_message(on_message);
        fixture.start(kFourThreads);
        const std::uint16_t port = fixture.port();

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
            for (int m = 0; m < kEchoMessages; ++m)
            {
                const std::size_t size = kEchoBaseSize + static_cast<std::size_t>(id) * kEchoClientStep
                                       + static_cast<std::size_t>(m) * kEchoMessageStep;
                const std::string message = spanio::encode_length_prefixed(make_payload(size));
                std::string reply(message.size(), '\0');
                if (!send_all(client.get(), message.data(), message.size())
                    || !recv_exact(client.get(), &reply[0], reply.size()) || reply != message)
                {
                    ++failures;
                    return;
                }
                ++echoed;
            }
        };
        std::vector<std::thread> clients;
        for (int id = 0; id < kEchoClients; ++id)
        {
            clients.emplace_back(run_client, id);
        }
        for (std::thread& client : clients)
        {
            client.join();
        }
        SPANIO_CHECK(failures.load() == 0);
        SPANIO_CHECK(echoed.load() == kEchoClients * kEchoMessages);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
