// ============================================================================
// File purpose: Tests of the time limits of the high-level servers: the idle
//               timeout and close() timeout of tcp_server, and the graceful
//               stop of tcp_server and udp_server. Clients are plain blocking
//               sockets. Every case runs once per proactor backend of the
//               platform. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_server_timeouts executable by CMake. Run it
//               with no arguments for every case, or with a substring of a
//               case name, e.g. `test_server_timeouts graceful`.
//
//               Timing checks are strict in one direction only: a limit must
//               not take effect early. Upper bounds are generous.
//
//               Declaration order in each case: recorders, fixture, clients.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#include "spanio/tcp_server.h"
#include "spanio/udp_server.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"
#include "test_proactor_decorators.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace udp = spanio::udp;
using spanio::io_context;
using spanio::tcp_connection_ptr;
using spanio::tcp_server;
using spanio::tcp_server_options;
using spanio::udp_server;
using spanio::udp_server_options;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::connect_client;
using spanio::test::connect_to;
using spanio::test::holding_proactor;
using spanio::test::loopback_address;
using spanio::test::make_context;
using spanio::test::send_all;
using spanio::test::socket_guard;

namespace {

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Threads running the server in the single-threaded cases
constexpr int kOneThread = 1;

// Threads running the server in the multi-threaded case
constexpr int kFourThreads = 4;

// Idle timeout of most idle cases
constexpr std::chrono::milliseconds kIdleTimeout(150);

// How long the activity cases keep the connection busy, well over
// kIdleTimeout
constexpr std::chrono::milliseconds kActivePeriod(600);

// Pause between two pieces of activity, well under kIdleTimeout
constexpr std::chrono::milliseconds kActivityInterval(40);

// How long a case watches a connection to prove it stays open
constexpr std::chrono::milliseconds kQuietWatch(400);

// Short close limit or grace period that a stalled flush runs into
constexpr std::chrono::milliseconds kShortLimit(200);

// Limit or grace period long enough that it never runs out in a case
constexpr std::chrono::milliseconds kLongLimit(30000);

// Delay before stop() in the stop-during-grace case
constexpr std::chrono::milliseconds kBeforeStop(100);

// Generous upper bound for anything that should finish soon
constexpr std::chrono::seconds kGenerousLimit(10);

// Pause between two checks of a condition being waited for
constexpr std::chrono::milliseconds kPollInterval(2);

// Size of each piece queued in the flush cases
constexpr std::size_t kChunkSize = 64 * 1024;

// Pieces queued for a client that does not read: far more than the kernel
// buffers, so the flush cannot finish
constexpr std::size_t kStalledChunks = 512;

// Send queue limit that lets kStalledChunks pieces be queued
constexpr std::size_t kLargeSendQueue = kChunkSize * kStalledChunks * 2;

// Pieces queued for a client that reads them all
constexpr std::size_t kFlushChunks = 16;

// Bytes sent per piece of activity
constexpr std::size_t kActivityBytes = 1024;

// Clients in the graceful-stop flush case
constexpr int kFlushClients = 2;

// Clients in the multi-threaded idle case
constexpr int kIdleClients = 20;

// Datagrams queued in the UDP flush case
constexpr int kQueuedDatagrams = 3;

// Size of the UDP client's receive buffer
constexpr std::size_t kDatagramBufferSize = 2048;

// Receive chunk of the TCP clients
constexpr std::size_t kReadChunk = 64 * 1024;

// Time since a starting point
// Parameter start: time the measurement began
// Return value: the elapsed time
test_clock::duration elapsed(test_clock::time_point start)
{
    return test_clock::now() - start;
}

// Wait until a condition holds
// Parameter condition: function object returning bool
// Return value: true if it held within kGenerousLimit
template <typename Condition>
bool wait_until(Condition condition)
{
    const test_clock::time_point start = test_clock::now();
    while (!condition())
    {
        if (elapsed(start) > kGenerousLimit)
        {
            return false;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
    return true;
}

// Payload made of a repeated letter
// Parameter chunks: number of kChunkSize pieces
// Return value: the payload
std::string make_payload(std::size_t chunks)
{
    return std::string(chunks * kChunkSize, 'p');
}

// Queue a payload in kChunkSize pieces, so most of it waits in the queue
// Parameter connection: the connection
// Parameter payload:    bytes to send
void send_in_chunks(const tcp_connection_ptr& connection, const std::string& payload)
{
    for (std::size_t offset = 0; offset < payload.size(); offset += kChunkSize)
    {
        connection->send(std::string_view(payload).substr(offset, kChunkSize));
    }
}

// Read everything until the peer closes
// Parameter handle: blocking client socket
// Parameter out:    receives the bytes
// Return value: true if the connection ended with an orderly close
bool read_until_closed(spanio::detail::native_handle_t handle, std::string& out)
{
    std::vector<char> chunk(kReadChunk);
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

// Thread-safe record of on_close calls and when they happened
class close_record
{
public:
    // Store one on_close call
    // Parameter reason: reason passed to on_close
    void add(const std::error_code& reason)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_reasons.push_back(reason);
        m_times.push_back(test_clock::now());
    }

    // Number of on_close calls so far
    // Return value: the count
    std::size_t count() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_reasons.size();
    }

    // Reason of one call
    // Parameter index: call number, from 0
    // Return value: the reason
    std::error_code reason(std::size_t index) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_reasons.at(index);
    }

    // Time of one call
    // Parameter index: call number, from 0
    // Return value: when it happened
    test_clock::time_point time(std::size_t index) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_times.at(index);
    }

    // Wait for a number of calls
    // Parameter expected: count to wait for
    // Return value: true if it was reached in time
    bool wait_for(std::size_t expected) const
    {
        auto reached = [this, expected]()
        {
            return count() >= expected;
        };
        return wait_until(reached);
    }

private:
    // Guards the vectors
    mutable std::mutex m_mutex;
    // Reasons in arrival order
    std::vector<std::error_code> m_reasons;
    // Times in arrival order
    std::vector<test_clock::time_point> m_times;
};

// Holds the connection most recently seen by on_connect, and when
class connection_slot
{
public:
    // Store a connection
    // Parameter connection: the connection
    void set(const tcp_connection_ptr& connection)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_connection = connection;
        m_connected_at = test_clock::now();
    }

    // Wait for a connection to arrive
    // Return value: the connection, or nullptr on timeout
    tcp_connection_ptr wait() const
    {
        auto arrived = [this]()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_connection != nullptr;
        };
        wait_until(arrived);
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_connection;
    }

    // When the connection arrived
    // Return value: the time
    test_clock::time_point connected_at() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_connected_at;
    }

    // Drop the stored connection, so it does not outlive the server
    void reset()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_connection.reset();
    }

private:
    // Guards the fields below
    mutable std::mutex m_mutex;
    // Latest connection
    tcp_connection_ptr m_connection;
    // When it arrived
    test_clock::time_point m_connected_at;
};

// A context with its run() threads
class context_threads
{
public:
    // Parameter context: context to run; must outlive this object
    explicit context_threads(io_context& context)
        : m_context(&context)
    {
    }

    // Join the threads
    ~context_threads()
    {
        join();
    }

    context_threads(const context_threads&) = delete;
    context_threads& operator=(const context_threads&) = delete;

    // Start the run() threads
    // Parameter threads: number of threads
    void start(int threads)
    {
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

    // Wait for every run() to return
    void join()
    {
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
    io_context* m_context;
    // Threads calling run()
    std::vector<std::thread> m_threads;
};

// A tcp_server on 127.0.0.1 on an external context with its run() threads.
// The destructor stops the server and joins them.
class tcp_fixture
{
public:
    // Parameter backend: proactor backend to use
    // Parameter options: server limits
    tcp_fixture(const backend_factory& backend, const tcp_server_options& options)
        : m_context(make_context(backend))
        , m_server(new tcp_server(*m_context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort),
                                  options))
        , m_threads(*m_context)
    {
    }

    // Stop and join
    ~tcp_fixture()
    {
        m_server->stop();
        m_threads.join();
    }

    tcp_fixture(const tcp_fixture&) = delete;
    tcp_fixture& operator=(const tcp_fixture&) = delete;

    // The server
    // Return value: the server
    tcp_server& server()
    {
        return *m_server;
    }

    // Port of the server, or fail the case
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
        m_threads.start(threads);
    }

    // Wait for every run() to return, without stopping the server
    void join()
    {
        m_threads.join();
    }

private:
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The server
    std::unique_ptr<tcp_server> m_server;
    // Threads calling run()
    context_threads m_threads;
};

// Options with an idle timeout
// Parameter timeout: the idle timeout
// Return value: the options
tcp_server_options idle_options(std::chrono::milliseconds timeout)
{
    tcp_server_options options;
    options.m_idle_timeout = timeout;
    return options;
}

// Options for queuing a lot of data with a given close limit
// Parameter limit: close timeout
// Return value: the options
tcp_server_options flush_options(std::chrono::milliseconds limit)
{
    tcp_server_options options;
    options.m_close_timeout = limit;
    options.m_max_send_queue_bytes = kLargeSendQueue;
    return options;
}

// Whether a new connection to a port is refused
// Parameter port: the port
// Return value: true if connect() failed with connection_refused
bool connection_refused(std::uint16_t port)
{
    std::error_code ec;
    socket_guard late(spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    return !ec && connect_to(late.get(), loopback_address(port)) != 0
        && spanio::detail::last_error() == std::errc::connection_refused;
}

// A udp_server on 127.0.0.1 whose backend holds sends until released
class udp_fixture
{
public:
    // Parameter backend: proactor backend to wrap
    explicit udp_fixture(const backend_factory& backend)
        : m_holder(nullptr)
        , m_context(make_holding_context(backend, m_holder))
        , m_server(new udp_server(*m_context, udp::endpoint(spanio::ipv4::loopback(), kAnyPort),
                                  udp_server_options()))
        , m_threads(*m_context)
    {
    }

    // Stop and join
    ~udp_fixture()
    {
        m_server->stop();
        m_threads.join();
    }

    udp_fixture(const udp_fixture&) = delete;
    udp_fixture& operator=(const udp_fixture&) = delete;

    // The server
    // Return value: the server
    udp_server& server()
    {
        return *m_server;
    }

    // The decorator that holds sends
    // Return value: the decorator, owned by the context
    holding_proactor& holder()
    {
        return *m_holder;
    }

    // Start the server and its run() thread
    void start()
    {
        m_server->start();
        m_threads.start(kOneThread);
    }

    // Wait for run() to return, without stopping the server
    void join()
    {
        m_threads.join();
    }

private:
    // Create a context whose backend holds sends
    // Parameter backend: proactor backend to wrap
    // Parameter holder:  receives the decorator
    // Return value: the context
    static std::unique_ptr<io_context> make_holding_context(const backend_factory& backend,
                                                            holding_proactor*& holder)
    {
        std::error_code ec;
        spanio::detail::io_proactor_ptr inner = backend.m_create(ec);
        SPANIO_REQUIRE(!ec);
        holder = new holding_proactor(std::move(inner));
        return std::unique_ptr<io_context>(new io_context(spanio::detail::io_proactor_ptr(holder)));
    }

    // Decorator in front of the real backend; owned by m_context
    holding_proactor* m_holder;
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The server
    std::unique_ptr<udp_server> m_server;
    // Thread calling run()
    context_threads m_threads;
};

// A blocking UDP socket bound to 127.0.0.1 with a receive timeout
class udp_client
{
public:
    // Create and bind the socket
    // Return value: true on success
    bool open()
    {
        std::error_code ec;
        m_socket.reset(spanio::detail::create_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, ec));
        const sockaddr_in local = loopback_address(kAnyPort);
        return !ec && spanio::test::set_receive_timeout(m_socket.get())
            && ::bind(m_socket.get(), reinterpret_cast<const sockaddr*>(&local),
                      static_cast<spanio::detail::socklen_type>(sizeof(local)))
                   != spanio::detail::kSocketCallFailed;
    }

    // This client's address
    // Return value: the endpoint
    udp::endpoint local() const
    {
        const sockaddr_in address = spanio::test::local_address(m_socket.get());
        return udp::endpoint::from_native(reinterpret_cast<const sockaddr*>(&address),
                                          static_cast<spanio::detail::socklen_type>(sizeof(address)));
    }

    // Receive one datagram
    // Parameter data: receives the bytes
    // Return value: false on timeout or error
    bool receive(std::string& data)
    {
        char buffer[kDatagramBufferSize];
        const int result = ::recv(m_socket.get(), buffer, static_cast<int>(sizeof(buffer)),
                                  spanio::test::kNoSocketFlags);
        if (result < 0)
        {
            return false;
        }
        data.assign(buffer, static_cast<std::size_t>(result));
        return true;
    }

private:
    // The socket
    socket_guard m_socket;
};

}

// Case 1: a connection that stays silent is closed with idle_timeout, not
// before the timeout
SPANIO_TEST(idle_timeout_closes_silent_connection)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        connection_slot slot;
        tcp_fixture fixture(backend, idle_options(kIdleTimeout));
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
        SPANIO_REQUIRE(slot.wait() != nullptr);
        slot.reset();
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::idle_timeout);
        SPANIO_CHECK(closes.time(0) - slot.connected_at() >= kIdleTimeout);
        SPANIO_CHECK(fixture.server().connection_count() == 0);
    }
}

// Case 2: a client that keeps sending is not idle; once it stops, the
// connection times out
SPANIO_TEST(received_data_keeps_connection_alive)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_fixture fixture(backend, idle_options(kIdleTimeout));
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        const std::string piece(kActivityBytes, 'a');
        const test_clock::time_point start = test_clock::now();
        test_clock::time_point last_send = start;
        while (elapsed(start) < kActivePeriod)
        {
            SPANIO_REQUIRE(send_all(client.get(), piece.data(), piece.size()));
            last_send = test_clock::now();
            std::this_thread::sleep_for(kActivityInterval);
        }
        SPANIO_CHECK(closes.count() == 0);

        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::idle_timeout);
        SPANIO_CHECK(closes.time(0) - last_send >= kIdleTimeout);
    }
}

// Case 3: a client that only receives is not idle while the server keeps
// sending to it
SPANIO_TEST(sending_counts_as_activity)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        connection_slot slot;
        tcp_fixture fixture(backend, idle_options(kIdleTimeout));
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
        slot.reset();

        std::atomic<bool> reading(true);
        const spanio::detail::native_handle_t handle = client.get();
        auto drain = [&reading, handle]()
        {
            std::vector<char> chunk(kReadChunk);
            while (reading.load()
                   && ::recv(handle, chunk.data(), static_cast<int>(chunk.size()), spanio::test::kNoSocketFlags) > 0)
            {
            }
        };
        std::thread reader(drain);

        const std::string piece(kActivityBytes, 's');
        const test_clock::time_point start = test_clock::now();
        while (elapsed(start) < kActivePeriod)
        {
            connection->send(piece);
            std::this_thread::sleep_for(kActivityInterval);
        }
        SPANIO_CHECK(closes.count() == 0);

        // Silence on both sides now: the connection times out, and the
        // reader sees the close
        SPANIO_CHECK(closes.wait_for(1));
        reading = false;
        reader.join();
        SPANIO_CHECK(closes.reason(0) == spanio::error::idle_timeout);
    }
}

// Case 4: with the default options a silent connection is never closed
SPANIO_TEST(idle_timeout_off_by_default)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_fixture fixture(backend, tcp_server_options());
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard client;
        SPANIO_REQUIRE(connect_client(fixture.port(), client));
        std::this_thread::sleep_for(kQuietWatch);
        SPANIO_CHECK(closes.count() == 0);
        SPANIO_CHECK(fixture.server().connection_count() == 1);
    }
}

// Case 5: close() gives up after the close timeout when the client does not
// read, and reports close_timeout
SPANIO_TEST(close_timeout_aborts_stalled_flush)
{
    const std::string payload = make_payload(kStalledChunks);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        std::mutex closed_at_mutex;
        test_clock::time_point closed_at;
        tcp_fixture fixture(backend, flush_options(kShortLimit));
        auto on_connect = [&](const tcp_connection_ptr& connection)
        {
            send_in_chunks(connection, payload);
            std::lock_guard<std::mutex> lock(closed_at_mutex);
            closed_at = test_clock::now();
            connection->close();
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard silent;
        SPANIO_REQUIRE(connect_client(fixture.port(), silent));
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(closes.reason(0) == spanio::error::close_timeout);
        std::lock_guard<std::mutex> lock(closed_at_mutex);
        SPANIO_CHECK(closes.time(0) - closed_at >= kShortLimit);
    }
}

// Case 6: a close() that can send everything finishes well before its
// timeout, with no reason
SPANIO_TEST(close_finishes_before_timeout)
{
    const std::string payload = make_payload(kFlushChunks);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_fixture fixture(backend, flush_options(kLongLimit));
        auto on_connect = [&payload](const tcp_connection_ptr& connection)
        {
            send_in_chunks(connection, payload);
            connection->close();
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
        const test_clock::time_point start = test_clock::now();
        std::string received;
        SPANIO_CHECK(read_until_closed(client.get(), received));
        SPANIO_CHECK(received == payload);
        SPANIO_REQUIRE(closes.wait_for(1));
        SPANIO_CHECK(!closes.reason(0));
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
    }
}

// Case 7: a graceful stop lets every client receive what was queued, then
// closes; nothing more reaches on_message, run() returns and new clients
// are refused
SPANIO_TEST(graceful_stop_flushes_and_closes)
{
    const std::string payload = make_payload(kFlushChunks);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        std::atomic<int> queued(0);
        std::atomic<bool> stopping(false);
        std::atomic<int> late_messages(0);
        tcp_fixture fixture(backend, flush_options(kLongLimit));
        auto on_connect = [&payload, &queued](const tcp_connection_ptr& connection)
        {
            send_in_chunks(connection, payload);
            ++queued;
        };
        auto on_message = [&](const tcp_connection_ptr&, std::string_view)
        {
            if (stopping.load())
            {
                ++late_messages;
            }
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_message(on_message);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        std::vector<socket_guard> clients(static_cast<std::size_t>(kFlushClients));
        for (socket_guard& client : clients)
        {
            SPANIO_REQUIRE(connect_client(port, client));
        }
        auto all_connected = [&queued]()
        {
            return queued.load() == kFlushClients;
        };
        SPANIO_REQUIRE(wait_until(all_connected));

        stopping = true;
        fixture.server().stop_gracefully(kLongLimit);
        const std::string late = "late";
        for (socket_guard& client : clients)
        {
            send_all(client.get(), late.data(), late.size());
            std::string received;
            SPANIO_CHECK(read_until_closed(client.get(), received));
            SPANIO_CHECK(received == payload);
        }
        const test_clock::time_point start = test_clock::now();
        fixture.join();
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(closes.count() == static_cast<std::size_t>(kFlushClients));
        SPANIO_CHECK(late_messages.load() == 0);
        SPANIO_CHECK(connection_refused(port));
    }
}

// Case 8: a graceful stop gives up on a client that does not read once the
// grace period is over
SPANIO_TEST(graceful_stop_aborts_after_grace)
{
    const std::string payload = make_payload(kStalledChunks);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        std::atomic<int> queued(0);
        tcp_fixture fixture(backend, flush_options(kLongLimit));
        auto on_connect = [&payload, &queued](const tcp_connection_ptr& connection)
        {
            send_in_chunks(connection, payload);
            ++queued;
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard silent;
        SPANIO_REQUIRE(connect_client(fixture.port(), silent));
        auto connected = [&queued]()
        {
            return queued.load() == 1;
        };
        SPANIO_REQUIRE(wait_until(connected));

        const test_clock::time_point start = test_clock::now();
        fixture.server().stop_gracefully(kShortLimit);
        fixture.join();
        SPANIO_CHECK(elapsed(start) >= kShortLimit);
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_REQUIRE(closes.count() == 1);
        SPANIO_CHECK(closes.reason(0) == spanio::error::close_timeout);
    }
}

// Case 9: a graceful stop with no connections finishes at once
SPANIO_TEST(graceful_stop_without_connections)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        tcp_fixture fixture(backend, tcp_server_options());
        fixture.start(kOneThread);

        const test_clock::time_point start = test_clock::now();
        fixture.server().stop_gracefully(kLongLimit);
        fixture.join();
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
    }
}

// Case 10: stop() during a grace period ends the wait at once
SPANIO_TEST(stop_during_grace_is_immediate)
{
    const std::string payload = make_payload(kStalledChunks);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        std::atomic<int> queued(0);
        tcp_fixture fixture(backend, flush_options(kLongLimit));
        auto on_connect = [&payload, &queued](const tcp_connection_ptr& connection)
        {
            send_in_chunks(connection, payload);
            ++queued;
        };
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_connect(on_connect);
        fixture.server().on_close(on_close);
        fixture.start(kOneThread);

        socket_guard silent;
        SPANIO_REQUIRE(connect_client(fixture.port(), silent));
        auto connected = [&queued]()
        {
            return queued.load() == 1;
        };
        SPANIO_REQUIRE(wait_until(connected));

        const test_clock::time_point start = test_clock::now();
        fixture.server().stop_gracefully(kLongLimit);
        std::this_thread::sleep_for(kBeforeStop);
        SPANIO_CHECK(closes.count() == 0);
        fixture.server().stop();
        fixture.join();
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(closes.count() == 1);
    }
}

// Case 11: a graceful stop of a udp_server sends every queued datagram
// before it closes, and refuses new ones
SPANIO_TEST(udp_graceful_stop_flushes_queue)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        udp_fixture fixture(backend);
        fixture.start();
        udp_client client;
        SPANIO_REQUIRE(client.open());

        for (int i = 0; i < kQueuedDatagrams; ++i)
        {
            SPANIO_CHECK(fixture.server().send_to(client.local(), std::to_string(i)));
        }
        fixture.server().stop_gracefully(kLongLimit);
        SPANIO_CHECK(!fixture.server().send_to(client.local(), "refused"));

        fixture.holder().release();
        for (int i = 0; i < kQueuedDatagrams; ++i)
        {
            std::string data;
            SPANIO_CHECK(client.receive(data));
            SPANIO_CHECK(data == std::to_string(i));
        }
        const test_clock::time_point start = test_clock::now();
        fixture.join();
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
        SPANIO_CHECK(fixture.server().stats().m_sent == static_cast<std::uint64_t>(kQueuedDatagrams));
    }
}

// Case 12: a graceful stop of a udp_server whose sends never finish closes
// once the grace period is over
SPANIO_TEST(udp_graceful_stop_gives_up_after_grace)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        udp_fixture fixture(backend);
        fixture.start();
        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_CHECK(fixture.server().send_to(client.local(), "held"));

        const test_clock::time_point start = test_clock::now();
        fixture.server().stop_gracefully(kShortLimit);
        fixture.join();
        SPANIO_CHECK(elapsed(start) >= kShortLimit);
        SPANIO_CHECK(elapsed(start) < kGenerousLimit);
    }
}

// Case 13: with four run() threads, twenty silent connections each time out
// exactly once
SPANIO_TEST(multi_thread_idle_timeouts)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        close_record closes;
        tcp_fixture fixture(backend, idle_options(kIdleTimeout));
        auto on_close = [&closes](const tcp_connection_ptr&, std::error_code reason)
        {
            closes.add(reason);
        };
        fixture.server().on_close(on_close);
        fixture.start(kFourThreads);

        std::vector<socket_guard> clients(static_cast<std::size_t>(kIdleClients));
        for (socket_guard& client : clients)
        {
            SPANIO_REQUIRE(connect_client(fixture.port(), client));
        }
        SPANIO_REQUIRE(closes.wait_for(static_cast<std::size_t>(kIdleClients)));
        std::this_thread::sleep_for(kQuietWatch);
        SPANIO_CHECK(closes.count() == static_cast<std::size_t>(kIdleClients));
        for (std::size_t i = 0; i < closes.count(); ++i)
        {
            SPANIO_CHECK(closes.reason(i) == spanio::error::idle_timeout);
        }
        SPANIO_CHECK(fixture.server().connection_count() == 0);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
