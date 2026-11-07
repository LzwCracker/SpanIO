// ============================================================================
// File purpose: Unit tests for udp_server (udp_server.h) and the
//               udp_echo_server example, which is included with
//               SPANIO_UDP_ECHO_SERVER_NO_MAIN defined so the tests run the
//               shipped code. Clients are plain blocking sockets. Cases run
//               once per proactor backend of the platform on an external
//               context; one case covers the owned context on the default
//               backend. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_udp_server executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_udp_server overflow`.
//
//               UDP may drop datagrams, so clients wait for each reply before
//               sending the next datagram, and readers use a large receive
//               buffer.
//
//               Declaration order in each case: recorders, fixture, clients.
//               The fixture stops the server and joins its threads before
//               the recorders the callback writes to are destroyed.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#define SPANIO_UDP_ECHO_SERVER_NO_MAIN
#include "../examples/udp_echo_server.cpp"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"
#include "test_proactor_decorators.h"

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

namespace udp = spanio::udp;
using spanio::io_context;
using spanio::udp_server;
using spanio::udp_server_options;
using spanio::detail::native_handle_t;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;
using spanio::test::holding_proactor;
using spanio::test::socket_guard;

namespace {

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Threads running the server in the single-threaded cases
constexpr int kOneThread = 1;

// Threads running the server in the concurrency cases
constexpr int kFourThreads = 4;

// Longest wait for something the server is expected to do
constexpr int kWaitLimitMs = 5000;

// Pause between two checks of a condition being waited for
constexpr int kPollIntervalMs = 2;

// Pause that lets a loopback datagram (or the ICMP reply to one) arrive
constexpr int kSettleMs = 50;

// Size of the client receive buffers
constexpr std::size_t kClientBufferSize = 70000;

// Kernel receive buffer of the clients, so bursts are not dropped
constexpr int kClientKernelBuffer = 4 * 1024 * 1024;

// Time a callback stays busy in the overlap case, so overlaps get caught
constexpr int kCallbackHoldUs = 200;

// Clients in the overlap case
constexpr int kOverlapClients = 4;

// Datagrams each client sends in the overlap case
constexpr int kOverlapDatagrams = 50;

// Threads calling send_to() in the many-senders case
constexpr int kSenderThreads = 4;

// Datagrams each of them sends
constexpr int kDatagramsPerSender = 100;

// Base of the numbers in many-senders messages
constexpr int kDecimalBase = 10;

// Length of the "s:" that starts a many-senders message
constexpr std::size_t kSenderPrefixLength = 2;

// Send queue limit in the overflow case
constexpr std::size_t kSmallSendQueue = 64 * 1024;

// Datagram size in the overflow case
constexpr std::size_t kOverflowDatagram = 16 * 1024;

// Most datagrams the overflow case may try before giving up
constexpr int kOverflowAttempts = 100;

// Larger than the 65507-byte UDP payload limit
constexpr std::size_t kOversizedDatagram = 70000;

// Datagram limit in the truncation case
constexpr std::size_t kSmallDatagramLimit = 16;

// Size of the datagram in the truncation case: over the limit
constexpr std::size_t kLongDatagramSize = 32;

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

// Sleep long enough for loopback traffic to arrive
void settle()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(kSettleMs));
}

// A blocking UDP client on 127.0.0.1 with a receive timeout
class udp_client
{
public:
    // Create and bind the socket
    // Return value: true on success; it uses no check macros, so it is safe
    //               in helper threads
    bool open()
    {
        std::error_code ec;
        m_socket.reset(spanio::detail::create_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, ec));
        if (ec)
        {
            return false;
        }
        const int kernel_buffer = kClientKernelBuffer;
        const sockaddr_in local = spanio::test::loopback_address(kAnyPort);
        return spanio::test::set_receive_timeout(m_socket.get())
            && ::setsockopt(m_socket.get(), SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&kernel_buffer),
                            static_cast<spanio::detail::socklen_type>(sizeof(kernel_buffer)))
                   != spanio::detail::kSocketCallFailed
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

    // Send one datagram
    // Parameter peer: destination
    // Parameter data: bytes to send
    // Return value: true if all of them went out
    bool send(const udp::endpoint& peer, std::string_view data)
    {
        const int result = ::sendto(m_socket.get(), data.data(), static_cast<int>(data.size()),
                                    spanio::test::kNoSocketFlags, peer.data(), peer.size());
        return result == static_cast<int>(data.size());
    }

    // Receive one datagram
    // Parameter data:   receives the bytes
    // Parameter sender: receives the sender, when given
    // Return value: false on timeout or error
    bool receive(std::string& data, udp::endpoint* sender = nullptr)
    {
        std::vector<char> buffer(kClientBufferSize);
        sockaddr_storage from{};
        spanio::detail::socklen_type length = static_cast<spanio::detail::socklen_type>(sizeof(from));
        const int result = ::recvfrom(m_socket.get(), buffer.data(), static_cast<int>(buffer.size()),
                                      spanio::test::kNoSocketFlags, reinterpret_cast<sockaddr*>(&from),
                                      &length);
        if (result < 0)
        {
            return false;
        }
        data.assign(buffer.data(), static_cast<std::size_t>(result));
        if (sender != nullptr)
        {
            *sender = udp::endpoint::from_native(reinterpret_cast<const sockaddr*>(&from), length);
        }
        return true;
    }

    // Send a datagram and check it comes back
    // Parameter peer: an echoing server
    // Parameter data: bytes to send
    // Return value: true if the reply matched
    bool ping(const udp::endpoint& peer, std::string_view data)
    {
        std::string reply;
        return send(peer, data) && receive(reply) && reply == data;
    }

private:
    // The socket
    socket_guard m_socket;
};

// A context with its run() threads, joined by the destructor or finish()
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

// A udp_server on 127.0.0.1 on an external context with its run() threads.
// The destructor stops the server and joins them.
class server_fixture
{
public:
    // Create the server on a backend
    // Parameter backend: proactor backend to use
    // Parameter options: server limits
    explicit server_fixture(const backend_factory& backend,
                            const udp_server_options& options = udp_server_options())
        : server_fixture(make_context(backend), options)
    {
    }

    // Create the server on a given context
    // Parameter context: context to run on
    // Parameter options: server limits
    server_fixture(std::unique_ptr<io_context> context, const udp_server_options& options)
        : m_context(std::move(context))
        , m_server(new udp_server(*m_context, udp::endpoint(spanio::ipv4::loopback(), kAnyPort), options))
        , m_threads(*m_context)
    {
    }

    // Stop and join
    ~server_fixture()
    {
        stop_and_join();
    }

    server_fixture(const server_fixture&) = delete;
    server_fixture& operator=(const server_fixture&) = delete;

    // The server
    // Return value: the server
    udp_server& server()
    {
        return *m_server;
    }

    // Address of the server, or fail the case
    // Return value: the endpoint
    udp::endpoint address() const
    {
        std::error_code ec;
        const udp::endpoint local = m_server->local_endpoint(ec);
        SPANIO_REQUIRE(!ec);
        return local;
    }

    // Start the server and the run() threads
    // Parameter threads: number of threads
    void start(int threads)
    {
        m_server->start();
        m_threads.start(threads);
    }

    // Stop the server and wait for every run() to return
    void stop_and_join()
    {
        m_server->stop();
        m_threads.join();
    }

private:
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The server
    std::unique_ptr<udp_server> m_server;
    // Threads calling run()
    context_threads m_threads;
};

// Create a context whose backend holds sends
// Parameter backend: proactor backend to wrap
// Parameter holder:  receives the decorator, owned by the context
// Return value: the context
std::unique_ptr<io_context> make_holding_context(const backend_factory& backend, holding_proactor*& holder)
{
    std::error_code ec;
    spanio::detail::io_proactor_ptr inner = backend.m_create(ec);
    SPANIO_REQUIRE(!ec);
    holder = new holding_proactor(std::move(inner));
    return std::unique_ptr<io_context>(new io_context(spanio::detail::io_proactor_ptr(holder)));
}

}

// Case 1: on its own context, run() serves clients, stop() from another
// thread ends it, and the server cannot run again
SPANIO_TEST(owned_context_run_and_stop)
{
    udp_server server(udp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    auto echo = [&server](const udp::endpoint& from, std::string_view data)
    {
        server.send_to(from, data);
    };
    server.on_datagram(echo);
    std::error_code ec;
    const udp::endpoint address = server.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);

    auto run_server = [&server]()
    {
        server.run();
    };
    std::thread runner(run_server);
    udp_client client;
    SPANIO_REQUIRE(client.open());
    SPANIO_CHECK(client.ping(address, "hello"));

    const test_clock::time_point start = test_clock::now();
    server.stop();
    runner.join();
    SPANIO_CHECK(elapsed_ms(start) < kWaitLimitMs);

    bool thrown = false;
    try
    {
        server.run();
    }
    catch (const std::logic_error&)
    {
        thrown = true;
    }
    SPANIO_CHECK(thrown);
}

// Case 2: on an external context start() works and run() is refused;
// starting twice and setting the callback after start are refused
SPANIO_TEST(external_context_start)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        udp_server& server = fixture.server();
        auto echo = [&server](const udp::endpoint& from, std::string_view data)
        {
            server.send_to(from, data);
        };
        server.on_datagram(echo);

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

        fixture.start(kOneThread);

        bool second_start_thrown = false;
        try
        {
            server.start();
        }
        catch (const std::logic_error&)
        {
            second_start_thrown = true;
        }
        SPANIO_CHECK(second_start_thrown);

        bool late_callback_thrown = false;
        try
        {
            server.on_datagram(echo);
        }
        catch (const std::logic_error&)
        {
            late_callback_thrown = true;
        }
        SPANIO_CHECK(late_callback_thrown);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_CHECK(client.ping(fixture.address(), "hello"));
    }
}

// Case 3: on_datagram gets the content, size and sender
SPANIO_TEST(on_datagram_gets_data_and_sender)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::mutex record_mutex;
        std::string received;
        udp::endpoint sender;
        std::atomic<int> calls(0);
        server_fixture fixture(backend);
        auto record = [&](const udp::endpoint& from, std::string_view data)
        {
            std::lock_guard<std::mutex> lock(record_mutex);
            received.assign(data.data(), data.size());
            sender = from;
            ++calls;
        };
        fixture.server().on_datagram(record);
        fixture.start(kOneThread);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_REQUIRE(client.send(fixture.address(), "hello"));
        auto arrived = [&calls]()
        {
            return calls.load() == 1;
        };
        SPANIO_REQUIRE(wait_until(arrived));

        std::lock_guard<std::mutex> lock(record_mutex);
        SPANIO_CHECK(received == "hello");
        SPANIO_CHECK(sender == client.local());
    }
}

// Case 4: the example echoes each datagram from its own address and counts
// what it did
SPANIO_TEST(udp_echo_example)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        udp_echo::udp_echo_server server(*ctx, udp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        context_threads threads(*ctx);
        server.start();
        threads.start(kOneThread);

        std::error_code ec;
        const udp::endpoint address = server.local_endpoint(ec);
        SPANIO_REQUIRE(!ec);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        const std::string message = "hello";
        SPANIO_CHECK(client.send(address, message));
        std::string reply;
        udp::endpoint replied_from;
        SPANIO_CHECK(client.receive(reply, &replied_from));
        SPANIO_CHECK(reply == message);
        SPANIO_CHECK(replied_from == address);

        auto counted = [&server]()
        {
            return server.stats().m_sent == 1;
        };
        SPANIO_CHECK(wait_until(counted));
        SPANIO_CHECK(server.stats().m_received == 1);

        server.stop();
        threads.join();
    }
}

// Case 5: an empty datagram reaches the callback and can be answered
SPANIO_TEST(empty_datagram)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> calls(0);
        std::atomic<std::size_t> last_size(1);
        server_fixture fixture(backend);
        udp_server& server = fixture.server();
        auto reply_empty = [&](const udp::endpoint& from, std::string_view data)
        {
            last_size = data.size();
            ++calls;
            server.send_to(from, std::string_view());
        };
        server.on_datagram(reply_empty);
        fixture.start(kOneThread);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_REQUIRE(client.send(fixture.address(), std::string_view()));
        std::string reply = "not empty";
        SPANIO_CHECK(client.receive(reply));
        SPANIO_CHECK(reply.empty());
        SPANIO_CHECK(calls.load() == 1);
        SPANIO_CHECK(last_size.load() == 0);
    }
}

// Case 6: with four run() threads and several clients, on_datagram calls
// never overlap
SPANIO_TEST(callbacks_never_overlap)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> busy(0);
        std::atomic<int> overlaps(0);
        server_fixture fixture(backend);
        udp_server& server = fixture.server();
        auto hold_and_echo = [&](const udp::endpoint& from, std::string_view data)
        {
            if (busy.exchange(1) != 0)
            {
                ++overlaps;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(kCallbackHoldUs));
            busy.store(0);
            server.send_to(from, data);
        };
        server.on_datagram(hold_and_echo);
        fixture.start(kFourThreads);
        const udp::endpoint address = fixture.address();

        std::atomic<int> failures(0);
        auto run_client = [&]()
        {
            udp_client client;
            if (!client.open())
            {
                ++failures;
                return;
            }
            for (int i = 0; i < kOverlapDatagrams; ++i)
            {
                if (!client.ping(address, std::to_string(i)))
                {
                    ++failures;
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
        SPANIO_CHECK(failures.load() == 0);
        SPANIO_CHECK(overlaps.load() == 0);
    }
}

// Case 7: several threads send to one client at once; every datagram is
// whole, and each thread's datagrams keep their order
SPANIO_TEST(send_from_many_threads)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        fixture.start(kFourThreads);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        const udp::endpoint target = client.local();
        udp_server& server = fixture.server();
        auto send_numbers = [&server, target](int sender)
        {
            for (int i = 0; i < kDatagramsPerSender; ++i)
            {
                server.send_to(target, std::to_string(sender) + ":" + std::to_string(i));
            }
        };
        std::vector<std::thread> senders;
        for (int sender = 0; sender < kSenderThreads; ++sender)
        {
            senders.emplace_back(send_numbers, sender);
        }

        const int total = kSenderThreads * kDatagramsPerSender;
        std::vector<int> next(static_cast<std::size_t>(kSenderThreads), 0);
        int bad_datagrams = 0;
        int received = 0;
        std::string data;
        while (received < total && client.receive(data))
        {
            ++received;
            const int sender = data[0] - '0';
            const int number = static_cast<int>(std::strtol(data.c_str() + kSenderPrefixLength, nullptr, kDecimalBase));
            if (data[1] != ':' || sender < 0 || sender >= kSenderThreads
                || number != next[static_cast<std::size_t>(sender)])
            {
                ++bad_datagrams;
                continue;
            }
            ++next[static_cast<std::size_t>(sender)];
        }
        for (std::thread& sender : senders)
        {
            sender.join();
        }
        SPANIO_CHECK(received == total);
        SPANIO_CHECK(bad_datagrams == 0);
    }
}

// Case 8: while sends are held the queue fills up: send_to() refuses more
// and counts them, and what was queued still goes out once sends resume
SPANIO_TEST(send_queue_overflow_drops)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        holding_proactor* holder = nullptr;
        udp_server_options options;
        options.m_max_send_queue_bytes = kSmallSendQueue;
        server_fixture fixture(make_holding_context(backend, holder), options);
        fixture.start(kOneThread);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        const std::string datagram(kOverflowDatagram, 'q');
        std::uint64_t accepted = 0;
        std::uint64_t refused = 0;
        for (int i = 0; i < kOverflowAttempts && refused == 0; ++i)
        {
            if (fixture.server().send_to(client.local(), datagram))
            {
                ++accepted;
            }
            else
            {
                ++refused;
            }
        }
        SPANIO_REQUIRE(refused == 1);
        SPANIO_CHECK(accepted * kOverflowDatagram <= kSmallSendQueue);
        SPANIO_CHECK(fixture.server().stats().m_queue_dropped == refused);

        holder->release();
        std::string data;
        for (std::uint64_t i = 0; i < accepted; ++i)
        {
            SPANIO_CHECK(client.receive(data));
            SPANIO_CHECK(data == datagram);
        }
        udp_server& server = fixture.server();
        auto all_sent = [&server, accepted]()
        {
            return server.stats().m_sent == accepted;
        };
        SPANIO_CHECK(wait_until(all_sent));
    }
}

// Case 9: a datagram the system rejects is counted, and sending continues
SPANIO_TEST(oversized_send_is_counted)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        fixture.start(kOneThread);
        udp_server& server = fixture.server();

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_CHECK(server.send_to(client.local(), std::string(kOversizedDatagram, 'x')));
        auto counted = [&server]()
        {
            return server.stats().m_send_failed == 1;
        };
        SPANIO_CHECK(wait_until(counted));

        SPANIO_CHECK(server.send_to(client.local(), "after"));
        std::string data;
        SPANIO_CHECK(client.receive(data));
        SPANIO_CHECK(data == "after");
    }
}

// Case 10: a datagram over the limit is counted and never reaches the
// callback; the next one does
SPANIO_TEST(truncated_datagram_is_dropped)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> calls(0);
        udp_server_options options;
        options.m_max_datagram_size = kSmallDatagramLimit;
        server_fixture fixture(backend, options);
        udp_server& server = fixture.server();
        auto echo = [&](const udp::endpoint& from, std::string_view data)
        {
            ++calls;
            server.send_to(from, data);
        };
        server.on_datagram(echo);
        fixture.start(kOneThread);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_REQUIRE(client.send(fixture.address(), std::string(kLongDatagramSize, 'L')));
        auto counted = [&server]()
        {
            return server.stats().m_truncated == 1;
        };
        SPANIO_CHECK(wait_until(counted));
        SPANIO_CHECK(calls.load() == 0);

        SPANIO_CHECK(client.ping(fixture.address(), "short"));
        SPANIO_CHECK(calls.load() == 1);
    }
}

// Case 11: an exception from on_datagram does not stop the server
SPANIO_TEST(handler_exception_keeps_serving)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> thrown(0);
        server_fixture fixture(backend);
        udp_server& server = fixture.server();
        auto throw_or_echo = [&](const udp::endpoint& from, std::string_view data)
        {
            if (data == "boom")
            {
                ++thrown;
                throw std::runtime_error("callback failure");
            }
            server.send_to(from, data);
        };
        server.on_datagram(throw_or_echo);
        fixture.start(kOneThread);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_REQUIRE(client.send(fixture.address(), "boom"));
        auto threw = [&thrown]()
        {
            return thrown.load() == 1;
        };
        SPANIO_CHECK(wait_until(threw));
        SPANIO_CHECK(client.ping(fixture.address(), "still here"));
    }
}

// Case 12: sending to a port nobody uses does not stop the server
SPANIO_TEST(closed_peer_port_keeps_serving)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        server_fixture fixture(backend);
        udp_server& server = fixture.server();
        auto echo = [&server](const udp::endpoint& from, std::string_view data)
        {
            server.send_to(from, data);
        };
        server.on_datagram(echo);
        fixture.start(kOneThread);

        udp::endpoint closed_port;
        {
            // Borrow a free port, then release it so nothing listens there
            udp_client probe;
            SPANIO_REQUIRE(probe.open());
            closed_port = probe.local();
        }
        SPANIO_CHECK(server.send_to(closed_port, "lost"));
        // The ICMP "port unreachable" reply arrives during this pause
        settle();

        udp_client client;
        SPANIO_REQUIRE(client.open());
        SPANIO_CHECK(client.ping(fixture.address(), "still here"));
        SPANIO_CHECK(server.stats().m_send_failed == 0);
    }
}

// Case 13: stop() with sends held and queued returns promptly, and later
// send_to() calls are refused
SPANIO_TEST(stop_drops_pending_sends)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        holding_proactor* holder = nullptr;
        server_fixture fixture(make_holding_context(backend, holder), udp_server_options());
        fixture.start(kFourThreads);

        udp_client client;
        SPANIO_REQUIRE(client.open());
        const udp::endpoint target = client.local();
        udp_server& server = fixture.server();
        SPANIO_CHECK(server.send_to(target, "held"));
        SPANIO_CHECK(server.send_to(target, "queued"));

        const test_clock::time_point start = test_clock::now();
        fixture.stop_and_join();
        SPANIO_CHECK(elapsed_ms(start) < kWaitLimitMs);
        SPANIO_CHECK(!server.send_to(target, "too late"));
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
