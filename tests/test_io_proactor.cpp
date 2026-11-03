// ============================================================================
// File purpose: Unit tests for the io_proactor contract (io_proactor.h). Each
//               case runs once per backend available on the platform:
//               - Windows: iocp_proactor (native) and reactor_proactor over
//                 the IOCP + AFD poll reactor
//               - Linux:   reactor_proactor over epoll
//               All traffic stays on 127.0.0.1.
// Usage:        Built as the test_io_proactor executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_io_proactor cancel`. Failures are printed
//               under the "[backend]" line of the backend being tested.
//
//               Declaration order matters in every case: operations first,
//               then the proactor, then the sockets. Sockets are destroyed
//               first (cancelling their I/O), then the proactor, and the
//               operations last, so the kernel never writes into an
//               operation that no longer exists.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#include "spanio/detail/platform_proactor.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <vector>

using namespace spanio::detail;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::local_address;
using spanio::test::make_loopback_listener;
using spanio::test::make_tcp_pair;
using spanio::test::socket_guard;

namespace {

// Number of create/destroy cycles per backend in the lifecycle case
constexpr int kCreateCycles = 10;

// Number of operations posted at once
constexpr int kPostCount = 100;

// Longest wait for operations that are expected to complete
constexpr int kCompleteWaitMs = 2000;

// Windows retries a refused loopback connect for about two seconds
constexpr int kConnectFailWaitMs = 5000;

// Longest time for the large transfer case
constexpr int kLargeTransferWaitMs = 10000;

// How long a case watches the proactor to prove nothing completes
constexpr int kQuietWaitMs = 150;

// Pause that lets loopback data reach the peer's receive buffer
constexpr int kSettleMs = 50;

// An operation that must come back "at once" has to arrive within this time
constexpr long long kImmediateLimitMs = 50;

// Delay before a helper thread acts, so the main thread is surely blocked
constexpr int kHelperDelayMs = 200;

// Lower bound of a wake-up that a helper thread triggers after kHelperDelayMs
constexpr long long kMinWakeMs = kHelperDelayMs / 2;

// Upper bound for being woken by a helper thread
constexpr long long kWakeLimitMs = 2000;

// Wait of a single poll() inside the polling loops
constexpr int kPollSliceMs = 200;

// Size of the large transfer: big enough to force partial sends
constexpr std::size_t kLargeTransferSize = 1024 * 1024;

// Receive chunk size used by the large transfer
constexpr std::size_t kChunkSize = 64 * 1024;

// The large transfer payload repeats byte values 0..kPatternModulo-1; a
// prime period catches misplaced chunks
constexpr std::size_t kPatternModulo = 251;

// Size of the small receive buffers
constexpr std::size_t kSmallBufferSize = 64;

// Number of connections used at the same time in the concurrency case
constexpr int kConcurrentPairs = 32;

// Size of the single-byte transfers in the concurrency case
constexpr std::size_t kOneByte = 1;

// Short message used by the data cases
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// recv/send flags for raw socket calls: none
constexpr int kNoFlags = 0;

// SO_LINGER on/off value that turns lingering on
constexpr int kLingerOn = 1;

// SO_LINGER timeout of zero: closing sends RST instead of FIN
constexpr int kLingerAbortSeconds = 0;

// Name reported by the readiness-emulation backend
constexpr char kAdapterName[] = "reactor-proactor";

// Clock used for all time measurements
using test_clock = std::chrono::steady_clock;

// Create a proactor or fail the case
// Parameter backend: which backend to create
// Return value: the proactor, never null
io_proactor_ptr make_proactor(const backend_factory& backend)
{
    std::error_code ec;
    io_proactor_ptr proactor = backend.m_create(ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(proactor != nullptr);
    return proactor;
}

// Whether a proactor is the readiness-emulation adapter
// Parameter proactor: proactor to inspect
// Return value: true for reactor_proactor
bool is_adapter(const io_proactor& proactor)
{
    return std::strcmp(proactor.name(), kAdapterName) == 0;
}

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Remaining milliseconds until a deadline, never negative
// Parameter deadline: end of the waiting period
// Return value: milliseconds left
int remaining_ms(test_clock::time_point deadline)
{
    const long long left = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - test_clock::now()).count();
    return left > 0 ? static_cast<int>(left) : 0;
}

// A socket registered with a proactor. On destruction it unregisters the
// socket (cancelling its I/O) and then closes it.
class registered_socket
{
public:
    // Take ownership of a socket and register it
    // Parameter proactor: proactor to register with
    // Parameter handle:   open socket; owned from now on
    registered_socket(io_proactor& proactor, native_handle_t handle)
        : m_proactor(proactor)
        , m_socket(handle)
        , m_registered(false)
    {
        SPANIO_REQUIRE(!m_proactor.register_handle(handle));
        m_registered = true;
    }

    // Unregister (if still registered), then close
    ~registered_socket()
    {
        unregister();
    }

    registered_socket(const registered_socket&) = delete;
    registered_socket& operator=(const registered_socket&) = delete;

    // The socket handle
    // Return value: handle, or kInvalidHandle after close()
    native_handle_t get() const
    {
        return m_socket.get();
    }

    // Unregister without closing
    // Return value: result of unregister_handle, or empty if not registered
    std::error_code unregister()
    {
        if (!m_registered)
        {
            return std::error_code();
        }
        m_registered = false;
        return m_proactor.unregister_handle(m_socket.get());
    }

    // Unregister and close the socket now
    void close()
    {
        unregister();
        m_socket.reset();
    }

private:
    // Proactor the socket is registered with
    io_proactor& m_proactor;
    // Owned socket, closed after unregistering
    socket_guard m_socket;
    // Whether unregister_handle still has to be called
    bool m_registered;
};

// Owning pointer to a registered socket
using registered_ptr = std::unique_ptr<registered_socket>;

// Create a connected loopback pair and register both ends
// Parameter proactor: proactor to register with
// Parameter client:   receives the connecting side
// Parameter server:   receives the accepted side
void make_registered_pair(io_proactor& proactor, registered_ptr& client, registered_ptr& server)
{
    socket_guard raw_client;
    socket_guard raw_server;
    make_tcp_pair(raw_client, raw_server);
    client.reset(new registered_socket(proactor, raw_client.release()));
    server.reset(new registered_socket(proactor, raw_server.release()));
}

// Poll until every expected operation has been returned. Checks that each
// is returned exactly once and that nothing unexpected is returned.
// Parameter proactor:   proactor to poll
// Parameter expected:   operations that must come back
// Parameter timeout_ms: overall time limit
// Return value: true if all came back in time
bool wait_all(io_proactor& proactor, const std::vector<io_operation*>& expected, int timeout_ms)
{
    const std::set<io_operation*> wanted(expected.begin(), expected.end());
    std::map<io_operation*, int> counts;
    const auto deadline = test_clock::now() + std::chrono::milliseconds(timeout_ms);
    completion_list out;
    while (counts.size() < wanted.size())
    {
        std::error_code ec;
        proactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        for (io_operation* op : out)
        {
            SPANIO_CHECK(wanted.count(op) == 1);
            ++counts[op];
        }
        if (remaining_ms(deadline) == 0)
        {
            break;
        }
    }
    for (const auto& entry : counts)
    {
        SPANIO_CHECK(entry.second == 1);
    }
    return counts.size() == wanted.size();
}

// Shorthand for waiting on a single operation
// Parameter proactor:   proactor to poll
// Parameter op:         operation that must come back
// Parameter timeout_ms: overall time limit
// Return value: true if it came back in time
bool wait_one(io_proactor& proactor, io_operation& op, int timeout_ms)
{
    return wait_all(proactor, std::vector<io_operation*>{&op}, timeout_ms);
}

// Poll for a while and count every operation returned
// Parameter proactor: proactor to poll
// Parameter wait_ms:  how long to keep polling
// Return value: number of operations returned in that time
std::size_t count_completions(io_proactor& proactor, int wait_ms)
{
    const auto deadline = test_clock::now() + std::chrono::milliseconds(wait_ms);
    completion_list out;
    std::size_t total = 0;
    do
    {
        std::error_code ec;
        proactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        total += out.size();
    } while (remaining_ms(deadline) > 0);
    return total;
}

// Send a short message with a plain send() call
// Parameter handle: connected socket with room in its send buffer
void raw_send_message(native_handle_t handle)
{
    SPANIO_REQUIRE(::send(handle, kMessage, static_cast<int>(kMessageLength), kNoFlags)
                   == static_cast<int>(kMessageLength));
}

// Whether a buffer starts with kMessage
// Parameter data:   received bytes
// Parameter length: number of received bytes
// Return value: true if exactly kMessage was received
bool is_message(const char* data, std::size_t length)
{
    return length == kMessageLength && std::memcmp(data, kMessage, kMessageLength) == 0;
}

} // namespace

// Case 1: every backend can be created and destroyed repeatedly, and the
// default backend matches the build configuration
SPANIO_TEST(proactor_create_destroy_and_default_backend)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        for (int i = 0; i < kCreateCycles; ++i)
        {
            io_proactor_ptr proactor = make_proactor(backend);
            SPANIO_CHECK(std::strlen(proactor->name()) > 0);
        }
    }

    std::error_code ec;
    io_proactor_ptr default_proactor = make_platform_proactor(ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(default_proactor != nullptr);
#if defined(SPANIO_PLATFORM_WINDOWS) && !defined(SPANIO_WINDOWS_USE_AFD_REACTOR)
    SPANIO_CHECK(std::strcmp(default_proactor->name(), "iocp-proactor") == 0);
#else
    SPANIO_CHECK(is_adapter(*default_proactor));
#endif
}

// Case 2: posted operations come back exactly once with an empty error, and
// a post from another thread wakes a blocked poll()
SPANIO_TEST(proactor_post_returns_each_operation_once)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::vector<io_operation> ops(kPostCount);
        io_operation late_op{};
        io_proactor_ptr proactor = make_proactor(backend);

        std::vector<io_operation*> expected;
        for (io_operation& op : ops)
        {
            prepare_post(op);
            proactor->post(&op);
            expected.push_back(&op);
        }
        SPANIO_CHECK(wait_all(*proactor, expected, kCompleteWaitMs));
        for (const io_operation& op : ops)
        {
            SPANIO_CHECK(!op.m_ec);
        }

        io_proactor& shared = *proactor;
        prepare_post(late_op);
        std::thread helper([&shared, &late_op]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
            shared.post(&late_op);
        });
        const auto start = test_clock::now();
        const bool woken = wait_one(*proactor, late_op, kCompleteWaitMs);
        const long long waited = elapsed_ms(start);
        helper.join();
        SPANIO_CHECK(woken);
        SPANIO_CHECK(waited >= kMinWakeMs);
        SPANIO_CHECK(waited < kWakeLimitMs);
    }
}

// Case 3: accept and connect both succeed; the accepted socket is usable
// and its peer is the client
SPANIO_TEST(proactor_accept_and_connect)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        io_operation accept_op{};
        io_operation connect_op{};
        io_proactor_ptr proactor = make_proactor(backend);

        registered_socket listener(*proactor, make_loopback_listener());
        std::error_code ec;
        registered_socket client(*proactor, create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);

        const sockaddr_in server_addr = local_address(listener.get());
        prepare_accept(accept_op, listener.get());
        proactor->start(&accept_op);
        prepare_connect(connect_op, client.get(), reinterpret_cast<const sockaddr*>(&server_addr),
                        static_cast<socklen_type>(sizeof(server_addr)));
        proactor->start(&connect_op);

        SPANIO_REQUIRE(wait_all(*proactor, {&accept_op, &connect_op}, kCompleteWaitMs));
        SPANIO_CHECK(!accept_op.m_ec);
        SPANIO_CHECK(!connect_op.m_ec);
        socket_guard accepted(accept_op.m_accepted);
        SPANIO_REQUIRE(accepted.get() != kInvalidHandle);

        sockaddr_in peer{};
        socklen_type peer_len = static_cast<socklen_type>(sizeof(peer));
        SPANIO_REQUIRE(::getpeername(accepted.get(), reinterpret_cast<sockaddr*>(&peer), &peer_len)
                       != kSocketCallFailed);
        SPANIO_CHECK(peer.sin_port == local_address(client.get()).sin_port);
    }
}

// Case 4: a receive started before data arrives waits, then completes
// together with the send that delivers the data
SPANIO_TEST(proactor_recv_waits_for_data)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_operation send_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);
        SPANIO_CHECK(count_completions(*proactor, kQuietWaitMs) == 0);

        prepare_send(send_op, client->get(), kMessage, kMessageLength);
        proactor->start(&send_op);
        SPANIO_REQUIRE(wait_all(*proactor, {&recv_op, &send_op}, kCompleteWaitMs));
        SPANIO_CHECK(!send_op.m_ec);
        SPANIO_CHECK(send_op.m_bytes == kMessageLength);
        SPANIO_CHECK(!recv_op.m_ec);
        SPANIO_CHECK(is_message(buffer, recv_op.m_bytes));
    }
}

// Case 5: a receive started when data is already waiting comes back at once
SPANIO_TEST(proactor_recv_with_data_already_waiting)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        raw_send_message(client->get());
        std::this_thread::sleep_for(std::chrono::milliseconds(kSettleMs));

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        const auto start = test_clock::now();
        proactor->start(&recv_op);
        SPANIO_REQUIRE(wait_one(*proactor, recv_op, kCompleteWaitMs));
        SPANIO_CHECK(elapsed_ms(start) < kImmediateLimitMs);
        SPANIO_CHECK(!recv_op.m_ec);
        SPANIO_CHECK(is_message(buffer, recv_op.m_bytes));
    }
}

// Case 6: 1 MB goes through intact. The sender continues after partial
// sends; the receiver collects chunks until everything arrived.
SPANIO_TEST(proactor_large_transfer_with_partial_sends)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::vector<char> payload(kLargeTransferSize);
        for (std::size_t i = 0; i < payload.size(); ++i)
        {
            payload[i] = static_cast<char>(i % kPatternModulo);
        }
        std::vector<char> chunk(kChunkSize);
        std::vector<char> received;
        received.reserve(kLargeTransferSize);
        io_operation send_op{};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        std::size_t sent = 0;
        prepare_send(send_op, client->get(), payload.data(), payload.size());
        proactor->start(&send_op);
        prepare_recv(recv_op, server->get(), chunk.data(), chunk.size());
        proactor->start(&recv_op);

        bool send_active = true;
        bool recv_active = true;
        const auto deadline = test_clock::now() + std::chrono::milliseconds(kLargeTransferWaitMs);
        completion_list out;
        while ((send_active || recv_active) && remaining_ms(deadline) > 0)
        {
            std::error_code ec;
            proactor->poll(out, kPollSliceMs, ec);
            SPANIO_REQUIRE(!ec);
            for (io_operation* op : out)
            {
                if (op == &send_op)
                {
                    SPANIO_REQUIRE(!send_op.m_ec);
                    SPANIO_REQUIRE(send_op.m_bytes > 0);
                    SPANIO_REQUIRE(send_op.m_bytes <= payload.size() - sent);
                    sent += send_op.m_bytes;
                    send_active = sent < payload.size();
                    if (send_active)
                    {
                        prepare_send(send_op, client->get(), payload.data() + sent, payload.size() - sent);
                        proactor->start(&send_op);
                    }
                }
                else if (op == &recv_op)
                {
                    SPANIO_REQUIRE(!recv_op.m_ec);
                    // 0 bytes would be an unexpected end of stream
                    SPANIO_REQUIRE(recv_op.m_bytes > 0);
                    received.insert(received.end(), chunk.begin(),
                                    chunk.begin() + static_cast<std::ptrdiff_t>(recv_op.m_bytes));
                    recv_active = received.size() < payload.size();
                    if (recv_active)
                    {
                        prepare_recv(recv_op, server->get(), chunk.data(), chunk.size());
                        proactor->start(&recv_op);
                    }
                }
            }
        }
        SPANIO_CHECK(!send_active);
        SPANIO_CHECK(!recv_active);
        SPANIO_CHECK(sent == payload.size());
        SPANIO_CHECK(received == payload);
    }
}

// Case 7: a graceful close by the peer completes a pending receive with
// 0 bytes and no error
SPANIO_TEST(proactor_peer_close_gives_end_of_stream)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);
        client->close();

        SPANIO_REQUIRE(wait_one(*proactor, recv_op, kCompleteWaitMs));
        SPANIO_CHECK(!recv_op.m_ec);
        SPANIO_CHECK(recv_op.m_bytes == 0);
    }
}

// Case 8: an abortive close (RST) by the peer fails a pending receive with
// connection_reset
SPANIO_TEST(proactor_peer_reset_gives_connection_reset)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);

        linger abort_linger{};
        abort_linger.l_onoff = static_cast<decltype(abort_linger.l_onoff)>(kLingerOn);
        abort_linger.l_linger = static_cast<decltype(abort_linger.l_linger)>(kLingerAbortSeconds);
        SPANIO_REQUIRE(::setsockopt(client->get(), SOL_SOCKET, SO_LINGER,
                                    reinterpret_cast<const char*>(&abort_linger),
                                    static_cast<socklen_type>(sizeof(abort_linger)))
                       != kSocketCallFailed);
        client->close();

        SPANIO_REQUIRE(wait_one(*proactor, recv_op, kCompleteWaitMs));
        SPANIO_CHECK(recv_op.m_ec == std::errc::connection_reset);
    }
}

// Case 9: connecting to a port nobody listens on fails with
// connection_refused
SPANIO_TEST(proactor_connect_refused)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        io_operation connect_op{};
        io_proactor_ptr proactor = make_proactor(backend);

        // Learn a free port by binding a listener, then close it
        sockaddr_in closed_addr{};
        {
            socket_guard listener(make_loopback_listener());
            closed_addr = local_address(listener.get());
        }
        std::error_code ec;
        registered_socket client(*proactor, create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);

        prepare_connect(connect_op, client.get(), reinterpret_cast<const sockaddr*>(&closed_addr),
                        static_cast<socklen_type>(sizeof(closed_addr)));
        proactor->start(&connect_op);
        SPANIO_REQUIRE(wait_one(*proactor, connect_op, kConnectFailWaitMs));
        SPANIO_CHECK(connect_op.m_ec == std::errc::connection_refused);
    }
}

// Case 10: cancel() returns pending receive and accept operations once with
// operation_canceled; cancelling an idle socket does nothing
SPANIO_TEST(proactor_cancel_pending_operations)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_operation accept_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);
        registered_socket listener(*proactor, make_loopback_listener());

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);
        prepare_accept(accept_op, listener.get());
        proactor->start(&accept_op);
        SPANIO_CHECK(count_completions(*proactor, kSettleMs) == 0);

        proactor->cancel(server->get());
        proactor->cancel(listener.get());
        SPANIO_REQUIRE(wait_all(*proactor, {&recv_op, &accept_op}, kCompleteWaitMs));
        SPANIO_CHECK(recv_op.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(accept_op.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(accept_op.m_accepted == kInvalidHandle);

        // Nothing is pending on the client: no completion may appear
        proactor->cancel(client->get());
        SPANIO_CHECK(count_completions(*proactor, kQuietWaitMs) == 0);
    }
}

// Case 11: unregister_handle() returns pending operations as cancelled
SPANIO_TEST(proactor_unregister_cancels_pending_operations)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        prepare_recv(recv_op, server->get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);
        SPANIO_CHECK(!server->unregister());

        SPANIO_REQUIRE(wait_one(*proactor, recv_op, kCompleteWaitMs));
        SPANIO_CHECK(recv_op.m_ec == std::errc::operation_canceled);
    }
}

// Case 12: a receive and a send outstanding on the same socket at the same
// time do not interfere (full duplex on both ends)
SPANIO_TEST(proactor_full_duplex_on_one_socket)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char client_buffer[kSmallBufferSize] = {};
        char server_buffer[kSmallBufferSize] = {};
        io_operation client_recv{};
        io_operation client_send{};
        io_operation server_recv{};
        io_operation server_send{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        prepare_recv(client_recv, client->get(), client_buffer, sizeof(client_buffer));
        proactor->start(&client_recv);
        prepare_recv(server_recv, server->get(), server_buffer, sizeof(server_buffer));
        proactor->start(&server_recv);
        prepare_send(client_send, client->get(), kMessage, kMessageLength);
        proactor->start(&client_send);
        prepare_send(server_send, server->get(), kMessage, kMessageLength);
        proactor->start(&server_send);

        SPANIO_REQUIRE(wait_all(*proactor, {&client_recv, &client_send, &server_recv, &server_send},
                                kCompleteWaitMs));
        SPANIO_CHECK(!client_send.m_ec);
        SPANIO_CHECK(!server_send.m_ec);
        SPANIO_CHECK(!client_recv.m_ec);
        SPANIO_CHECK(!server_recv.m_ec);
        SPANIO_CHECK(is_message(client_buffer, client_recv.m_bytes));
        SPANIO_CHECK(is_message(server_buffer, server_recv.m_bytes));
    }
}

// Case 13: starting an operation on an unregistered socket reports the
// error through poll(), not synchronously
SPANIO_TEST(proactor_start_on_unregistered_socket_fails_through_poll)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        // Unconnected and unregistered: the native backend fails at once
        // (WSAENOTCONN) instead of waiting on a port it is not bound to
        std::error_code ec;
        socket_guard stray(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);

        prepare_recv(recv_op, stray.get(), buffer, sizeof(buffer));
        proactor->start(&recv_op);
        SPANIO_REQUIRE(wait_one(*proactor, recv_op, kCompleteWaitMs));
        SPANIO_CHECK(static_cast<bool>(recv_op.m_ec));
        if (is_adapter(*proactor))
        {
            SPANIO_CHECK(recv_op.m_ec == std::errc::bad_file_descriptor);
        }
    }
}

// Case 14: an operation started from another thread while poll() is
// blocked wakes that poll()
SPANIO_TEST(proactor_cross_thread_start_wakes_blocked_poll)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        char buffer[kSmallBufferSize] = {};
        io_operation recv_op{};
        io_proactor_ptr proactor = make_proactor(backend);
        registered_ptr client;
        registered_ptr server;
        make_registered_pair(*proactor, client, server);

        io_proactor& shared = *proactor;
        const native_handle_t server_handle = server->get();
        const native_handle_t client_handle = client->get();
        std::thread helper([&shared, &recv_op, &buffer, server_handle, client_handle]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
            prepare_recv(recv_op, server_handle, buffer, sizeof(buffer));
            shared.start(&recv_op);
            // The check macros are not thread-safe; a failed send shows up
            // as a missing or wrong completion in the main thread
            ::send(client_handle, kMessage, static_cast<int>(kMessageLength), kNoFlags);
        });
        const auto start = test_clock::now();
        const bool completed = wait_one(*proactor, recv_op, kCompleteWaitMs);
        const long long waited = elapsed_ms(start);
        helper.join();

        SPANIO_CHECK(completed);
        SPANIO_CHECK(waited < kWakeLimitMs);
        SPANIO_CHECK(!recv_op.m_ec);
        SPANIO_CHECK(is_message(buffer, recv_op.m_bytes));
    }
}

// Case 15: interrupt() wakes a poll() that waits without a timeout; it
// returns no operations
SPANIO_TEST(proactor_interrupt_wakes_blocked_poll)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        io_proactor_ptr proactor = make_proactor(backend);

        io_proactor& shared = *proactor;
        std::thread helper([&shared]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
            shared.interrupt();
        });
        completion_list out;
        std::error_code ec;
        const auto start = test_clock::now();
        proactor->poll(out, kInfiniteTimeout, ec);
        const long long waited = elapsed_ms(start);
        helper.join();

        SPANIO_CHECK(!ec);
        SPANIO_CHECK(out.empty());
        SPANIO_CHECK(waited >= kMinWakeMs);
        SPANIO_CHECK(waited < kWakeLimitMs);
    }
}

// Case 16: many connections transferring at the same time all complete,
// each operation exactly once, with the right data
SPANIO_TEST(proactor_many_concurrent_connections)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        // Each pair sends one distinct byte so mixed-up buffers are caught
        std::vector<char> outgoing(kConcurrentPairs);
        std::vector<char> incoming(kConcurrentPairs);
        std::vector<io_operation> send_ops(kConcurrentPairs);
        std::vector<io_operation> recv_ops(kConcurrentPairs);
        io_proactor_ptr proactor = make_proactor(backend);
        std::vector<registered_ptr> clients(kConcurrentPairs);
        std::vector<registered_ptr> servers(kConcurrentPairs);

        std::vector<io_operation*> expected;
        for (int i = 0; i < kConcurrentPairs; ++i)
        {
            make_registered_pair(*proactor, clients[i], servers[i]);
            outgoing[i] = static_cast<char>(i);
            prepare_recv(recv_ops[i], servers[i]->get(), &incoming[i], kOneByte);
            proactor->start(&recv_ops[i]);
            prepare_send(send_ops[i], clients[i]->get(), &outgoing[i], kOneByte);
            proactor->start(&send_ops[i]);
            expected.push_back(&recv_ops[i]);
            expected.push_back(&send_ops[i]);
        }

        SPANIO_REQUIRE(wait_all(*proactor, expected, kCompleteWaitMs));
        for (int i = 0; i < kConcurrentPairs; ++i)
        {
            SPANIO_CHECK(!send_ops[i].m_ec);
            SPANIO_CHECK(!recv_ops[i].m_ec);
            SPANIO_CHECK(recv_ops[i].m_bytes == kOneByte);
            SPANIO_CHECK(incoming[i] == outgoing[i]);
        }
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
