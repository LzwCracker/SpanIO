// ============================================================================
// File purpose: Unit tests for the io_reactor contract (io_reactor.h). Every
//               case goes through make_platform_reactor(), so the same suite
//               tests the IOCP + AFD backend on Windows and the epoll backend
//               on Linux. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_io_reactor executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_io_reactor interrupt`.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#include "spanio/detail/platform_reactor.h"
#include "test_framework.h"
#include "test_net_helpers.h"

#include <chrono>
#include <cstring>
#include <memory>
#include <set>
#include <thread>
#include <vector>

using namespace spanio::detail;
using spanio::test::connect_to;
using spanio::test::local_address;
using spanio::test::make_loopback_listener;
using spanio::test::make_tcp_pair;
using spanio::test::socket_guard;

namespace {

// Number of create/destroy cycles in the lifecycle case
constexpr int kCreateCycles = 10;

// Longest time a case waits for an event that is expected to arrive
constexpr int kEventWaitMs = 2000;

// Windows retries a refused loopback connect for about two seconds before
// reporting the failure, so this case gets a longer limit
constexpr int kConnectFailWaitMs = 5000;

// How long a case watches a socket to prove that it reports nothing
constexpr int kQuietWaitMs = 150;

// A poll() that must time out has to wait at least this long; half of
// kQuietWaitMs absorbs timer granularity
constexpr long long kMinTimedWaitMs = kQuietWaitMs / 2;

// A poll() that must return at once has to finish within this time
constexpr long long kImmediateLimitMs = 50;

// Delay before a helper thread acts, so the main thread is surely blocked
constexpr int kHelperDelayMs = 200;

// Upper bound for being woken by a helper thread after kHelperDelayMs
constexpr long long kWakeLimitMs = 2000;

// Number of interrupt() calls that must collapse into a single wake-up
constexpr int kRepeatedInterrupts = 3;

// Upper bound for destroying a reactor that still has requests in flight
constexpr long long kDestroyLimitMs = 1000;

// Number of idle sockets registered when the reactor is destroyed
constexpr int kSocketsAtDestroy = 8;

// Number of connections that become readable at the same time
constexpr int kManyPairs = 64;

// Size of the single-byte payloads sent by the cases
constexpr int kOneByte = 1;

// Payload byte value; any value works
constexpr char kPayloadByte = 'x';

// recv/send flags: none
constexpr int kNoFlags = 0;

// SO_LINGER on/off value that turns lingering on
constexpr int kLingerOn = 1;

// SO_LINGER timeout of zero: closesocket sends RST instead of FIN
constexpr int kLingerAbortSeconds = 0;

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(std::chrono::steady_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
}

// Create the platform reactor or fail the case
// Return value: the reactor, never null
io_reactor_ptr make_reactor()
{
    std::error_code ec;
    io_reactor_ptr reactor = make_platform_reactor(ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(reactor != nullptr);
    return reactor;
}

// Combine every event reported for one socket in a poll() result
// Parameter table:  poll() output
// Parameter handle: socket to look for
// Return value: OR of its events, kEventNone if it is absent
event_mask events_of(const event_table& table, descriptor_t handle)
{
    event_mask events = kEventNone;
    for (const ready_event& entry : table)
    {
        if (entry.m_handle == handle)
        {
            events |= entry.m_events;
        }
    }
    return events;
}

// Remaining milliseconds until a deadline, never negative
// Parameter deadline: end of the waiting period
// Return value: milliseconds left
int remaining_ms(std::chrono::steady_clock::time_point deadline)
{
    const long long left = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now()).count();
    return left > 0 ? static_cast<int>(left) : 0;
}

// Poll until a socket reports any of the wanted events or time runs out
// Parameter reactor:    reactor to poll
// Parameter handle:     socket to wait for
// Parameter wanted:     event bits of interest
// Parameter timeout_ms: overall time limit
// Return value: all events of the poll() call that matched, or kEventNone
//               on timeout
event_mask poll_until(io_reactor& reactor, descriptor_t handle, event_mask wanted, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    event_table out;
    for (;;)
    {
        std::error_code ec;
        reactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        const event_mask events = events_of(out, handle);
        if ((events & wanted) != 0)
        {
            return events;
        }
        if (remaining_ms(deadline) == 0)
        {
            return kEventNone;
        }
    }
}

// Poll for a while and collect everything reported for one socket
// Parameter reactor: reactor to poll
// Parameter handle:  socket to watch
// Parameter wait_ms: how long to keep polling
// Return value: OR of all events reported for the socket
event_mask collect_for(io_reactor& reactor, descriptor_t handle, int wait_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_ms);
    event_table out;
    event_mask events = kEventNone;
    do
    {
        std::error_code ec;
        reactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        events |= events_of(out, handle);
    } while (remaining_ms(deadline) > 0);
    return events;
}

// Send one byte or fail the case
// Parameter handle: connected socket
void send_byte(native_handle_t handle)
{
    const char payload = kPayloadByte;
    SPANIO_REQUIRE(::send(handle, &payload, kOneByte, kNoFlags) == kOneByte);
}

// Receive up to one byte
// Parameter handle: connected socket
// Return value: raw result of ::recv (1, 0 on EOF, or kSocketCallFailed)
long long recv_byte(native_handle_t handle)
{
    char payload = 0;
    return ::recv(handle, &payload, kOneByte, kNoFlags);
}

} // namespace

// Case 1: the reactor can be created and destroyed repeatedly
SPANIO_TEST(reactor_create_and_destroy_repeatedly)
{
    for (int i = 0; i < kCreateCycles; ++i)
    {
        io_reactor_ptr reactor = make_reactor();
        SPANIO_CHECK(reactor->name() != nullptr);
        SPANIO_CHECK(std::strlen(reactor->name()) > 0);
    }
}

// Case 2: add/mod/del reject bad arguments with the documented errors
SPANIO_TEST(reactor_rejects_bad_arguments)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);

    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));
    SPANIO_CHECK(reactor->add(server.get(), kEventRead) == std::errc::file_exists);
    SPANIO_CHECK(reactor->add(client.get(), kEventError) == std::errc::invalid_argument);
    SPANIO_CHECK(reactor->mod(server.get(), kEventHangup) == std::errc::invalid_argument);

    // client is a real socket that was never registered
    SPANIO_CHECK(reactor->mod(client.get(), kEventRead) == std::errc::no_such_file_or_directory);
    SPANIO_CHECK(reactor->del(client.get()) == std::errc::no_such_file_or_directory);
    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 3: without events poll() waits for the timeout; poll(0) returns at once
SPANIO_TEST(reactor_poll_honours_timeout)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    event_table out;
    std::error_code ec;
    auto start = std::chrono::steady_clock::now();
    reactor->poll(out, kQuietWaitMs, ec);
    const long long waited = elapsed_ms(start);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(out.empty());
    SPANIO_CHECK(waited >= kMinTimedWaitMs);

    start = std::chrono::steady_clock::now();
    reactor->poll(out, 0, ec);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(out.empty());
    SPANIO_CHECK(elapsed_ms(start) < kImmediateLimitMs);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 4: a listener with a pending connection is readable on every poll()
// until the connection is accepted
SPANIO_TEST(reactor_listener_readable_until_accepted)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard listener(make_loopback_listener());
    SPANIO_REQUIRE(!reactor->add(listener.get(), kEventRead));

    std::error_code ec;
    socket_guard client(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(connect_to(client.get(), local_address(listener.get())) != kSocketCallFailed);

    SPANIO_CHECK((poll_until(*reactor, listener.get(), kEventRead, kEventWaitMs) & kEventRead) != 0);
    // Level-triggered: still reported because nothing was accepted
    SPANIO_CHECK((poll_until(*reactor, listener.get(), kEventRead, kEventWaitMs) & kEventRead) != 0);

    socket_guard server(::accept(listener.get(), nullptr, nullptr));
    SPANIO_REQUIRE(server.get() != kInvalidHandle);
    SPANIO_CHECK(collect_for(*reactor, listener.get(), kQuietWaitMs) == kEventNone);

    SPANIO_CHECK(!reactor->del(listener.get()));
}

// Case 5: received data is reported on every poll() until it is read
SPANIO_TEST(reactor_data_readable_until_consumed)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    send_byte(client.get());
    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) == kEventRead);
    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) == kEventRead);

    SPANIO_REQUIRE(recv_byte(server.get()) == kOneByte);
    SPANIO_CHECK(collect_for(*reactor, server.get(), kQuietWaitMs) == kEventNone);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 6: write interest reports writability; removing it stops the reports
SPANIO_TEST(reactor_write_interest_follows_mod)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventWrite));

    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventWrite, kEventWaitMs) == kEventWrite);

    SPANIO_REQUIRE(!reactor->mod(server.get(), kEventRead));
    SPANIO_CHECK((collect_for(*reactor, server.get(), kQuietWaitMs) & kEventWrite) == 0);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 7: an empty interest suppresses events even with unread data
SPANIO_TEST(reactor_empty_interest_reports_nothing)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    send_byte(client.get());
    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) == kEventRead);

    SPANIO_REQUIRE(!reactor->mod(server.get(), kEventNone));
    SPANIO_CHECK(collect_for(*reactor, server.get(), kQuietWaitMs) == kEventNone);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 8: a graceful close by the peer makes the socket readable with EOF
SPANIO_TEST(reactor_peer_close_reports_readable_eof)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    client.reset();
    SPANIO_CHECK((poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) & kEventRead) != 0);
    SPANIO_CHECK(recv_byte(server.get()) == 0);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 9: an abortive close (RST) by the peer reports Hangup or Error. The
// exact bits differ between platforms, so either one is accepted.
SPANIO_TEST(reactor_peer_reset_reports_hangup_or_error)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    linger abort_linger{};
    abort_linger.l_onoff = static_cast<decltype(abort_linger.l_onoff)>(kLingerOn);
    abort_linger.l_linger = static_cast<decltype(abort_linger.l_linger)>(kLingerAbortSeconds);
    SPANIO_REQUIRE(::setsockopt(client.get(), SOL_SOCKET, SO_LINGER,
                                reinterpret_cast<const char*>(&abort_linger),
                                static_cast<socklen_type>(sizeof(abort_linger)))
                   != kSocketCallFailed);
    client.reset();

    const event_mask events =
        poll_until(*reactor, server.get(), kEventHangup | kEventError, kEventWaitMs);
    SPANIO_CHECK((events & (kEventHangup | kEventError)) != 0);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 10: a non-blocking connect to a closed port reports Error
SPANIO_TEST(reactor_failed_connect_reports_error)
{
    io_reactor_ptr reactor = make_reactor();

    // Learn a free port by binding a listener, then close it so nothing
    // listens there any more
    sockaddr_in closed_addr{};
    {
        socket_guard listener(make_loopback_listener());
        closed_addr = local_address(listener.get());
    }

    std::error_code ec;
    socket_guard client(create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(!set_nonblocking(client.get(), true));
    const int result = connect_to(client.get(), closed_addr);
    const std::error_code connect_ec = last_error();
    SPANIO_REQUIRE(result == kSocketCallFailed);
    SPANIO_REQUIRE(connect_ec == std::errc::operation_would_block
                   || connect_ec == std::errc::operation_in_progress);

    SPANIO_REQUIRE(!reactor->add(client.get(), kEventWrite));
    const event_mask events = poll_until(*reactor, client.get(), kEventError, kConnectFailWaitMs);
    SPANIO_CHECK((events & kEventError) != 0);

    SPANIO_CHECK(!reactor->del(client.get()));
}

// Case 11: interrupt() wakes a poll() blocked without timeout, and several
// interrupts collapse into one wake-up
SPANIO_TEST(reactor_interrupt_wakes_blocked_poll)
{
    io_reactor_ptr reactor = make_reactor();
    io_reactor& shared = *reactor;

    std::thread helper([&shared]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
        shared.interrupt();
    });
    event_table out;
    std::error_code ec;
    auto start = std::chrono::steady_clock::now();
    reactor->poll(out, kInfiniteTimeout, ec);
    const long long woken_after = elapsed_ms(start);
    helper.join();
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(woken_after >= kMinTimedWaitMs);
    SPANIO_CHECK(woken_after < kWakeLimitMs);

    for (int i = 0; i < kRepeatedInterrupts; ++i)
    {
        reactor->interrupt();
    }
    start = std::chrono::steady_clock::now();
    reactor->poll(out, kEventWaitMs, ec);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(elapsed_ms(start) < kImmediateLimitMs);

    // The extra interrupts must not leave further wake-ups behind
    start = std::chrono::steady_clock::now();
    reactor->poll(out, kQuietWaitMs, ec);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(elapsed_ms(start) >= kMinTimedWaitMs);
}

// Case 12: mod() from another thread takes effect for a poll() that is
// already blocked
SPANIO_TEST(reactor_cross_thread_mod_wakes_blocked_poll)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventNone));

    io_reactor& shared = *reactor;
    const descriptor_t target = server.get();
    // Written by the helper, read after join(); the check macros are not
    // thread-safe, so the helper never calls them
    std::error_code mod_ec;
    std::thread helper([&shared, &mod_ec, target]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kHelperDelayMs));
        mod_ec = shared.mod(target, kEventWrite);
    });
    const auto start = std::chrono::steady_clock::now();
    const event_mask events = poll_until(*reactor, server.get(), kEventWrite, kEventWaitMs);
    const long long woken_after = elapsed_ms(start);
    helper.join();

    SPANIO_CHECK(!mod_ec);
    SPANIO_CHECK(events == kEventWrite);
    SPANIO_CHECK(woken_after < kWakeLimitMs);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 13: after del() a socket is no longer reported, and it can be added
// again
SPANIO_TEST(reactor_del_stops_reports_and_allows_re_add)
{
    io_reactor_ptr reactor = make_reactor();
    socket_guard client;
    socket_guard server;
    make_tcp_pair(client, server);
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));

    send_byte(client.get());
    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) == kEventRead);

    SPANIO_REQUIRE(!reactor->del(server.get()));
    SPANIO_CHECK(collect_for(*reactor, server.get(), kQuietWaitMs) == kEventNone);

    // The byte is still unread, so the re-added socket is readable again
    SPANIO_REQUIRE(!reactor->add(server.get(), kEventRead));
    SPANIO_CHECK(poll_until(*reactor, server.get(), kEventRead, kEventWaitMs) == kEventRead);

    SPANIO_CHECK(!reactor->del(server.get()));
}

// Case 14: destroying a reactor with sockets still registered (requests in
// flight on Windows) returns promptly
SPANIO_TEST(reactor_destroy_with_registered_sockets)
{
    io_reactor_ptr reactor = make_reactor();
    std::vector<std::unique_ptr<socket_guard>> sockets;
    for (int i = 0; i < kSocketsAtDestroy; ++i)
    {
        std::unique_ptr<socket_guard> client(new socket_guard());
        std::unique_ptr<socket_guard> server(new socket_guard());
        make_tcp_pair(*client, *server);
        SPANIO_REQUIRE(!reactor->add(server->get(), kEventRead));
        sockets.push_back(std::move(client));
        sockets.push_back(std::move(server));
    }

    event_table out;
    std::error_code ec;
    reactor->poll(out, 0, ec);
    SPANIO_CHECK(!ec);

    const auto start = std::chrono::steady_clock::now();
    reactor.reset();
    SPANIO_CHECK(elapsed_ms(start) < kDestroyLimitMs);
}

// Case 15: many sockets readable at once are all reported
SPANIO_TEST(reactor_reports_many_ready_sockets)
{
    io_reactor_ptr reactor = make_reactor();
    std::vector<std::unique_ptr<socket_guard>> clients;
    std::vector<std::unique_ptr<socket_guard>> servers;
    std::set<descriptor_t> expected;
    for (int i = 0; i < kManyPairs; ++i)
    {
        std::unique_ptr<socket_guard> client(new socket_guard());
        std::unique_ptr<socket_guard> server(new socket_guard());
        make_tcp_pair(*client, *server);
        SPANIO_REQUIRE(!reactor->add(server->get(), kEventRead));
        expected.insert(server->get());
        clients.push_back(std::move(client));
        servers.push_back(std::move(server));
    }
    for (const std::unique_ptr<socket_guard>& client : clients)
    {
        send_byte(client->get());
    }

    std::set<descriptor_t> reported;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventWaitMs);
    event_table out;
    while (reported.size() < expected.size() && remaining_ms(deadline) > 0)
    {
        std::error_code ec;
        reactor->poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        for (const ready_event& entry : out)
        {
            if ((entry.m_events & kEventRead) != 0)
            {
                reported.insert(entry.m_handle);
            }
        }
    }
    SPANIO_CHECK(reported == expected);

    for (const std::unique_ptr<socket_guard>& server : servers)
    {
        SPANIO_CHECK(!reactor->del(server->get()));
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
