// ============================================================================
// File purpose: Unit tests for udp::socket (udp.h) and the datagram
//               operations of io_proactor. Cases run once per proactor
//               backend of the platform, except the move case, which uses the
//               default backend. All traffic stays on the loopback interface.
// Usage:        Built as the test_udp executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_udp truncated`.
//
//               In every case the io_context is declared before the sockets,
//               so they are closed (cancelling their I/O) before the context
//               is destroyed, and buffers are declared before the sockets
//               that use them.
//
//               UDP may drop datagrams, so these cases send one at a time
//               and wait for each to arrive. The multi-threaded case is lock
//               step: a side sends its next datagram only after it received
//               the peer's previous one.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#include "spanio/udp.h"
#include "test_framework.h"
#include "test_proactor_backends.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <vector>

namespace udp = spanio::udp;
using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;

namespace {

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Size of the receive buffers
constexpr std::size_t kBufferSize = 256;

// Short message used by most cases
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// Pause that lets a loopback datagram (or the ICMP reply to one) arrive
constexpr int kSettleMs = 50;

// Size of the too-small buffer in the truncation case
constexpr std::size_t kSmallBufferSize = 4;

// Size of the datagram in the truncation case
constexpr std::size_t kLongDatagramSize = 32;

// Larger than the 65507-byte UDP payload limit
constexpr std::size_t kOversizedDatagram = 70000;

// Threads calling run() in the multi-threaded case
constexpr int kRunThreads = 4;

// Pairs of sockets in the multi-threaded case
constexpr int kPingPongPairs = 8;

// Rounds each pair exchanges
constexpr int kPingPongRounds = 100;

// Sides of a pair
constexpr int kSidesPerPair = 2;

// A round is over when both its send and its receive have finished
constexpr int kOperationsPerRound = 2;

// Longest time the multi-threaded case may take before it is stopped
constexpr int kPingPongLimitMs = 30000;

// Result of a receive, recorded by a test
struct receive_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // Byte count passed to the handler
    std::size_t m_bytes = 0;
    // Sender passed to the handler
    udp::endpoint m_sender;
};

// Handler that records its arguments in a receive_record
class record_receive
{
public:
    // Parameter record: where to store the result; must outlive the handler
    explicit record_receive(receive_record& record)
        : m_record(&record)
    {
    }

    // Parameter ec:     operation result
    // Parameter bytes:  bytes received
    // Parameter sender: address of the sender
    void operator()(std::error_code ec, std::size_t bytes, const udp::endpoint& sender)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
        m_record->m_bytes = bytes;
        m_record->m_sender = sender;
    }

private:
    // Destination of the recorded values
    receive_record* m_record;
};

// Result of a send, recorded by a test
struct send_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // Byte count passed to the handler
    std::size_t m_bytes = 0;
};

// Handler that records its arguments in a send_record
class record_send
{
public:
    // Parameter record: where to store the result; must outlive the handler
    explicit record_send(send_record& record)
        : m_record(&record)
    {
    }

    // Parameter ec:    operation result
    // Parameter bytes: bytes sent
    void operator()(std::error_code ec, std::size_t bytes)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
        m_record->m_bytes = bytes;
    }

private:
    // Destination of the recorded values
    send_record* m_record;
};

// Sleep long enough for loopback traffic to arrive
void settle()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(kSettleMs));
}

// A socket's bound endpoint, or fail the case
// Parameter sock: bound socket
// Return value: its local endpoint
udp::endpoint bound_endpoint(const udp::socket& sock)
{
    std::error_code ec;
    const udp::endpoint local = sock.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return local;
}

// A loopback endpoint with a system-picked port
// Return value: the endpoint
udp::endpoint loopback_any()
{
    return udp::endpoint(spanio::ipv4::loopback(), kAnyPort);
}

// Run a context until it is out of work, then make it reusable
// Parameter ctx: the context
// Return value: number of handlers run
std::size_t run_and_restart(io_context& ctx)
{
    const std::size_t ran = ctx.run();
    ctx.restart();
    return ran;
}

// One side of a pair in the multi-threaded case. Each round it arms a
// receive, sends its datagram, and moves on once both have finished, so it
// never has two sends or two receives outstanding.
class ping_pong_side
{
public:
    // Parameter ctx:      context to run on
    // Parameter id:       unique number of this side, part of each message
    // Parameter failures: counts wrong or failed datagrams
    // Parameter finished: counts sides that completed every round
    ping_pong_side(io_context& ctx, int id, std::atomic<int>& failures, std::atomic<int>& finished)
        : m_socket(ctx, loopback_any())
        , m_id(id)
        , m_peer_id(0)
        , m_round(0)
        , m_pending(0)
        , m_failures(&failures)
        , m_finished(&finished)
        , m_input{}
    {
    }

    // Connect this side to its peer, before start()
    // Parameter peer:    the other side's address
    // Parameter peer_id: the other side's number
    void set_peer(const udp::endpoint& peer, int peer_id)
    {
        m_peer = peer;
        m_peer_id = peer_id;
    }

    // This side's address
    // Return value: the endpoint
    udp::endpoint local() const
    {
        return bound_endpoint(m_socket);
    }

    // Start the next round
    void start()
    {
        m_pending.store(kOperationsPerRound);
        m_output = message(m_id);
        auto received = [this](std::error_code ec, std::size_t bytes, const udp::endpoint& sender)
        {
            on_received(ec, bytes, sender);
        };
        auto sent = [this](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                ++*m_failures;
                return;
            }
            operation_done();
        };
        m_socket.async_receive_from(spanio::buffer(m_input), received);
        m_socket.async_send_to(spanio::buffer(m_output), m_peer, sent);
    }

private:
    // Text a side sends in the current round
    // Parameter side: number of the sending side
    // Return value: the message
    std::string message(int side) const
    {
        return std::to_string(side) + ":" + std::to_string(m_round);
    }

    // Check a received datagram
    // Parameter ec:     receive result
    // Parameter bytes:  bytes received
    // Parameter sender: address of the sender
    void on_received(std::error_code ec, std::size_t bytes, const udp::endpoint& sender)
    {
        if (ec || sender != m_peer || std::string(m_input, bytes) != message(m_peer_id))
        {
            ++*m_failures;
            return;
        }
        operation_done();
    }

    // Finish the round when both its operations are done
    void operation_done()
    {
        if (m_pending.fetch_sub(1) != 1)
        {
            return;
        }
        ++m_round;
        if (m_round < kPingPongRounds)
        {
            start();
        }
        else
        {
            ++*m_finished;
        }
    }

    // The socket
    udp::socket m_socket;
    // Address of the other side
    udp::endpoint m_peer;
    // Number of this side
    int m_id;
    // Number of the other side
    int m_peer_id;
    // Current round, 0-based
    int m_round;
    // Operations of this round still running
    std::atomic<int> m_pending;
    // Counts wrong or failed datagrams
    std::atomic<int>* m_failures;
    // Counts sides that completed every round
    std::atomic<int>* m_finished;
    // Message being sent
    std::string m_output;
    // Receive buffer
    char m_input[kBufferSize];
};

}

// Case 1: a datagram arrives with the right content and size, and the
// handler learns who sent it
SPANIO_TEST(send_and_receive_roundtrip)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());
        receive_record received;
        send_record sent;

        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(receiver),
                             record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);

        SPANIO_CHECK(sent.m_calls == 1);
        SPANIO_CHECK(!sent.m_ec);
        SPANIO_CHECK(sent.m_bytes == kMessageLength);
        SPANIO_CHECK(received.m_calls == 1);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(buffer, kMessage, kMessageLength) == 0);
        SPANIO_CHECK(received.m_sender == bound_endpoint(sender));
    }
}

// Case 2: the sender address given to a handler can be used to reply
SPANIO_TEST(reply_to_sender)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char server_buffer[kBufferSize] = {};
        char client_buffer[kBufferSize] = {};
        udp::socket server(*ctx, loopback_any());
        udp::socket client(*ctx, loopback_any());
        receive_record reply;
        send_record request_sent;
        send_record reply_sent;

        auto on_request = [&](std::error_code ec, std::size_t bytes, const udp::endpoint& sender)
        {
            SPANIO_CHECK(!ec);
            server.async_send_to(spanio::buffer(server_buffer, bytes), sender, record_send(reply_sent));
        };
        server.async_receive_from(spanio::buffer(server_buffer), on_request);
        client.async_receive_from(spanio::buffer(client_buffer), record_receive(reply));
        client.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(server),
                             record_send(request_sent));
        ctx->run();

        SPANIO_CHECK(!request_sent.m_ec);
        SPANIO_CHECK(!reply_sent.m_ec);
        SPANIO_CHECK(!reply.m_ec);
        SPANIO_CHECK(reply.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(client_buffer, kMessage, kMessageLength) == 0);
        SPANIO_CHECK(reply.m_sender == bound_endpoint(server));
    }
}

// Case 3: three datagrams arrive as three receives and never merge
SPANIO_TEST(message_boundaries_preserved)
{
    const std::vector<std::string> messages = {"one", "second", "the third"};
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());
        const udp::endpoint target = bound_endpoint(receiver);

        // All of them wait in the socket's queue before the first receive
        for (const std::string& text : messages)
        {
            send_record sent;
            sender.async_send_to(spanio::buffer(text), target, record_send(sent));
            run_and_restart(*ctx);
            SPANIO_CHECK(!sent.m_ec);
        }
        settle();
        for (const std::string& text : messages)
        {
            receive_record received;
            receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
            run_and_restart(*ctx);
            SPANIO_CHECK(!received.m_ec);
            SPANIO_CHECK(std::string(buffer, received.m_bytes) == text);
        }
    }
}

// Case 4: an empty datagram is a success with 0 bytes, not an end of file,
// and it counts as one datagram
SPANIO_TEST(empty_datagram)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());
        const udp::endpoint target = bound_endpoint(receiver);
        receive_record received;
        send_record sent;

        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::const_buffer(), target, record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        ctx->restart();
        SPANIO_CHECK(!sent.m_ec);
        SPANIO_CHECK(sent.m_bytes == 0);
        SPANIO_CHECK(received.m_calls == 1);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == 0);
        SPANIO_CHECK(received.m_sender == bound_endpoint(sender));

        // An empty receive buffer takes no datagram, so this one stays queued
        receive_record empty_buffer;
        send_record second_sent;
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), target, record_send(second_sent));
        run_and_restart(*ctx);
        settle();
        receiver.async_receive_from(spanio::mutable_buffer(), record_receive(empty_buffer));
        SPANIO_CHECK(empty_buffer.m_calls == 0);
        run_and_restart(*ctx);
        SPANIO_CHECK(empty_buffer.m_calls == 1);
        SPANIO_CHECK(!empty_buffer.m_ec);
        SPANIO_CHECK(empty_buffer.m_bytes == 0);

        receive_record queued;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(queued));
        run_and_restart(*ctx);
        SPANIO_CHECK(queued.m_bytes == kMessageLength);
    }
}

// Case 5: a datagram larger than the buffer reports datagram_truncated
// exactly once, whether it arrived before or after the receive started, and
// the next datagram is unaffected
SPANIO_TEST(truncated_datagram)
{
    const std::string long_datagram(kLongDatagramSize, 'L');
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        char short_buffer[kSmallBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());
        const udp::endpoint target = bound_endpoint(receiver);

        // The datagram is already waiting: the receive finishes at once
        {
            send_record sent;
            receive_record received;
            sender.async_send_to(spanio::buffer(long_datagram), target, record_send(sent));
            run_and_restart(*ctx);
            settle();
            receiver.async_receive_from(spanio::buffer(short_buffer), record_receive(received));
            SPANIO_CHECK(ctx->run() == 1);
            ctx->restart();
            SPANIO_CHECK(received.m_calls == 1);
            SPANIO_CHECK(received.m_ec == spanio::error::misc_errors::datagram_truncated);
            SPANIO_CHECK(received.m_bytes == kSmallBufferSize);
            SPANIO_CHECK(std::memcmp(short_buffer, long_datagram.data(), kSmallBufferSize) == 0);
            SPANIO_CHECK(received.m_sender == bound_endpoint(sender));
        }

        // The receive is already waiting: the datagram completes it
        {
            send_record sent;
            receive_record received;
            receiver.async_receive_from(spanio::buffer(short_buffer), record_receive(received));
            sender.async_send_to(spanio::buffer(long_datagram), target, record_send(sent));
            SPANIO_CHECK(ctx->run() == 2);
            ctx->restart();
            SPANIO_CHECK(received.m_calls == 1);
            SPANIO_CHECK(received.m_ec == spanio::error::misc_errors::datagram_truncated);
            SPANIO_CHECK(received.m_bytes == kSmallBufferSize);
        }

        send_record sent;
        receive_record received;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), target, record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
    }
}

// Case 6: a datagram over the protocol limit fails with message_size, once,
// and the socket keeps working
SPANIO_TEST(oversized_send_fails)
{
    const std::vector<char> oversized(kOversizedDatagram);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());
        const udp::endpoint target = bound_endpoint(receiver);

        send_record failed;
        sender.async_send_to(spanio::buffer(oversized), target, record_send(failed));
        SPANIO_CHECK(failed.m_calls == 0);
        SPANIO_CHECK(ctx->run() == 1);
        ctx->restart();
        SPANIO_CHECK(failed.m_calls == 1);
        SPANIO_CHECK(failed.m_ec == std::errc::message_size);

        send_record sent;
        receive_record received;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), target, record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!sent.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
    }
}

// Case 7: binding port 0 gets a real port; a second socket cannot bind it,
// through either the error_code or the throwing API
SPANIO_TEST(bind_ephemeral_and_in_use)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        udp::socket first(*ctx, loopback_any());
        SPANIO_CHECK(first.is_open());
        const udp::endpoint bound = bound_endpoint(first);
        SPANIO_CHECK(bound.port() != kAnyPort);
        SPANIO_CHECK(bound.address() == spanio::ipv4::loopback());

        udp::socket second(*ctx);
        SPANIO_REQUIRE(!second.open(AF_INET));
        SPANIO_CHECK(second.bind(bound) == std::errc::address_in_use);
        SPANIO_CHECK(second.open(AF_INET) == std::errc::invalid_argument);

        bool thrown = false;
        std::error_code thrown_ec;
        try
        {
            udp::socket third(*ctx, bound);
        }
        catch (const std::system_error& e)
        {
            thrown = true;
            thrown_ec = e.code();
        }
        SPANIO_CHECK(thrown);
        SPANIO_CHECK(thrown_ec == std::errc::address_in_use);
    }
}

// Case 8: IPv6 works the same way; skipped where the loopback ::1 is missing
SPANIO_TEST(ipv6_loopback)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        const udp::endpoint local(spanio::ipv6::loopback(), kAnyPort);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx);
        udp::socket sender(*ctx);
        SPANIO_REQUIRE(!receiver.open(AF_INET6));
        SPANIO_REQUIRE(!sender.open(AF_INET6));
        if (receiver.bind(local) || sender.bind(local))
        {
            std::printf("    skipped: IPv6 loopback not available\n");
            continue;
        }

        receive_record received;
        send_record sent;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(receiver),
                             record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!sent.m_ec);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
        SPANIO_CHECK(received.m_sender.family() == AF_INET6);
        SPANIO_CHECK(received.m_sender == bound_endpoint(sender));
    }
}

// Case 9: sending to a port nobody uses does not make a later receive fail
SPANIO_TEST(closed_peer_port_does_not_break_receive)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        udp::endpoint closed_port;
        {
            // Borrow a free port, then release it so nothing listens there
            udp::socket probe(*ctx, loopback_any());
            closed_port = bound_endpoint(probe);
        }
        char buffer[kBufferSize] = {};
        udp::socket sock(*ctx, loopback_any());
        udp::socket peer(*ctx, loopback_any());

        send_record lost;
        sock.async_send_to(spanio::buffer(kMessage, kMessageLength), closed_port, record_send(lost));
        run_and_restart(*ctx);
        SPANIO_CHECK(!lost.m_ec);
        // The ICMP "port unreachable" reply arrives during this pause
        settle();

        receive_record received;
        send_record sent;
        sock.async_receive_from(spanio::buffer(buffer), record_receive(received));
        peer.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(sock), record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
        SPANIO_CHECK(received.m_sender == bound_endpoint(peer));
    }
}

// Case 10: close() with a pending receive gives operation_canceled, and
// run() returns
SPANIO_TEST(close_cancels_pending_receive)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket sock(*ctx, loopback_any());
        receive_record received;

        sock.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sock.close();
        SPANIO_CHECK(!sock.is_open());
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(received.m_calls == 1);
        SPANIO_CHECK(received.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(received.m_bytes == 0);
    }
}

// Case 11: destroying a socket with a pending receive still runs the
// handler once, cancelled
SPANIO_TEST(destroy_socket_with_pending_receive)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        receive_record received;
        std::unique_ptr<udp::socket> sock(new udp::socket(*ctx, loopback_any()));

        sock->async_receive_from(spanio::buffer(buffer), record_receive(received));
        sock.reset();
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(received.m_calls == 1);
        SPANIO_CHECK(received.m_ec == std::errc::operation_canceled);
    }
}

// Case 12: cancel() ends a pending receive, and the socket can still receive
SPANIO_TEST(cancel_keeps_socket_usable)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket sender(*ctx, loopback_any());

        receive_record canceled;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(canceled));
        receiver.cancel();
        SPANIO_CHECK(ctx->run() == 1);
        ctx->restart();
        SPANIO_CHECK(canceled.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(receiver.is_open());

        receive_record received;
        send_record sent;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        sender.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(receiver),
                             record_send(sent));
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
    }
}

// Case 13: a receive on a socket that is not open fails with
// bad_file_descriptor, through run(), never inline
SPANIO_TEST(op_on_closed_socket_fails_async)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket closed(*ctx);
        receive_record received;

        closed.async_receive_from(spanio::buffer(buffer), record_receive(received));
        SPANIO_CHECK(received.m_calls == 0);
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(received.m_calls == 1);
        SPANIO_CHECK(received.m_ec == std::errc::bad_file_descriptor);
        SPANIO_CHECK(received.m_bytes == 0);
    }
}

// Case 14: sending on a socket that is not open opens it, and it then has a
// local address the peer can reply to
SPANIO_TEST(send_opens_socket_automatically)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        char buffer[kBufferSize] = {};
        udp::socket receiver(*ctx, loopback_any());
        udp::socket client(*ctx);
        SPANIO_CHECK(!client.is_open());

        receive_record received;
        send_record sent;
        receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
        client.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(receiver),
                             record_send(sent));
        SPANIO_CHECK(client.is_open());
        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(!sent.m_ec);
        SPANIO_CHECK(!received.m_ec);
        SPANIO_CHECK(received.m_bytes == kMessageLength);
        SPANIO_CHECK(received.m_sender.port() == bound_endpoint(client).port());
    }
}

// Case 15: moving a socket transfers it; move-assigning onto an open socket
// closes the old one
SPANIO_TEST(socket_move_semantics)
{
    io_context ctx;
    char buffer[kBufferSize] = {};
    udp::socket receiver(ctx, loopback_any());
    udp::socket original(ctx, loopback_any());
    const spanio::detail::native_handle_t handle = original.native_handle();
    const udp::endpoint address = bound_endpoint(original);

    udp::socket moved(std::move(original));
    SPANIO_CHECK(!original.is_open());
    SPANIO_CHECK(moved.native_handle() == handle);

    udp::socket target(ctx, loopback_any());
    const spanio::detail::native_handle_t replaced = target.native_handle();
    target = std::move(moved);
    SPANIO_CHECK(!moved.is_open());
    SPANIO_CHECK(target.native_handle() == handle);
    // The replaced handle was closed by the assignment, so closing it again
    // must fail
    SPANIO_CHECK(static_cast<bool>(spanio::detail::close_socket(replaced)));

    receive_record received;
    send_record sent;
    receiver.async_receive_from(spanio::buffer(buffer), record_receive(received));
    target.async_send_to(spanio::buffer(kMessage, kMessageLength), bound_endpoint(receiver),
                         record_send(sent));
    SPANIO_CHECK(ctx.run() == 2);
    SPANIO_CHECK(!sent.m_ec);
    SPANIO_CHECK(received.m_bytes == kMessageLength);
    SPANIO_CHECK(received.m_sender == address);
}

// Case 16: four run() threads drive eight pairs of sockets through 100
// lock-step rounds; every datagram is right
SPANIO_TEST(multi_thread_ping_pong)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        std::atomic<int> failures(0);
        std::atomic<int> finished(0);

        std::vector<std::unique_ptr<ping_pong_side>> sides;
        for (int id = 0; id < kPingPongPairs * kSidesPerPair; ++id)
        {
            sides.emplace_back(new ping_pong_side(*ctx, id, failures, finished));
        }
        for (std::size_t i = 0; i + 1 < sides.size(); i += kSidesPerPair)
        {
            sides[i]->set_peer(sides[i + 1]->local(), static_cast<int>(i + 1));
            sides[i + 1]->set_peer(sides[i]->local(), static_cast<int>(i));
        }
        for (const std::unique_ptr<ping_pong_side>& side : sides)
        {
            side->start();
        }

        // A lost datagram would leave run() waiting; this stops it instead
        std::mutex done_mutex;
        std::condition_variable done_signal;
        bool done = false;
        io_context& context = *ctx;
        auto stop_when_late = [&]()
        {
            std::unique_lock<std::mutex> lock(done_mutex);
            auto is_done = [&done]()
            {
                return done;
            };
            if (!done_signal.wait_for(lock, std::chrono::milliseconds(kPingPongLimitMs), is_done))
            {
                context.stop();
            }
        };
        std::thread watchdog(stop_when_late);
        auto run_context = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> runners;
        for (int i = 0; i < kRunThreads; ++i)
        {
            runners.emplace_back(run_context);
        }
        for (std::thread& runner : runners)
        {
            runner.join();
        }
        {
            std::lock_guard<std::mutex> lock(done_mutex);
            done = true;
        }
        done_signal.notify_one();
        watchdog.join();

        SPANIO_CHECK(failures.load() == 0);
        SPANIO_CHECK(finished.load() == kPingPongPairs * kSidesPerPair);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
