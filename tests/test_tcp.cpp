// ============================================================================
// File purpose: Unit tests for the TCP API: ip::address, tcp::endpoint,
//               buffer(), spanio::error, tcp::acceptor and tcp::socket.
//               Cases that do I/O run once per proactor backend of the
//               platform (see test_proactor_backends.h); the others use the
//               default backend. All traffic stays on 127.0.0.1.
// Usage:        Built as the test_tcp executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_tcp echo`.
//
//               In every case the io_context is declared before the sockets
//               and acceptors, so they are closed (cancelling their I/O)
//               before the context is destroyed.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/tcp.h"
#include "test_framework.h"
#include "test_proactor_backends.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace tcp = spanio::tcp;
namespace ip = spanio::ip;
using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;

namespace {

// Port used by the endpoint value tests; nothing listens on it
constexpr std::uint16_t kSamplePort = 8080;

// A second port, different from kSamplePort
constexpr std::uint16_t kOtherPort = 8081;

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Size of the char array in the buffer test
constexpr std::size_t kArraySize = 16;

// Size of the string in the buffer test
constexpr std::size_t kStringSize = 10;

// Size of the vector in the buffer test
constexpr std::size_t kVectorSize = 7;

// Size of the raw-memory buffer in the buffer test
constexpr std::size_t kRawSize = 5;

// Size of the small receive buffers
constexpr std::size_t kSmallBufferSize = 64;

// Short message sent by the data cases
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// Size of the large transfer: big enough to need several sends
constexpr std::size_t kLargeTransferSize = 4 * 1024 * 1024;

// Receive chunk size used by the large transfer
constexpr std::size_t kChunkSize = 64 * 1024;

// SO_SNDBUF of the sender in the large transfer; much smaller than the
// payload so sends complete partially
constexpr int kSmallSendBuffer = 8 * 1024;

// Largest send chunking_proactor lets through in one operation
constexpr std::size_t kForcedSendChunk = 4 * 1024;

// Number of full chunks in the forced partial send payload
constexpr std::size_t kForcedFullChunks = 64;

// Bytes after the last full chunk, so the final send is a short one
constexpr std::size_t kForcedRemainder = 123;

// Payload of the forced partial send case
constexpr std::size_t kForcedPartialSize = kForcedFullChunks * kForcedSendChunk + kForcedRemainder;

// Fewest sends async_write needs for that payload: every full chunk plus
// one for the remainder
constexpr int kForcedMinSends = static_cast<int>(kForcedFullChunks + 1);

// Payload bytes repeat with this prime period, so misplaced chunks show up
constexpr std::size_t kPatternModulo = 251;

// Longest time a refused connect may take (Windows retries for ~2 s)
constexpr long long kRefusedLimitMs = 10000;

// Handlers expected in the "operations on closed objects" case
constexpr std::size_t kClosedObjectOps = 4;

// Threads calling run() in the echo case
constexpr int kEchoThreads = 4;

// Connections in the echo case
constexpr int kEchoConnections = 16;

// Messages each echo client sends
constexpr int kEchoRounds = 10;

// Size of each echo message
constexpr std::size_t kEchoMessageSize = 1000;

// Size of the echo server's receive buffer; smaller than a message, so
// messages are echoed in pieces
constexpr std::size_t kEchoChunkSize = 512;

// Multiplier of the connection id in the echo payload pattern
constexpr std::size_t kIdStride = 31;

// Multiplier of the round number in the echo payload pattern
constexpr std::size_t kRoundStride = 7;

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Byte at a position of the large transfer payload
// Parameter index: position in the payload
// Return value: the expected byte
char pattern_byte(std::size_t index)
{
    return static_cast<char>(index % kPatternModulo);
}

// Byte at a position of one echo message
// Parameter id:    connection id
// Parameter round: message number of that connection
// Parameter index: position in the message
// Return value: the expected byte
char echo_byte(int id, int round, std::size_t index)
{
    const std::size_t value = static_cast<std::size_t>(id) * kIdStride
                            + static_cast<std::size_t>(round) * kRoundStride + index;
    return static_cast<char>(value % kPatternModulo);
}

// Listening endpoint of an acceptor, or fail the case
// Parameter acc: open, bound acceptor
// Return value: its local endpoint
tcp::endpoint listening_endpoint(const tcp::acceptor& acc)
{
    std::error_code ec;
    const tcp::endpoint local = acc.local_endpoint(ec);
    SPANIO_REQUIRE(!ec);
    return local;
}

// Connect a client to an acceptor through the async API, running the
// context until both sides are done; restarts the context afterwards
// Parameter ctx:    context of all three objects
// Parameter acc:    listening acceptor
// Parameter client: socket that is not open yet
// Parameter server: receives the accepted socket
void connect_pair(io_context& ctx, tcp::acceptor& acc, tcp::socket& client, tcp::socket& server)
{
    const tcp::endpoint target = listening_endpoint(acc);
    bool accepted = false;
    bool connected = false;
    std::error_code accept_ec;
    std::error_code connect_ec;
    auto on_accept = [&](std::error_code ec, tcp::socket peer)
    {
        accept_ec = ec;
        server = std::move(peer);
        accepted = true;
    };
    auto on_connect = [&](std::error_code ec)
    {
        connect_ec = ec;
        connected = true;
    };
    acc.async_accept(on_accept);
    client.async_connect(target, on_connect);
    ctx.run();
    ctx.restart();
    SPANIO_REQUIRE(accepted);
    SPANIO_REQUIRE(connected);
    SPANIO_REQUIRE(!accept_ec);
    SPANIO_REQUIRE(!connect_ec);
    SPANIO_REQUIRE(server.is_open());
}

// Result of an operation with (error, bytes) handler, recorded by a test
struct transfer_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // Byte count passed to the handler
    std::size_t m_bytes = 0;
};

// Handler that records its arguments in a transfer_record
class record_transfer
{
public:
    // Parameter record: where to store the result; must outlive the handler
    explicit record_transfer(transfer_record& record)
        : m_record(&record)
    {
    }

    // Parameter ec:    operation result
    // Parameter bytes: bytes transferred
    void operator()(std::error_code ec, std::size_t bytes)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
        m_record->m_bytes = bytes;
    }

private:
    // Destination of the recorded values
    transfer_record* m_record;
};

// Reads from a socket chunk by chunk until a given number of bytes arrived
// or an error occurred. Copies of it share the same destination objects.
class chunk_reader
{
public:
    // Parameter socket:   connected socket to read from
    // Parameter received: grows with the bytes read
    // Parameter chunk:    receive buffer for one read
    // Parameter ec:       receives the first error
    // Parameter expected: total number of bytes to read
    chunk_reader(tcp::socket& socket, std::vector<char>& received, std::vector<char>& chunk,
                 std::error_code& ec, std::size_t expected)
        : m_socket(&socket)
        , m_received(&received)
        , m_chunk(&chunk)
        , m_ec(&ec)
        , m_expected(expected)
    {
    }

    // Start the next read
    void start()
    {
        m_socket->async_read_some(spanio::buffer(*m_chunk), *this);
    }

    // Completion of one read
    // Parameter ec:    read result
    // Parameter bytes: bytes read into the chunk buffer
    void operator()(std::error_code ec, std::size_t bytes)
    {
        if (ec)
        {
            *m_ec = ec;
            return;
        }
        m_received->insert(m_received->end(), m_chunk->begin(),
                           m_chunk->begin() + static_cast<std::ptrdiff_t>(bytes));
        if (m_received->size() < m_expected)
        {
            start();
        }
    }

private:
    // Socket being read
    tcp::socket* m_socket;
    // All bytes read so far
    std::vector<char>* m_received;
    // Buffer for one read
    std::vector<char>* m_chunk;
    // First error seen
    std::error_code* m_ec;
    // Number of bytes to read in total
    std::size_t m_expected;
};

// Counters shared by every session of the echo case. Updated from several
// run() threads, so all of them are atomic.
struct echo_stats
{
    // Unexpected errors in any client or server session
    std::atomic<int> m_errors{0};
    // Echo replies that did not match what was sent
    std::atomic<int> m_mismatches{0};
    // Replies received and compared
    std::atomic<int> m_echoed{0};
    // Clients that finished all rounds and saw the server close
    std::atomic<int> m_finished{0};
    // Connections accepted by the server
    std::atomic<int> m_accepted{0};
};

// Server side of one echo connection: reads a piece, writes it back, and
// repeats until end of file. Kept alive by the handlers that reference it.
class echo_session : public std::enable_shared_from_this<echo_session>
{
public:
    // Parameter peer:  accepted connection
    // Parameter stats: shared counters
    echo_session(tcp::socket peer, echo_stats& stats)
        : m_socket(std::move(peer))
        , m_stats(&stats)
        , m_buffer{}
    {
    }

    // Start echoing
    void start()
    {
        read();
    }

private:
    // Read the next piece
    void read()
    {
        std::shared_ptr<echo_session> self = shared_from_this();
        auto on_read = [self](std::error_code ec, std::size_t bytes)
        {
            self->on_read(ec, bytes);
        };
        m_socket.async_read_some(spanio::buffer(m_buffer.data(), m_buffer.size()), on_read);
    }

    // Echo a piece back, or stop at end of file
    // Parameter ec:    read result
    // Parameter bytes: bytes read
    void on_read(std::error_code ec, std::size_t bytes)
    {
        if (ec)
        {
            if (ec != spanio::error::misc_errors::eof)
            {
                ++m_stats->m_errors;
            }
            // Returning drops the last reference; the socket closes
            return;
        }
        std::shared_ptr<echo_session> self = shared_from_this();
        auto on_written = [self](std::error_code write_ec, std::size_t)
        {
            if (write_ec)
            {
                ++self->m_stats->m_errors;
                return;
            }
            self->read();
        };
        m_socket.async_write(spanio::buffer(m_buffer.data(), bytes), on_written);
    }

    // The connection
    tcp::socket m_socket;
    // Shared counters
    echo_stats* m_stats;
    // Receive buffer, also the source of the echo
    std::array<char, kEchoChunkSize> m_buffer;
};

// Accepts kEchoConnections connections, starting an echo_session for each
class echo_acceptor
{
public:
    // Parameter acc:   listening acceptor; must outlive the accepts
    // Parameter stats: shared counters
    echo_acceptor(tcp::acceptor& acc, echo_stats& stats)
        : m_acceptor(&acc)
        , m_stats(&stats)
    {
    }

    // Start the next accept
    void start()
    {
        m_acceptor->async_accept(*this);
    }

    // Completion of one accept
    // Parameter ec:   accept result
    // Parameter peer: the new connection
    void operator()(std::error_code ec, tcp::socket peer)
    {
        if (ec)
        {
            ++m_stats->m_errors;
            return;
        }
        std::make_shared<echo_session>(std::move(peer), *m_stats)->start();
        if (++m_stats->m_accepted < kEchoConnections)
        {
            start();
        }
    }

private:
    // Acceptor accepting the connections
    tcp::acceptor* m_acceptor;
    // Shared counters
    echo_stats* m_stats;
};

// Client side of one echo connection: sends kEchoRounds messages, checks
// each reply, then shuts down sending and waits for the server to close
class echo_client : public std::enable_shared_from_this<echo_client>
{
public:
    // Parameter ctx:    context to run on
    // Parameter server: address of the echo server
    // Parameter id:     connection id, used in the payload pattern
    // Parameter stats:  shared counters
    echo_client(io_context& ctx, const tcp::endpoint& server, int id, echo_stats& stats)
        : m_socket(ctx)
        , m_server(server)
        , m_id(id)
        , m_round(0)
        , m_received(0)
        , m_stats(&stats)
        , m_out(kEchoMessageSize)
        , m_in(kEchoMessageSize)
    {
    }

    // Connect and start the first round
    void start()
    {
        std::shared_ptr<echo_client> self = shared_from_this();
        auto on_connect = [self](std::error_code ec)
        {
            if (ec)
            {
                ++self->m_stats->m_errors;
                return;
            }
            self->send_round();
        };
        m_socket.async_connect(m_server, on_connect);
    }

private:
    // Send the message of the current round
    void send_round()
    {
        for (std::size_t i = 0; i < kEchoMessageSize; ++i)
        {
            m_out[i] = echo_byte(m_id, m_round, i);
        }
        std::shared_ptr<echo_client> self = shared_from_this();
        auto on_written = [self](std::error_code ec, std::size_t)
        {
            if (ec)
            {
                ++self->m_stats->m_errors;
                return;
            }
            self->m_received = 0;
            self->read_reply();
        };
        m_socket.async_write(spanio::buffer(m_out), on_written);
    }

    // Read the rest of the current reply
    void read_reply()
    {
        std::shared_ptr<echo_client> self = shared_from_this();
        auto on_read = [self](std::error_code ec, std::size_t bytes)
        {
            self->on_reply(ec, bytes);
        };
        m_socket.async_read_some(spanio::buffer(m_in.data() + m_received, kEchoMessageSize - m_received),
                                 on_read);
    }

    // Collect a piece of the reply; check it once complete
    // Parameter ec:    read result
    // Parameter bytes: bytes read
    void on_reply(std::error_code ec, std::size_t bytes)
    {
        if (ec)
        {
            ++m_stats->m_errors;
            return;
        }
        m_received += bytes;
        if (m_received < kEchoMessageSize)
        {
            read_reply();
            return;
        }
        if (m_in != m_out)
        {
            ++m_stats->m_mismatches;
        }
        ++m_stats->m_echoed;
        ++m_round;
        if (m_round < kEchoRounds)
        {
            send_round();
            return;
        }
        if (m_socket.shutdown(tcp::shutdown_type::send))
        {
            ++m_stats->m_errors;
            return;
        }
        wait_for_close();
    }

    // Wait for the server to close after our shutdown
    void wait_for_close()
    {
        std::shared_ptr<echo_client> self = shared_from_this();
        auto on_eof = [self](std::error_code ec, std::size_t)
        {
            if (ec == spanio::error::misc_errors::eof)
            {
                ++self->m_stats->m_finished;
            }
            else
            {
                ++self->m_stats->m_errors;
            }
        };
        m_socket.async_read_some(spanio::buffer(m_in), on_eof);
    }

    // The connection
    tcp::socket m_socket;
    // Address of the echo server
    tcp::endpoint m_server;
    // Connection id
    int m_id;
    // Current round, 0-based
    int m_round;
    // Bytes of the current reply received so far
    std::size_t m_received;
    // Shared counters
    echo_stats* m_stats;
    // Message of the current round
    std::vector<char> m_out;
    // Reply of the current round
    std::vector<char> m_in;
};

// Proactor decorator that forwards everything to a real backend but cuts
// every send down to kForcedSendChunk bytes, so each send completes
// partially. Lets the tests drive the "send the rest" path of async_write
// on platforms whose real sends never come back partial.
class chunking_proactor final : public spanio::detail::io_proactor
{
public:
    // Parameter inner:      real backend that performs the I/O
    // Parameter send_count: incremented for every send started
    chunking_proactor(spanio::detail::io_proactor_ptr inner, std::atomic<int>& send_count)
        : m_inner(std::move(inner))
        , m_send_count(&send_count)
    {
    }

    // Forwarded to the real backend
    // Parameter handle: socket to register
    // Return value: the backend's result
    std::error_code register_handle(spanio::detail::native_handle_t handle) override
    {
        return m_inner->register_handle(handle);
    }

    // Forwarded to the real backend
    // Parameter handle: socket to unregister
    // Return value: the backend's result
    std::error_code unregister_handle(spanio::detail::native_handle_t handle) override
    {
        return m_inner->unregister_handle(handle);
    }

    // Shrink sends to kForcedSendChunk bytes, then forward
    // Parameter op: operation to start
    void start(spanio::detail::io_operation* op) override
    {
        if (op->m_kind == spanio::detail::op_kind::kSend)
        {
            ++*m_send_count;
            if (op->m_size > kForcedSendChunk)
            {
                op->m_size = kForcedSendChunk;
            }
        }
        m_inner->start(op);
    }

    // Forwarded to the real backend
    // Parameter handle: socket whose operations to cancel
    void cancel(spanio::detail::native_handle_t handle) override
    {
        m_inner->cancel(handle);
    }

    // Forwarded to the real backend
    // Parameter op: operation to queue
    void post(spanio::detail::io_operation* op) override
    {
        m_inner->post(op);
    }

    // Forwarded to the real backend
    // Parameter out:        receives finished operations
    // Parameter timeout_ms: maximum wait
    // Parameter ec:         receives the failure reason
    // Return value: number of finished operations
    std::size_t poll(spanio::detail::completion_list& out, int timeout_ms, std::error_code& ec) override
    {
        return m_inner->poll(out, timeout_ms, ec);
    }

    // Forwarded to the real backend
    void interrupt() noexcept override
    {
        m_inner->interrupt();
    }

    // Name of the wrapped backend
    // Return value: static string
    const char* name() const noexcept override
    {
        return m_inner->name();
    }

private:
    // Real backend doing the I/O
    spanio::detail::io_proactor_ptr m_inner;
    // Number of sends started, shared with the test
    std::atomic<int>* m_send_count;
};

}

// Case 1: numeric addresses parse, print back, and the well-known
// addresses are right; invalid text is rejected
SPANIO_TEST(ip_address_parse_and_format)
{
    std::error_code ec;
    const ip::address v4 = ip::make_address("127.0.0.1", ec);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(v4.is_v4());
    SPANIO_CHECK(!v4.is_v6());
    SPANIO_CHECK(v4.to_string() == "127.0.0.1");
    SPANIO_CHECK(v4 == spanio::ipv4::loopback());
    SPANIO_CHECK(v4 != spanio::ipv4::any());

    const ip::address v6 = ip::make_address("::1", ec);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(v6.is_v6());
    SPANIO_CHECK(v6.to_string() == "::1");
    SPANIO_CHECK(v6 == spanio::ipv6::loopback());
    SPANIO_CHECK(v6 != v4);

    SPANIO_CHECK(spanio::ipv4::any().to_string() == "0.0.0.0");
    SPANIO_CHECK(spanio::ipv6::any().to_string() == "::");
    SPANIO_CHECK(ip::address() == spanio::ipv4::any());

    ip::make_address("192.168.1.300", ec);
    SPANIO_CHECK(ec == std::errc::invalid_argument);
    ip::make_address("not an address", ec);
    SPANIO_CHECK(ec == std::errc::invalid_argument);

    bool thrown = false;
    try
    {
        ip::make_address(std::string("bad"));
    }
    catch (const std::system_error&)
    {
        thrown = true;
    }
    SPANIO_CHECK(thrown);
}

// Case 2: an endpoint keeps address and port, and survives the round trip
// through the native socket address
SPANIO_TEST(endpoint_roundtrip)
{
    const tcp::endpoint v4(spanio::ipv4::loopback(), kSamplePort);
    SPANIO_CHECK(v4.family() == AF_INET);
    SPANIO_CHECK(v4.address() == spanio::ipv4::loopback());
    SPANIO_CHECK(v4.port() == kSamplePort);
    SPANIO_CHECK(v4.size() == static_cast<spanio::detail::socklen_type>(sizeof(sockaddr_in)));
    SPANIO_CHECK(tcp::endpoint::from_native(v4.data(), v4.size()) == v4);
    SPANIO_CHECK(v4 != tcp::endpoint(spanio::ipv4::loopback(), kOtherPort));
    SPANIO_CHECK(v4 != tcp::endpoint(spanio::ipv4::any(), kSamplePort));

    const tcp::endpoint v6(spanio::ipv6::loopback(), kSamplePort);
    SPANIO_CHECK(v6.family() == AF_INET6);
    SPANIO_CHECK(v6.address() == spanio::ipv6::loopback());
    SPANIO_CHECK(v6.port() == kSamplePort);
    SPANIO_CHECK(v6.size() == static_cast<spanio::detail::socklen_type>(sizeof(sockaddr_in6)));
    SPANIO_CHECK(tcp::endpoint::from_native(v6.data(), v6.size()) == v6);

    const tcp::endpoint unset;
    SPANIO_CHECK(unset.address() == spanio::ipv4::any());
    SPANIO_CHECK(unset.port() == 0);
}

// Case 3: every buffer() overload views the right memory
SPANIO_TEST(buffer_helpers)
{
    char array[kArraySize] = {};
    const spanio::mutable_buffer from_array = spanio::buffer(array);
    SPANIO_CHECK(from_array.data() == array);
    SPANIO_CHECK(from_array.size() == kArraySize);

    std::string text(kStringSize, 'x');
    const spanio::mutable_buffer from_string = spanio::buffer(text);
    SPANIO_CHECK(from_string.data() == text.data());
    SPANIO_CHECK(from_string.size() == kStringSize);

    const std::string const_text(kStringSize, 'y');
    const spanio::const_buffer from_const_string = spanio::buffer(const_text);
    SPANIO_CHECK(from_const_string.data() == const_text.data());
    SPANIO_CHECK(from_const_string.size() == kStringSize);

    std::vector<char> bytes(kVectorSize);
    const spanio::mutable_buffer from_vector = spanio::buffer(bytes);
    SPANIO_CHECK(from_vector.data() == bytes.data());
    SPANIO_CHECK(from_vector.size() == kVectorSize);

    const spanio::mutable_buffer from_raw = spanio::buffer(array, kRawSize);
    SPANIO_CHECK(from_raw.data() == array);
    SPANIO_CHECK(from_raw.size() == kRawSize);

    const std::string_view view = "abc";
    const spanio::const_buffer from_view = spanio::buffer(view);
    SPANIO_CHECK(from_view.data() == view.data());
    SPANIO_CHECK(from_view.size() == view.size());

    // A string literal is a char array, terminating zero included
    const spanio::const_buffer from_literal = spanio::buffer(kMessage);
    SPANIO_CHECK(from_literal.size() == kMessageLength + 1);

    const spanio::const_buffer converted = from_array;
    SPANIO_CHECK(converted.data() == array);
    SPANIO_CHECK(converted.size() == kArraySize);

    const spanio::mutable_buffer empty;
    SPANIO_CHECK(empty.data() == nullptr);
    SPANIO_CHECK(empty.size() == 0);
}

// Case 4: spanio::error::misc_errors::eof is a proper error_code in its own category
SPANIO_TEST(error_eof_category)
{
    const std::error_code ec = spanio::error::misc_errors::eof;
    SPANIO_CHECK(static_cast<bool>(ec));
    SPANIO_CHECK(std::strcmp(ec.category().name(), "spanio.misc") == 0);
    SPANIO_CHECK(&ec.category() == &spanio::error::misc_category());
    SPANIO_CHECK(!ec.message().empty());
    SPANIO_CHECK(ec == spanio::error::make_error_code(spanio::error::misc_errors::eof));
    SPANIO_CHECK(ec == spanio::error::misc_errors::eof);
    SPANIO_CHECK(ec != std::make_error_code(std::errc::operation_canceled));
}

// Case 5: binding port 0 gets a real port; a second acceptor on that port
// fails with address_in_use, through both the error_code and throwing APIs
SPANIO_TEST(acceptor_bind_ephemeral_and_in_use)
{
    io_context ctx;
    tcp::acceptor first(ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    SPANIO_CHECK(first.is_open());
    const tcp::endpoint bound = listening_endpoint(first);
    SPANIO_CHECK(bound.port() != kAnyPort);
    SPANIO_CHECK(bound.address() == spanio::ipv4::loopback());

    tcp::acceptor second(ctx);
    SPANIO_REQUIRE(!second.open(AF_INET));
    const std::error_code bind_ec = second.bind(bound);
    SPANIO_CHECK(bind_ec == std::errc::address_in_use);

    bool thrown = false;
    std::error_code thrown_ec;
    try
    {
        tcp::acceptor third(ctx, bound);
    }
    catch (const std::system_error& e)
    {
        thrown = true;
        thrown_ec = e.code();
    }
    SPANIO_CHECK(thrown);
    SPANIO_CHECK(thrown_ec == std::errc::address_in_use);

    first.close();
    SPANIO_CHECK(!first.is_open());
    SPANIO_CHECK(second.open(AF_INET) == std::errc::invalid_argument);
}

// Case 6: accept and connect both complete; the two ends see each other's
// addresses
SPANIO_TEST(async_accept_and_connect)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        std::error_code ec;
        const tcp::endpoint client_local = client.local_endpoint(ec);
        SPANIO_CHECK(!ec);
        const tcp::endpoint server_remote = server.remote_endpoint(ec);
        SPANIO_CHECK(!ec);
        SPANIO_CHECK(server_remote == client_local);
        const tcp::endpoint client_remote = client.remote_endpoint(ec);
        SPANIO_CHECK(!ec);
        SPANIO_CHECK(client_remote == listening_endpoint(acc));
        SPANIO_CHECK(&server.context() == ctx.get());
    }
}

// Case 7: a message written with async_write_some arrives through
// async_read_some with the right content and size
SPANIO_TEST(read_some_write_some_roundtrip)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        char buffer[kSmallBufferSize] = {};
        transfer_record written;
        transfer_record read;
        server.async_read_some(spanio::buffer(buffer), record_transfer(read));
        client.async_write_some(spanio::buffer(kMessage, kMessageLength), record_transfer(written));
        SPANIO_CHECK(ctx->run() == 2);

        SPANIO_CHECK(written.m_calls == 1);
        SPANIO_CHECK(!written.m_ec);
        SPANIO_CHECK(written.m_bytes == kMessageLength);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(!read.m_ec);
        SPANIO_CHECK(read.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(buffer, kMessage, kMessageLength) == 0);
    }
}

// Case 8: a 4 MB async_write reports every byte, and the peer receives
// exactly that data
SPANIO_TEST(async_write_large_buffer)
{
    std::vector<char> payload(kLargeTransferSize);
    for (std::size_t i = 0; i < payload.size(); ++i)
    {
        payload[i] = pattern_byte(i);
    }
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        std::vector<char> received;
        std::vector<char> chunk(kChunkSize);
        std::error_code read_ec;
        transfer_record written;
        // A small send buffer is meant to make sends partial, so the "send
        // the rest" path of async_write runs. Measured on Windows: both
        // backends still send all 4 MB in one call, so there this only
        // checks the single-send path; Linux non-blocking sends are expected
        // to be partial (not verified yet)
        const int send_buffer = kSmallSendBuffer;
        SPANIO_REQUIRE(::setsockopt(client.native_handle(), SOL_SOCKET, SO_SNDBUF,
                                    reinterpret_cast<const char*>(&send_buffer),
                                    static_cast<spanio::detail::socklen_type>(sizeof(send_buffer)))
                       != spanio::detail::kSocketCallFailed);
        chunk_reader reader(server, received, chunk, read_ec, kLargeTransferSize);
        reader.start();
        client.async_write(spanio::buffer(payload), record_transfer(written));
        ctx->run();

        SPANIO_CHECK(written.m_calls == 1);
        SPANIO_CHECK(!written.m_ec);
        SPANIO_CHECK(written.m_bytes == kLargeTransferSize);
        SPANIO_CHECK(!read_ec);
        SPANIO_CHECK(received == payload);
    }
}

// Case 9: after the peer shuts down sending, a read reports
// spanio::error::misc_errors::eof with 0 bytes
SPANIO_TEST(read_eof_after_peer_shutdown)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        SPANIO_CHECK(!client.shutdown(tcp::shutdown_type::send));
        char buffer[kSmallBufferSize] = {};
        transfer_record read;
        server.async_read_some(spanio::buffer(buffer), record_transfer(read));
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(read.m_ec == spanio::error::misc_errors::eof);
        SPANIO_CHECK(read.m_bytes == 0);
    }
}

// Case 10: connecting to a port nobody listens on reports
// connection_refused
SPANIO_TEST(connect_refused)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::endpoint closed_port;
        {
            // Borrow a free port, then release it so nothing listens there
            tcp::acceptor probe(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
            closed_port = listening_endpoint(probe);
        }
        tcp::socket client(*ctx);
        int calls = 0;
        std::error_code connect_ec;
        auto on_connect = [&](std::error_code ec)
        {
            ++calls;
            connect_ec = ec;
        };
        const test_clock::time_point start = test_clock::now();
        client.async_connect(closed_port, on_connect);
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(elapsed_ms(start) < kRefusedLimitMs);
        SPANIO_CHECK(calls == 1);
        SPANIO_CHECK(connect_ec == std::errc::connection_refused);
    }
}

// Case 11: close() with a pending read and a pending accept: both
// handlers get operation_canceled and run() returns
SPANIO_TEST(close_cancels_pending_ops)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        char buffer[kSmallBufferSize] = {};
        transfer_record read;
        int accept_calls = 0;
        std::error_code accept_ec;
        bool accepted_open = true;
        auto on_accept = [&](std::error_code ec, tcp::socket peer)
        {
            ++accept_calls;
            accept_ec = ec;
            accepted_open = peer.is_open();
        };
        server.async_read_some(spanio::buffer(buffer), record_transfer(read));
        acc.async_accept(on_accept);
        server.close();
        acc.close();
        SPANIO_CHECK(!server.is_open());
        SPANIO_CHECK(!acc.is_open());

        SPANIO_CHECK(ctx->run() == 2);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(read.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(read.m_bytes == 0);
        SPANIO_CHECK(accept_calls == 1);
        SPANIO_CHECK(accept_ec == std::errc::operation_canceled);
        SPANIO_CHECK(!accepted_open);
    }
}

// Case 12: destroying a socket with a pending read still runs the handler
// once, cancelled
SPANIO_TEST(destroy_socket_with_pending_read)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        // The buffer outlives the socket: the read may still refer to it
        char buffer[kSmallBufferSize] = {};
        transfer_record read;
        std::unique_ptr<tcp::socket> server(new tcp::socket(*ctx));
        connect_pair(*ctx, acc, client, *server);

        server->async_read_some(spanio::buffer(buffer), record_transfer(read));
        server.reset();
        SPANIO_CHECK(ctx->run() == 1);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(read.m_ec == std::errc::operation_canceled);
    }
}

// Case 13: operations on a socket or acceptor that is not open fail with
// bad_file_descriptor, through run(), never inline
SPANIO_TEST(op_on_closed_socket_fails_async)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::socket closed(*ctx);
        tcp::acceptor closed_acceptor(*ctx);
        char buffer[kSmallBufferSize] = {};
        transfer_record read;
        transfer_record written_some;
        transfer_record written_all;
        int accept_calls = 0;
        std::error_code accept_ec;
        auto on_accept = [&](std::error_code ec, tcp::socket)
        {
            ++accept_calls;
            accept_ec = ec;
        };

        closed.async_read_some(spanio::buffer(buffer), record_transfer(read));
        closed.async_write_some(spanio::buffer(kMessage, kMessageLength), record_transfer(written_some));
        closed.async_write(spanio::buffer(kMessage, kMessageLength), record_transfer(written_all));
        closed_acceptor.async_accept(on_accept);
        SPANIO_CHECK(read.m_calls == 0);
        SPANIO_CHECK(written_some.m_calls == 0);
        SPANIO_CHECK(written_all.m_calls == 0);
        SPANIO_CHECK(accept_calls == 0);

        SPANIO_CHECK(ctx->run() == kClosedObjectOps);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(read.m_ec == std::errc::bad_file_descriptor);
        SPANIO_CHECK(read.m_bytes == 0);
        SPANIO_CHECK(written_some.m_ec == std::errc::bad_file_descriptor);
        SPANIO_CHECK(written_all.m_ec == std::errc::bad_file_descriptor);
        SPANIO_CHECK(accept_calls == 1);
        SPANIO_CHECK(accept_ec == std::errc::bad_file_descriptor);
    }
}

// Case 14: moving a socket transfers the connection; move-assigning onto
// an open socket closes the old one
SPANIO_TEST(socket_move_semantics)
{
    io_context ctx;
    tcp::acceptor acc(ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    tcp::socket client(ctx);
    tcp::socket server(ctx);
    connect_pair(ctx, acc, client, server);

    const spanio::detail::native_handle_t handle = client.native_handle();
    tcp::socket moved(std::move(client));
    SPANIO_CHECK(!client.is_open());
    SPANIO_CHECK(client.native_handle() == spanio::detail::kInvalidHandle);
    SPANIO_CHECK(moved.is_open());
    SPANIO_CHECK(moved.native_handle() == handle);

    tcp::socket target(ctx);
    SPANIO_REQUIRE(!target.open(AF_INET));
    const spanio::detail::native_handle_t replaced = target.native_handle();
    target = std::move(moved);
    SPANIO_CHECK(!moved.is_open());
    SPANIO_CHECK(target.native_handle() == handle);
    // The replaced handle was closed by the assignment, so closing it again
    // must fail
    SPANIO_CHECK(static_cast<bool>(spanio::detail::close_socket(replaced)));

    // The moved connection still works
    char buffer[kSmallBufferSize] = {};
    transfer_record written;
    transfer_record read;
    server.async_read_some(spanio::buffer(buffer), record_transfer(read));
    target.async_write_some(spanio::buffer(kMessage, kMessageLength), record_transfer(written));
    SPANIO_CHECK(ctx.run() == 2);
    SPANIO_CHECK(!written.m_ec);
    SPANIO_CHECK(read.m_bytes == kMessageLength);
    SPANIO_CHECK(std::memcmp(buffer, kMessage, kMessageLength) == 0);
}

// Case 15: four threads run() an echo server and 16 clients that each send
// 10 messages; every reply matches and every connection ends cleanly
SPANIO_TEST(multi_thread_echo)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        echo_stats stats;
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        const tcp::endpoint server = listening_endpoint(acc);

        echo_acceptor accept_loop(acc, stats);
        accept_loop.start();
        for (int id = 0; id < kEchoConnections; ++id)
        {
            std::make_shared<echo_client>(*ctx, server, id, stats)->start();
        }

        io_context& context = *ctx;
        auto run_context = [&context]()
        {
            context.run();
        };
        std::vector<std::thread> runners;
        for (int i = 0; i < kEchoThreads; ++i)
        {
            runners.emplace_back(run_context);
        }
        for (std::thread& runner : runners)
        {
            runner.join();
        }

        SPANIO_CHECK(stats.m_errors.load() == 0);
        SPANIO_CHECK(stats.m_mismatches.load() == 0);
        SPANIO_CHECK(stats.m_accepted.load() == kEchoConnections);
        SPANIO_CHECK(stats.m_echoed.load() == kEchoConnections * kEchoRounds);
        SPANIO_CHECK(stats.m_finished.load() == kEchoConnections);
    }
}

// Case 16: when every send completes partially (forced by
// chunking_proactor), async_write keeps sending the rest: it reports every
// byte once, needs one send per chunk, and the peer gets the data in order
SPANIO_TEST(async_write_continues_after_partial_send)
{
    std::vector<char> payload(kForcedPartialSize);
    for (std::size_t i = 0; i < payload.size(); ++i)
    {
        payload[i] = pattern_byte(i);
    }
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::error_code ec;
        spanio::detail::io_proactor_ptr inner = backend.m_create(ec);
        SPANIO_REQUIRE(!ec);
        std::atomic<int> send_count(0);
        io_context ctx(spanio::detail::io_proactor_ptr(new chunking_proactor(std::move(inner), send_count)));
        tcp::acceptor acc(ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(ctx);
        tcp::socket server(ctx);
        connect_pair(ctx, acc, client, server);

        std::vector<char> received;
        std::vector<char> chunk(kChunkSize);
        std::error_code read_ec;
        transfer_record written;
        chunk_reader reader(server, received, chunk, read_ec, kForcedPartialSize);
        // Shutting down after the write ends the reader with eof if
        // async_write ever reports completion before sending everything,
        // so such a bug fails the checks instead of hanging the case
        record_transfer record_written(written);
        auto on_written = [&](std::error_code write_ec, std::size_t bytes)
        {
            record_written(write_ec, bytes);
            client.shutdown(tcp::shutdown_type::send);
        };
        reader.start();
        client.async_write(spanio::buffer(payload), on_written);
        ctx.run();

        SPANIO_CHECK(written.m_calls == 1);
        SPANIO_CHECK(!written.m_ec);
        SPANIO_CHECK(written.m_bytes == kForcedPartialSize);
        SPANIO_CHECK(send_count.load() >= kForcedMinSends);
        SPANIO_CHECK(!read_ec);
        SPANIO_CHECK(received == payload);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
