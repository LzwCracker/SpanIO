// ============================================================================
// File purpose: Guards for the API frozen at 0.3.0: the umbrella header and
//               version macros, internals that must stay out of the public
//               API, scoped enums, native_handle_type, tcp_connection's
//               user_data(), and tcp::socket::async_read (read until the
//               buffer is full) on every proactor backend.
// Usage:        Built as the test_api executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_api async_read`.
//
//               Most guards are compile-time checks: a change that puts an
//               internal back into the public API, or a scoped enum back to a
//               plain one, stops this file from compiling. The namespace-scope
//               constants that moved to spanio::detail are guarded by
//               declaring the same names here: if the library declared them
//               again, the build would fail with a redefinition.
//
//               Declaration order in each case: recorders, context, I/O
//               objects, so I/O objects are destroyed first.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

// The umbrella header alone must provide the whole public API
#include "spanio/spanio.h"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <algorithm>
#include <any>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

// ---- Sentinels for constants that moved to spanio::detail -----------------
// Value no real constant has, so a lookup that finds the library's
// declaration instead of these would also be caught at run time
#define SPANIO_TEST_SENTINEL (-12345)

namespace spanio { namespace tcp {
// Was the value turning a socket option on; now detail::kSocketOptionOn
constexpr int kSocketOptionOn = SPANIO_TEST_SENTINEL;
}}

namespace spanio { namespace ip {
// Were inet_pton / inet_ntop helpers; now in spanio::detail
constexpr int kV4AddressBytes = SPANIO_TEST_SENTINEL;
constexpr int kV6AddressBytes = SPANIO_TEST_SENTINEL;
constexpr int kAddressTextSize = SPANIO_TEST_SENTINEL;
constexpr int kPtonSuccess = SPANIO_TEST_SENTINEL;
}}

namespace spanio {
// Were the server defaults; now in spanio::detail
constexpr int kLingerTimeout = SPANIO_TEST_SENTINEL;
constexpr int kDefaultMaxConnections = SPANIO_TEST_SENTINEL;
constexpr int kDefaultMaxMessageSize = SPANIO_TEST_SENTINEL;
constexpr int kDefaultMaxSendQueueBytes = SPANIO_TEST_SENTINEL;
constexpr int kDefaultReadBufferSize = SPANIO_TEST_SENTINEL;
constexpr int kDefaultIdleTimeout = SPANIO_TEST_SENTINEL;
constexpr int kDefaultCloseTimeout = SPANIO_TEST_SENTINEL;
constexpr int kDefaultUdpSendQueueBytes = SPANIO_TEST_SENTINEL;
}

using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;
namespace tcp = spanio::tcp;
namespace udp = spanio::udp;
namespace error = spanio::error;

namespace {

// ---- Member detection ------------------------------------------------------
// Defines a trait name<T> that is true when `expression` (using `object`, an
// lvalue of T) compiles from outside the class. Access is checked during
// substitution, so a private member makes the trait false.
#define SPANIO_TEST_DEFINE_HAS(name, expression)                             \
    template <typename T, typename = void>                                    \
    struct name : std::false_type                                             \
    {                                                                         \
    };                                                                        \
    template <typename T>                                                     \
    struct name<T, std::void_t<decltype(std::declval<T&>().expression)>>     \
        : std::true_type                                                      \
    {                                                                         \
    }

// Internal interface of io_context, reachable only through context_access
SPANIO_TEST_DEFINE_HAS(has_register_handle, register_handle(spanio::detail::native_handle_t()));
SPANIO_TEST_DEFINE_HAS(has_unregister_handle, unregister_handle(spanio::detail::native_handle_t()));
SPANIO_TEST_DEFINE_HAS(has_cancel_handle, cancel_handle(spanio::detail::native_handle_t()));
SPANIO_TEST_DEFINE_HAS(has_start_io, start_io(static_cast<spanio::detail::io_op_base*>(nullptr)));
SPANIO_TEST_DEFINE_HAS(has_cancel_timer, cancel_timer(std::declval<spanio::detail::timer_state&>()));
SPANIO_TEST_DEFINE_HAS(has_schedule_timer,
                       schedule_timer(std::declval<spanio::detail::timer_state&>(),
                                      spanio::detail::timer_time_point(),
                                      static_cast<spanio::detail::timer_wait_op*>(nullptr)));
// tcp_connection: user data is user_data(), not context()
SPANIO_TEST_DEFINE_HAS(has_context, context());
SPANIO_TEST_DEFINE_HAS(has_user_data, user_data());

// Whether an enum is scoped (enum class): a scoped enum is an enum that does
// not convert to int implicitly
template <typename E>
constexpr bool is_scoped_enum()
{
    return std::is_enum<E>::value && !std::is_convertible<E, int>::value;
}

// Bytes the peer sends in the exact-read case
constexpr std::size_t kExactBytes = 10 * 1024;

// Writes the peer splits them into
constexpr std::size_t kExactStages = 3;

// Pause between those writes, so they arrive as separate segments
constexpr std::chrono::milliseconds kStagePause(30);

// Bytes the peer sends before closing in the end-of-file case
constexpr std::size_t kEofSentBytes = 100;

// Buffer the reader asks to fill in the end-of-file case
constexpr std::size_t kEofBufferBytes = 1024;

// Buffer of the cancel case
constexpr std::size_t kCancelBufferBytes = 64;

// The byte pattern repeats with this period; a prime, so it does not line up
// with the write boundaries
constexpr std::size_t kPatternPeriod = 251;

// Result of an operation with an (error, bytes) handler
struct transfer_record
{
    // Number of times the handler ran
    int m_calls = 0;
    // Error passed to the handler
    std::error_code m_ec;
    // Byte count passed to the handler
    std::size_t m_bytes = 0;
};

// Handler that records into a transfer_record
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

// Connect a client to an acceptor through the async API; restarts the
// context afterwards
// Parameter ctx:    context of all three objects
// Parameter acc:    listening acceptor
// Parameter client: socket that is not open yet
// Parameter server: receives the accepted socket
void connect_pair(io_context& ctx, tcp::acceptor& acc, tcp::socket& client, tcp::socket& server)
{
    std::error_code accept_ec;
    std::error_code connect_ec;
    auto on_accept = [&](std::error_code ec, tcp::socket peer)
    {
        accept_ec = ec;
        server = std::move(peer);
    };
    auto on_connect = [&](std::error_code ec)
    {
        connect_ec = ec;
    };
    acc.async_accept(on_accept);
    client.async_connect(listening_endpoint(acc), on_connect);
    ctx.run();
    ctx.restart();
    SPANIO_REQUIRE(!accept_ec);
    SPANIO_REQUIRE(!connect_ec);
    SPANIO_REQUIRE(server.is_open());
}

// A buffer filled with the test pattern
// Parameter size: bytes
// Return value: the buffer
std::vector<char> pattern(std::size_t size)
{
    std::vector<char> data(size);
    for (std::size_t i = 0; i < size; ++i)
    {
        data[i] = static_cast<char>(i % kPatternPeriod);
    }
    return data;
}

}

// Case 1: spanio/spanio.h alone provides the public API; the version is 0.3.0
SPANIO_TEST(umbrella_header)
{
    static_assert(SPANIO_VERSION_MAJOR == 0, "major version");
    static_assert(SPANIO_VERSION_MINOR == 3, "minor version");
    static_assert(SPANIO_VERSION_PATCH == 0, "patch version");
    static_assert(SPANIO_VERSION == SPANIO_MAKE_VERSION(0, 3, 0), "combined version");
    static_assert(SPANIO_MAKE_VERSION(0, 3, 0) > SPANIO_MAKE_VERSION(0, 2, 9), "versions compare in order");
    SPANIO_CHECK(std::strcmp(SPANIO_VERSION_STRING, "0.3.0") == 0);

    // One type or function from every public header
    static_assert(std::is_class<spanio::io_context>::value, "io_context.h");
    static_assert(std::is_class<spanio::executor_work_guard>::value, "io_context.h");
    static_assert(std::is_class<spanio::steady_timer>::value, "timer.h");
    static_assert(std::is_class<spanio::strand<io_context::executor_type>>::value, "strand.h");
    static_assert(std::is_class<tcp::socket>::value, "tcp.h");
    static_assert(std::is_class<tcp::acceptor>::value, "tcp.h");
    static_assert(std::is_class<udp::socket>::value, "udp.h");
    static_assert(std::is_class<tcp::resolver>::value, "resolver.h");
    static_assert(std::is_class<udp::resolver>::value, "resolver.h");
    static_assert(std::is_class<spanio::ip::address>::value, "ip.h");
    static_assert(std::is_class<spanio::ip::endpoint>::value, "ip.h");
    static_assert(std::is_class<spanio::mutable_buffer>::value, "buffer.h");
    static_assert(std::is_class<spanio::const_buffer>::value, "buffer.h");
    static_assert(std::is_class<spanio::tcp_server>::value, "tcp_server.h");
    static_assert(std::is_class<spanio::tcp_server_options>::value, "tcp_server.h");
    static_assert(std::is_class<spanio::udp_server>::value, "udp_server.h");
    static_assert(std::is_class<spanio::frame_result>::value, "framing.h");
    static_assert(std::is_enum<error::misc_errors>::value, "error.h");

    io_context ctx;
    const spanio::strand<io_context::executor_type> s = spanio::make_strand(ctx);
    SPANIO_CHECK(&s.context() == &ctx);
    SPANIO_CHECK(spanio::ipv4::loopback().to_string() == "127.0.0.1");
    SPANIO_CHECK(static_cast<bool>(spanio::line_framer()));
}

// Case 2: internals stay out of the public API, and every public enum is
// scoped
SPANIO_TEST(internals_not_public)
{
    static_assert(!has_register_handle<io_context>::value, "register_handle must be private");
    static_assert(!has_unregister_handle<io_context>::value, "unregister_handle must be private");
    static_assert(!has_cancel_handle<io_context>::value, "cancel_handle must be private");
    static_assert(!has_start_io<io_context>::value, "start_io must be private");
    static_assert(!has_schedule_timer<io_context>::value, "schedule_timer must be private");
    static_assert(!has_cancel_timer<io_context>::value, "cancel_timer must be private");

    static_assert(is_scoped_enum<tcp::shutdown_type>(), "shutdown_type is an enum class");
    static_assert(is_scoped_enum<error::misc_errors>(), "misc_errors is an enum class");
    static_assert(is_scoped_enum<error::netdb_errors>(), "netdb_errors is an enum class");
    static_assert(is_scoped_enum<spanio::resolve_family>(), "resolve_family is an enum class");

    // The sentinels above are the only declarations of the moved names
    SPANIO_CHECK(tcp::kSocketOptionOn == SPANIO_TEST_SENTINEL);
    SPANIO_CHECK(spanio::ip::kV4AddressBytes == SPANIO_TEST_SENTINEL);
    SPANIO_CHECK(spanio::ip::kPtonSuccess == SPANIO_TEST_SENTINEL);
    SPANIO_CHECK(spanio::kLingerTimeout == SPANIO_TEST_SENTINEL);
    SPANIO_CHECK(spanio::kDefaultMaxConnections == SPANIO_TEST_SENTINEL);
    SPANIO_CHECK(spanio::kDefaultUdpSendQueueBytes == SPANIO_TEST_SENTINEL);

    // Scoped error values still compare with error codes directly
    const std::error_code eof = error::misc_errors::eof;
    SPANIO_CHECK(eof == error::misc_errors::eof);
    const std::error_code not_found = error::netdb_errors::host_not_found;
    SPANIO_CHECK(not_found == error::netdb_errors::host_not_found);
    SPANIO_CHECK(eof != not_found);

    // The constants that stay public
    static_assert(tcp::kDefaultBacklog > 0, "kDefaultBacklog stays public");
    static_assert(spanio::kLengthHeaderBytes == 4, "kLengthHeaderBytes stays public");
    static_assert(spanio::kMaxUdpPayload == 65507, "kMaxUdpPayload stays public");
}

// Case 3: the three socket classes share native_handle_type, and
// native_handle() / assign() / endpoint sizes use the public aliases
SPANIO_TEST(native_handle_type)
{
    using handle_type = tcp::socket::native_handle_type;
    static_assert(std::is_same<handle_type, tcp::acceptor::native_handle_type>::value, "acceptor");
    static_assert(std::is_same<handle_type, udp::socket::native_handle_type>::value, "udp::socket");
    static_assert(std::is_same<decltype(std::declval<const tcp::socket&>().native_handle()), handle_type>::value,
                  "tcp::socket::native_handle");
    static_assert(std::is_same<decltype(std::declval<const tcp::acceptor&>().native_handle()), handle_type>::value,
                  "tcp::acceptor::native_handle");
    static_assert(std::is_same<decltype(std::declval<const udp::socket&>().native_handle()), handle_type>::value,
                  "udp::socket::native_handle");
    using assign_type = std::error_code (tcp::socket::*)(handle_type);
    const assign_type assign = &tcp::socket::assign;
    SPANIO_CHECK(assign != nullptr);
    static_assert(std::is_same<decltype(std::declval<const spanio::ip::endpoint&>().size()),
                               spanio::ip::endpoint::native_size_type>::value,
                  "endpoint::size");

    // assign() adopts a handle, native_handle() gives it back
    io_context ctx;
    tcp::socket sock(ctx);
    std::error_code ec;
    const handle_type raw = spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(!sock.assign(raw));
    SPANIO_CHECK(sock.native_handle() == raw);
    sock.close();
    SPANIO_CHECK(!sock.is_open());
}

// Case 4: async_read fills the whole buffer from data that arrives in
// several writes, and calls its handler once
SPANIO_TEST(async_read_exact)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        transfer_record read;
        std::error_code write_ec;
        std::size_t stage = 0;
        bool last_part_started = false;
        bool last_started_when_read = false;
        const std::vector<char> sent = pattern(kExactBytes);
        std::vector<char> received(kExactBytes);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        spanio::steady_timer pause(*ctx);
        connect_pair(*ctx, acc, client, server);

        // The server sends kExactStages parts, pausing between them
        const std::size_t part = kExactBytes / kExactStages;
        std::function<void()> write_next;
        auto on_pause = [&](std::error_code /*ec*/)
        {
            write_next();
        };
        auto on_written = [&](std::error_code ec, std::size_t /*bytes*/)
        {
            write_ec = ec;
            ++stage;
            if (!ec && stage < kExactStages)
            {
                pause.expires_after(kStagePause);
                pause.async_wait(on_pause);
            }
        };
        write_next = [&]()
        {
            const std::size_t offset = stage * part;
            const bool last = stage + 1 == kExactStages;
            const std::size_t size = last ? kExactBytes - offset : part;
            last_part_started = last;
            server.async_write(spanio::buffer(sent.data() + offset, size), on_written);
        };
        auto on_read = [&](std::error_code ec, std::size_t bytes)
        {
            last_started_when_read = last_part_started;
            record_transfer recorder(read);
            recorder(ec, bytes);
        };
        client.async_read(spanio::buffer(received), on_read);
        write_next();
        ctx->run();

        SPANIO_CHECK(!write_ec);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(!read.m_ec);
        SPANIO_CHECK(read.m_bytes == kExactBytes);
        SPANIO_CHECK(received == sent);
        SPANIO_CHECK(stage == kExactStages);
        // The read could not finish before the last part was being sent; its
        // completion may be handled before that write's own completion
        SPANIO_CHECK(last_started_when_read);
    }
}

// Case 5: when the peer closes before the buffer is full, async_read reports
// eof with the bytes that did arrive
SPANIO_TEST(async_read_eof)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        transfer_record read;
        std::error_code write_ec;
        const std::vector<char> sent = pattern(kEofSentBytes);
        std::vector<char> received(kEofBufferBytes);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        auto on_written = [&](std::error_code ec, std::size_t /*bytes*/)
        {
            write_ec = ec;
            server.shutdown(tcp::shutdown_type::send);
        };
        server.async_write(spanio::buffer(sent), on_written);
        client.async_read(spanio::buffer(received), record_transfer(read));
        ctx->run();

        SPANIO_CHECK(!write_ec);
        SPANIO_CHECK(read.m_calls == 1);
        SPANIO_CHECK(read.m_ec == error::misc_errors::eof);
        SPANIO_CHECK(read.m_bytes == kEofSentBytes);
        SPANIO_CHECK(std::equal(sent.begin(), sent.end(), received.begin()));
    }
}

// Case 6: async_read with an empty buffer, on a socket that is not open, and
// cancelled by close(); never inline, always exactly once
SPANIO_TEST(async_read_edge_cases)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        transfer_record empty;
        transfer_record not_open;
        transfer_record cancelled;
        std::vector<char> buffer(kCancelBufferBytes);
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        tcp::socket closed(*ctx);
        connect_pair(*ctx, acc, client, server);

        client.async_read(spanio::mutable_buffer(buffer.data(), 0), record_transfer(empty));
        closed.async_read(spanio::buffer(buffer), record_transfer(not_open));
        client.async_read(spanio::buffer(buffer), record_transfer(cancelled));
        // Nothing runs inside the calls
        SPANIO_CHECK(empty.m_calls == 0);
        SPANIO_CHECK(not_open.m_calls == 0);
        SPANIO_CHECK(cancelled.m_calls == 0);
        client.close();
        ctx->run();

        SPANIO_CHECK(empty.m_calls == 1);
        SPANIO_CHECK(!empty.m_ec);
        SPANIO_CHECK(empty.m_bytes == 0);
        SPANIO_CHECK(not_open.m_calls == 1);
        SPANIO_CHECK(not_open.m_ec == std::errc::bad_file_descriptor);
        SPANIO_CHECK(cancelled.m_calls == 1);
        SPANIO_CHECK(cancelled.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(cancelled.m_bytes == 0);
    }
}

// Case 7: tcp_connection keeps user data in user_data() (a std::any) and has
// no context() that could be confused with the io_context accessors. Its
// behaviour across callbacks is covered by test_tcp_server case 17.
SPANIO_TEST(user_data)
{
    static_assert(has_user_data<spanio::tcp_connection>::value, "tcp_connection::user_data");
    static_assert(!has_context<spanio::tcp_connection>::value, "tcp_connection has no context()");
    static_assert(std::is_same<decltype(std::declval<spanio::tcp_connection&>().user_data()), std::any&>::value,
                  "user_data returns std::any&");
    // The other classes keep context() for their io_context
    static_assert(has_context<tcp::socket>::value, "tcp::socket::context");
    static_assert(has_context<spanio::tcp_server>::value, "tcp_server::context");
    static_assert(has_context<spanio::udp_server>::value, "udp_server::context");
    SPANIO_CHECK(true);
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
