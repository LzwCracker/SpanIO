// ============================================================================
// File purpose: Unit tests for the unified error codes: cancellation of the
//               endpoint-list connect, writes after a local shutdown and after
//               a peer reset (the same errc on every platform and backend),
//               the spanio.misc / spanio.netdb categories, and the platform
//               code translation of detail::normalize_error().
// Usage:        Built as the test_errors executable by CMake. Run it with no
//               arguments for every case, or with a substring of a case name,
//               e.g. `test_errors reset`.
//
//               Failed I/O cases print the raw error (value and category), so
//               a platform difference can be read straight from the output.
//
//               Declaration order in each case: recorders, context, I/O
//               objects, so I/O objects are destroyed first.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-09
// ============================================================================

#include "spanio/error.h"
#include "spanio/resolver.h"
#include "spanio/tcp.h"
#include "spanio/detail/socket_ops.h"
#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::kAnyPort;
using spanio::test::make_context;
namespace tcp = spanio::tcp;
namespace error = spanio::error;

namespace {

// Time given to a reset segment to reach the other side over loopback
constexpr std::chrono::milliseconds kResetSettle(200);

// Bytes written per attempt in the peer-reset case
constexpr std::size_t kResetChunk = 64 * 1024;

// Writes tried in the peer-reset case before giving up; the first one after
// the reset is expected to fail, the rest only absorb buffering surprises
constexpr int kResetWriteAttempts = 100;

// Text the categories return for a value they do not know; every real
// value must have its own text
const char* const kMiscUnknownText = "spanio.misc error";
const char* const kNetdbUnknownText = "spanio.netdb error";

// Print an error code with its raw value and category
// Parameter what: label
// Parameter ec:   the error
void print_error(const char* what, const std::error_code& ec)
{
    std::printf("      %s: %d (%s) %s\n", what, ec.value(), ec.category().name(), ec.message().c_str());
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

// An endpoint on 127.0.0.1 where nothing listens: a port the system just
// handed out and that is closed again
// Parameter ctx: context for the temporary acceptor
// Return value: the endpoint
tcp::endpoint closed_endpoint(io_context& ctx)
{
    tcp::acceptor temporary(ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
    return listening_endpoint(temporary);
}

// Write once and run the context until the write finished
// Parameter ctx:  context of the socket
// Parameter sock: socket to write on
// Parameter data: bytes to write
// Return value: the write's error
std::error_code write_once(io_context& ctx, tcp::socket& sock, const std::vector<char>& data)
{
    std::error_code result;
    auto on_write = [&result](std::error_code ec, std::size_t /*bytes*/)
    {
        result = ec;
    };
    sock.async_write(spanio::buffer(data), on_write);
    ctx.run();
    ctx.restart();
    return result;
}

}

// Case 1: closing the socket while tcp::async_connect tries an endpoint list
// ends the operation with operation_canceled; it does not go on to the next
// endpoint (which would accept)
SPANIO_TEST(connect_sequence_cancel)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        int calls = 0;
        std::error_code result;
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor listener(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket sock(*ctx);

        // The first endpoint refuses slowly enough (or only after a poll) to
        // still be pending when the socket is closed; the second would accept
        const std::vector<tcp::endpoint> endpoints{closed_endpoint(*ctx), listening_endpoint(listener)};
        auto on_connect = [&](std::error_code ec, const tcp::endpoint& /*connected*/)
        {
            ++calls;
            result = ec;
        };
        tcp::async_connect(sock, endpoints, on_connect);
        sock.close();
        ctx->run();

        SPANIO_CHECK(calls == 1);
        if (result != std::errc::operation_canceled)
        {
            print_error("connect result", result);
        }
        SPANIO_CHECK(result == std::errc::operation_canceled);
        SPANIO_CHECK(!sock.is_open());
    }
}

// Case 2: writing after shutting down the sending side reports broken_pipe
// on every platform and backend
SPANIO_TEST(write_after_shutdown)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        const std::vector<char> data(1, 'x');
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        SPANIO_REQUIRE(!client.shutdown(tcp::shutdown_type::send));
        const std::error_code ec = write_once(*ctx, client, data);
        print_error("write after shutdown", ec);
        SPANIO_CHECK(ec == std::errc::broken_pipe);
    }
}

// Case 3: the first failing write after the peer reset the connection
// reports connection_reset on every platform and backend
SPANIO_TEST(write_after_peer_reset)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        const std::vector<char> data(kResetChunk, 'r');
        std::unique_ptr<io_context> ctx = make_context(backend);
        tcp::acceptor acc(*ctx, tcp::endpoint(spanio::ipv4::loopback(), kAnyPort));
        tcp::socket client(*ctx);
        tcp::socket server(*ctx);
        connect_pair(*ctx, acc, client, server);

        // A zero linger time makes close() send RST instead of FIN
        linger abort_on_close{};
        abort_on_close.l_onoff = 1;
        abort_on_close.l_linger = 0;
        SPANIO_REQUIRE(::setsockopt(server.native_handle(), SOL_SOCKET, SO_LINGER,
                                    reinterpret_cast<const char*>(&abort_on_close),
                                    static_cast<spanio::detail::socklen_type>(sizeof(abort_on_close)))
                       != spanio::detail::kSocketCallFailed);
        server.close();
        std::this_thread::sleep_for(kResetSettle);

        std::error_code first_error;
        for (int attempt = 0; attempt < kResetWriteAttempts && !first_error; ++attempt)
        {
            first_error = write_once(*ctx, client, data);
        }
        print_error("first failed write", first_error);
        SPANIO_CHECK(first_error == std::errc::connection_reset);

        // Writing again on the dead connection keeps failing; whatever it
        // reports is printed for the record
        const std::error_code again = write_once(*ctx, client, data);
        print_error("write after that", again);
        SPANIO_CHECK(static_cast<bool>(again));
    }
}

// Case 4: the two SpanIO categories: names, a text for every value, values
// that are not 0, comparison with the enums, and no confusion between
// categories that share a value
SPANIO_TEST(error_categories)
{
    const std::error_code eof = error::misc_errors::eof;
    SPANIO_CHECK(std::string(eof.category().name()) == "spanio.misc");
    const std::error_code not_found = error::netdb_errors::host_not_found;
    SPANIO_CHECK(std::string(not_found.category().name()) == "spanio.netdb");

    const std::vector<error::misc_errors> misc{error::misc_errors::eof,           error::misc_errors::message_too_large,
                                               error::misc_errors::send_queue_full, error::misc_errors::framing_error,
                                               error::misc_errors::handler_failed,  error::misc_errors::datagram_truncated,
                                               error::misc_errors::idle_timeout,    error::misc_errors::close_timeout};
    for (error::misc_errors value : misc)
    {
        const std::error_code ec = value;
        SPANIO_CHECK(static_cast<bool>(ec));
        SPANIO_CHECK(ec == value);
        SPANIO_CHECK(&ec.category() == &error::misc_category());
        if (ec.message() == kMiscUnknownText)
        {
            print_error("misc value without text", ec);
        }
        SPANIO_CHECK(ec.message() != kMiscUnknownText);
    }

    const std::vector<error::netdb_errors> netdb{error::netdb_errors::host_not_found, error::netdb_errors::host_not_found_try_again,
                                                 error::netdb_errors::no_data, error::netdb_errors::no_recovery,
                                                 error::netdb_errors::service_not_found};
    for (error::netdb_errors value : netdb)
    {
        const std::error_code ec = value;
        SPANIO_CHECK(static_cast<bool>(ec));
        SPANIO_CHECK(ec == value);
        SPANIO_CHECK(&ec.category() == &error::netdb_category());
        if (ec.message() == kNetdbUnknownText)
        {
            print_error("netdb value without text", ec);
        }
        SPANIO_CHECK(ec.message() != kNetdbUnknownText);
    }

    // eof and host_not_found are both value 1 in different categories
    SPANIO_CHECK(eof.value() == not_found.value());
    SPANIO_CHECK(eof != not_found);
    SPANIO_CHECK(!(eof == error::netdb_errors::host_not_found));
    // A SpanIO code never equals a system condition with the same value
    SPANIO_CHECK(eof != std::make_error_code(std::errc::operation_canceled));
}

// Case 5: detail::normalize_error() turns the platform codes that std::errc
// does not recognise into the matching errc, and leaves others alone
SPANIO_TEST(normalize_platform_codes)
{
    using spanio::detail::normalize_error;
#if defined(SPANIO_PLATFORM_WINDOWS)
    struct mapping
    {
        // Raw Windows code
        int m_code;
        // errc it must compare equal to after normalization
        std::errc m_expected;
    };
    const mapping table[] = {
        {WSA_OPERATION_ABORTED, std::errc::operation_canceled},
        {WSAESHUTDOWN, std::errc::broken_pipe},
        {WSAENOTSOCK, std::errc::bad_file_descriptor},
        {ERROR_NETNAME_DELETED, std::errc::connection_reset},
        {ERROR_CONNECTION_REFUSED, std::errc::connection_refused},
        // Already recognised: passed through
        {WSAECONNRESET, std::errc::connection_reset},
        {WSAECONNREFUSED, std::errc::connection_refused},
        {WSAEADDRINUSE, std::errc::address_in_use},
    };
    for (const mapping& entry : table)
    {
        const std::error_code ec = normalize_error(std::error_code(entry.m_code, std::system_category()));
        if (ec != entry.m_expected)
        {
            print_error("not normalized", std::error_code(entry.m_code, std::system_category()));
        }
        SPANIO_CHECK(ec == entry.m_expected);
    }
#else
    // Linux codes already compare equal and must stay as they are
    const int unchanged[] = {EPIPE, ECONNRESET, ECONNREFUSED, EADDRINUSE, EBADF};
    for (int code : unchanged)
    {
        const std::error_code raw(code, std::system_category());
        SPANIO_CHECK(normalize_error(raw) == raw);
    }
    SPANIO_CHECK(normalize_error(std::error_code(ECANCELED, std::system_category()))
                 == std::errc::operation_canceled);
#endif
    // Codes of other categories are never touched, even with a value that
    // happens to match a platform code
    const std::error_code eof = error::misc_errors::eof;
    SPANIO_CHECK(normalize_error(eof) == eof);
    SPANIO_CHECK(!normalize_error(std::error_code()));
#if defined(SPANIO_PLATFORM_WINDOWS)
    const int mapped_value = WSAESHUTDOWN;
#else
    const int mapped_value = ECANCELED;
#endif
    const std::error_code foreign(mapped_value, error::misc_category());
    const std::error_code kept = normalize_error(foreign);
    SPANIO_CHECK(kept.value() == mapped_value);
    SPANIO_CHECK(&kept.category() == &error::misc_category());
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
