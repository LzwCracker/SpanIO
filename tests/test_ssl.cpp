// ============================================================================
// File purpose: Unit tests for TLS (spanio/ssl.h) and the tls_echo_server and
//               tls_client examples, which are included with their *_NO_MAIN
//               macros. Certificates are made at run time with OpenSSL; no
//               private key is stored. I/O cases run on every backend.
// Usage:        Built as test_ssl (C++20) when SPANIO_ENABLE_SSL is on. Run it
//               with no arguments for every case, or with part of a case name.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-25
// ============================================================================

#define SPANIO_TLS_ECHO_SERVER_NO_MAIN
#define SPANIO_TLS_CLIENT_NO_MAIN
#include "../examples/tls_client.cpp"
#include "../examples/tls_echo_server.cpp"

#include "spanio/strand.h"
#include "spanio/timer.h"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <openssl/ec.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace ssl = spanio::ssl;
namespace tcp = spanio::tcp;
using spanio::as_tuple;
using spanio::awaitable;
using spanio::io_context;
using spanio::use_awaitable;
using spanio::use_future;
using spanio::detail::ssl_ptr;
using spanio::error::misc_errors;
using spanio::error::ssl_errors;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::make_context;
using spanio::test::socket_guard;

namespace {

// Longest a case may run before it fails
constexpr std::chrono::seconds kLimit(30);

// Bytes sent each way in the large-transfer cases
constexpr std::size_t kLargeSize = 1024 * 1024;

// Size of the chunks of the key-update case; odd, so records never line up
constexpr std::size_t kUpdateChunk = 10007;

// Chunks sent each way in the key-update case
constexpr std::size_t kUpdateRounds = 60;

// Every how many chunks keys are updated
constexpr std::size_t kUpdateEvery = 5;

// Connections of the concurrency case
constexpr int kConnections = 8;

// Threads of the concurrency case
constexpr int kThreads = 4;

// Echoes per connection in the concurrency case
constexpr int kEchoes = 30;

// Bytes per echo in the concurrency case
constexpr std::size_t kEchoSize = 5000;

// Pattern period: prime, so shifted data never matches
constexpr std::size_t kPatternPeriod = 251;

// Size of small receive buffers
constexpr std::size_t kSmallBuffer = 64;

// Seconds in a day, for certificate validity
constexpr long kDay = 24 * 60 * 60;

// X.509 version 3, as OpenSSL counts
constexpr long kX509Version3 = 2;

// Name the test server certificate is for
const std::string kHost = "server.test";

// Message of the small echoes
const std::string kHello = "hello over tls";

// Test data that differs at every position within a period
// Parameter size: bytes wanted
// Return value: the data
std::string pattern(std::size_t size)
{
    std::string data(size, '\0');
    for (std::size_t i = 0; i < size; ++i)
    {
        data[i] = static_cast<char>(i % kPatternPeriod);
    }
    return data;
}

// Text a memory buffer holds
// Parameter bio: memory BIO
// Return value: its content
std::string text_of(BIO* bio)
{
    char* data = nullptr;
    const long size = BIO_get_mem_data(bio, &data);
    return std::string(data, static_cast<std::size_t>(size));
}

// A certificate with its private key
struct identity
{
    // Private key
    ssl_ptr<EVP_PKEY> m_key;
    // Certificate
    ssl_ptr<X509> m_certificate;
    // The key as PEM text
    std::string m_key_pem;
    // The certificate as PEM text
    std::string m_certificate_pem;
};

// Add an extension to a certificate
// Parameter certificate: certificate to change
// Parameter nid:         extension
// Parameter value:       its value in OpenSSL's text form
void add_extension(X509* certificate, int nid, const std::string& value)
{
    X509_EXTENSION* extension = ::X509V3_EXT_conf_nid(nullptr, nullptr, nid, value.c_str());
    SPANIO_REQUIRE(extension != nullptr);
    const int added = ::X509_add_ext(certificate, extension, -1);
    ::X509_EXTENSION_free(extension);
    SPANIO_REQUIRE(added == 1);
}

// Make a certificate and its key
// Parameter name:   common name, and DNS name unless it is an authority
// Parameter issuer: authority that signs; null makes a self-signed authority
// Parameter from:   start of validity, seconds from now
// Parameter until:  end of validity, seconds from now
// Return value: the identity
identity make_identity(const std::string& name, const identity* issuer, long from = -kDay, long until = kDay)
{
    static std::atomic<long> s_serial{1};
    identity made;
    made.m_key.reset(EVP_EC_gen("P-256"));
    made.m_certificate.reset(::X509_new());
    X509* certificate = made.m_certificate.get();
    SPANIO_REQUIRE(made.m_key != nullptr && certificate != nullptr);
    ::X509_set_version(certificate, kX509Version3);
    ::ASN1_INTEGER_set(::X509_get_serialNumber(certificate), s_serial.fetch_add(1));
    ::X509_gmtime_adj(::X509_getm_notBefore(certificate), from);
    ::X509_gmtime_adj(::X509_getm_notAfter(certificate), until);
    ::X509_set_pubkey(certificate, made.m_key.get());

    X509_NAME* subject = ::X509_NAME_new();
    ::X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                 reinterpret_cast<const unsigned char*>(name.c_str()), -1, -1, 0);
    ::X509_set_subject_name(certificate, subject);
    ::X509_set_issuer_name(certificate,
                           issuer != nullptr ? ::X509_get_subject_name(issuer->m_certificate.get()) : subject);
    ::X509_NAME_free(subject);
    if (issuer == nullptr)
    {
        add_extension(certificate, NID_basic_constraints, "critical,CA:TRUE");
        add_extension(certificate, NID_key_usage, "critical,keyCertSign,cRLSign");
    }
    else
    {
        add_extension(certificate, NID_subject_alt_name, "DNS:" + name + ",IP:127.0.0.1");
    }
    EVP_PKEY* signer = issuer != nullptr ? issuer->m_key.get() : made.m_key.get();
    SPANIO_REQUIRE(::X509_sign(certificate, signer, ::EVP_sha256()) > 0);

    const ssl_ptr<BIO> key_text(::BIO_new(::BIO_s_mem()));
    const ssl_ptr<BIO> certificate_text(::BIO_new(::BIO_s_mem()));
    SPANIO_REQUIRE(::PEM_write_bio_PrivateKey(key_text.get(), made.m_key.get(), nullptr, nullptr, 0, nullptr,
                                              nullptr)
                   == 1);
    SPANIO_REQUIRE(::PEM_write_bio_X509(certificate_text.get(), certificate) == 1);
    made.m_key_pem = text_of(key_text.get());
    made.m_certificate_pem = text_of(certificate_text.get());
    return made;
}

// The authority the test clients trust
// Return value: the same one for the whole program
const identity& authority()
{
    static const identity s_authority = make_identity("test authority", nullptr);
    return s_authority;
}

// The certificate of the test servers
// Return value: the same one for the whole program
const identity& server_identity()
{
    static const identity s_server = make_identity(kHost, &authority());
    return s_server;
}

// Give a server context a certificate
// Parameter settings: server context
// Parameter who:      certificate and key
void use_identity(ssl::context& settings, const identity& who)
{
    SPANIO_REQUIRE(!settings.use_certificate(who.m_certificate_pem));
    SPANIO_REQUIRE(!settings.use_private_key(who.m_key_pem));
}

// Both contexts of a test connection
struct context_pair
{
    // Parameter who: certificate of the server
    explicit context_pair(const identity& who = server_identity())
        : m_client(ssl::role::client)
        , m_server(ssl::role::server)
    {
        use_identity(m_server, who);
        SPANIO_REQUIRE(!m_client.add_certificate_authority(authority().m_certificate_pem));
    }

    // Trusts the test authority
    ssl::context m_client;
    // Has the server certificate
    ssl::context m_server;
};

// Two connected sockets on one context
struct socket_pair
{
    // Parameter context: context of both sockets
    explicit socket_pair(io_context& context)
        : m_client(context)
        , m_server(context)
    {
        socket_guard client;
        socket_guard server;
        spanio::test::make_tcp_pair(client, server);
        SPANIO_REQUIRE(!m_client.assign(client.get()));
        client.release();
        SPANIO_REQUIRE(!m_server.assign(server.get()));
        server.release();
    }

    // Connecting side
    tcp::socket m_client;
    // Accepted side
    tcp::socket m_server;
};

// Both ends of a TLS connection, before the handshake
struct stream_pair
{
    // Parameter context: context of both ends
    // Parameter tls:     settings of both ends
    // Parameter host:    name the client expects
    stream_pair(io_context& context, context_pair& tls, const std::string& host = kHost)
        : stream_pair(socket_pair(context), tls, host)
    {
    }

    // Parameter sockets: connected sockets
    // Parameter tls:     settings of both ends
    // Parameter host:    name the client expects
    stream_pair(socket_pair&& sockets, context_pair& tls, const std::string& host)
        : m_client(std::move(sockets.m_client), tls.m_client)
        , m_server(std::move(sockets.m_server), tls.m_server)
    {
        SPANIO_REQUIRE(!m_client.set_verify_hostname(host));
    }

    // Client end
    ssl::stream m_client;
    // Server end
    ssl::stream m_server;
};

// Run a context until it has no work left, or fail the case after kLimit
// Parameter context: context to run
void run_to_end(io_context& context)
{
    context.restart();
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + kLimit;
    std::promise<void> ended;
    std::future<void> end = ended.get_future();
    std::atomic<bool> timed_out{false};
    auto watch = [&context, &end, &timed_out, deadline]()
    {
        if (end.wait_until(deadline) != std::future_status::ready)
        {
            timed_out = true;
            context.stop();
        }
    };
    std::thread watchdog(watch);
    context.run();
    ended.set_value();
    watchdog.join();
    SPANIO_REQUIRE(!timed_out);
}

// Start a coroutine whose exceptions fail the case through a future
// Parameter context: context to run on
// Parameter body:    the coroutine
// Return value: result of the coroutine
std::future<void> spawn(io_context& context, awaitable<void> body)
{
    return spanio::co_spawn(context, std::move(body), use_future);
}

// Counts the calls of a status handler and keeps the last result
struct status_record
{
    // Calls so far
    int m_calls = 0;
    // Last result
    std::error_code m_ec;
};

// Status handler that fills a record
struct status_handler
{
    // Parameter ec: result
    void operator()(const std::error_code& ec)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
    }

    // Where the result goes
    status_record* m_record;
};

// Transfer handler that fills a record
struct transfer_handler
{
    // Parameter ec: result
    void operator()(const std::error_code& ec, std::size_t /*bytes*/)
    {
        ++m_record->m_calls;
        m_record->m_ec = ec;
    }

    // Where the result goes
    status_record* m_record;
};

// Do the handshake of both ends and report the results
// Parameter context: context of the streams
// Parameter streams: both ends
// Return value: results of the client and the server
std::pair<std::error_code, std::error_code> handshake(io_context& context, stream_pair& streams)
{
    status_record client;
    status_record server;
    streams.m_client.async_handshake(status_handler{&client});
    streams.m_server.async_handshake(status_handler{&server});
    // Never inline
    SPANIO_CHECK(client.m_calls == 0 && server.m_calls == 0);
    run_to_end(context);
    SPANIO_CHECK(client.m_calls == 1 && server.m_calls == 1);
    return std::make_pair(client.m_ec, server.m_ec);
}

// Handshake that must work on both ends
// Parameter context: context of the streams
// Parameter streams: both ends
void establish(io_context& context, stream_pair& streams)
{
    const std::pair<std::error_code, std::error_code> result = handshake(context, streams);
    SPANIO_REQUIRE(!result.first);
    SPANIO_REQUIRE(!result.second);
}

// Server side of an echo: one message, then goodbye
// Parameter stream: connection
// Parameter size:   bytes to echo
awaitable<void> echo_once(ssl::stream& stream, std::size_t size)
{
    co_await stream.async_handshake(use_awaitable);
    std::string data(size, '\0');
    co_await stream.async_read(spanio::buffer(data), use_awaitable);
    co_await stream.async_write(spanio::buffer(data), use_awaitable);
}

// Client side of an echo
// Parameter stream: connection
// Parameter data:   bytes to send
// Parameter reply:  receives the echo
awaitable<void> ask_once(ssl::stream& stream, const std::string& data, std::string& reply)
{
    co_await stream.async_handshake(use_awaitable);
    co_await stream.async_write(spanio::buffer(data), use_awaitable);
    reply.assign(data.size(), '\0');
    co_await stream.async_read(spanio::buffer(reply), use_awaitable);
}

// Echo session that must end with eof; every stage is callback style
class callback_echo
{
public:
    // Parameter streams: both ends
    explicit callback_echo(stream_pair& streams)
        : m_streams(&streams)
        , m_received(kHello.size(), '\0')
        , m_reply(kHello.size(), '\0')
    {
    }

    // Start both ends
    void start()
    {
        auto server_ready = [this](std::error_code ec)
        {
            check(ec);
            read_request();
        };
        auto client_ready = [this](std::error_code ec)
        {
            check(ec);
            send_request();
        };
        m_streams->m_server.async_handshake(server_ready);
        m_streams->m_client.async_handshake(client_ready);
    }

    // What came back
    // Return value: the reply
    const std::string& reply() const
    {
        return m_reply;
    }

    // Whether the reply arrived
    // Return value: true after the last step
    bool done() const
    {
        return m_done;
    }

private:
    // Fail the case on an error
    // Parameter ec: result of a step
    void check(const std::error_code& ec)
    {
        SPANIO_CHECK(!ec);
    }

    // Server: read the message, then send it back
    void read_request()
    {
        auto answer = [this](std::error_code ec, std::size_t /*bytes*/)
        {
            check(ec);
            auto sent = [this](std::error_code write_ec, std::size_t /*written*/)
            {
                check(write_ec);
            };
            m_streams->m_server.async_write(spanio::buffer(m_received), sent);
        };
        m_streams->m_server.async_read(spanio::buffer(m_received), answer);
    }

    // Client: send the message, then read the reply
    void send_request()
    {
        auto wait_reply = [this](std::error_code ec, std::size_t bytes)
        {
            check(ec);
            SPANIO_CHECK(bytes == kHello.size());
            auto arrived = [this](std::error_code read_ec, std::size_t /*read*/)
            {
                check(read_ec);
                m_done = true;
            };
            m_streams->m_client.async_read(spanio::buffer(m_reply), arrived);
        };
        m_streams->m_client.async_write(spanio::buffer(kHello), wait_reply);
    }

    // Both ends
    stream_pair* m_streams;
    // What the server read
    std::string m_received;
    // What the client read
    std::string m_reply;
    // The reply arrived
    bool m_done = false;
};

// Send data and read as much at the same time
// Parameter stream: connection, handshake done
// Parameter data:   bytes to send
// Parameter got:    receives the peer's bytes
// Parameter done:   counts finished directions
void exchange(ssl::stream& stream, const std::string& data, std::string& got, int& done)
{
    got.assign(data.size(), '\0');
    auto finished = [&done](std::error_code ec, std::size_t bytes)
    {
        SPANIO_CHECK(!ec);
        SPANIO_CHECK(bytes == kLargeSize);
        ++done;
    };
    stream.async_read(spanio::buffer(got), finished);
    stream.async_write(spanio::buffer(data), finished);
}

// A file that is removed at the end of the case
class temporary_file
{
public:
    // Parameter name: file name, in the working directory
    // Parameter text: content
    temporary_file(const std::string& name, const std::string& text)
        : m_name(name)
    {
        std::ofstream out(m_name, std::ios::binary);
        out << text;
    }

    // Remove the file
    ~temporary_file()
    {
        std::remove(m_name.c_str());
    }

    temporary_file(const temporary_file&) = delete;
    temporary_file& operator=(const temporary_file&) = delete;

    // Name of the file
    // Return value: path usable in this process
    const std::string& name() const
    {
        return m_name;
    }

private:
    // Path of the file
    std::string m_name;
};

// The tls_echo_server example on its own context and thread
class tls_echo_fixture
{
public:
    // Parameter backend: proactor backend
    // Parameter threads: threads running the context
    explicit tls_echo_fixture(const backend_factory& backend, int threads = 1)
        : m_context(make_context(backend))
        , m_settings(ssl::role::server)
        , m_acceptor(*m_context, tcp::endpoint(spanio::ipv4::loopback(), 0))
    {
        use_identity(m_settings, server_identity());
        tls_echo::start(*m_context, m_acceptor, m_settings);
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

    // Stop serving and join
    ~tls_echo_fixture()
    {
        tcp::acceptor& acceptor = m_acceptor;
        auto close_acceptor = [&acceptor]()
        {
            acceptor.close();
        };
        m_context->post(close_acceptor);
        for (std::thread& thread : m_threads)
        {
            thread.join();
        }
    }

    tls_echo_fixture(const tls_echo_fixture&) = delete;
    tls_echo_fixture& operator=(const tls_echo_fixture&) = delete;

    // Port the server listens on
    // Return value: the port
    std::uint16_t port() const
    {
        std::error_code ec;
        return m_acceptor.local_endpoint(ec).port();
    }

private:
    // Context of the server
    std::unique_ptr<io_context> m_context;
    // Certificate of the server
    ssl::context m_settings;
    // Listening socket
    tcp::acceptor m_acceptor;
    // Threads running the context
    std::vector<std::thread> m_threads;
};

// A client that echoes several times, its steps on a strand
// Parameter context: context to run on
// Parameter port:    server port
// Parameter tls:     client settings
// Parameter seed:    makes the data of each client different
awaitable<void> echo_client(io_context& context, std::uint16_t port, ssl::context& tls, int seed)
{
    tcp::socket socket(context);
    co_await socket.async_connect(tcp::endpoint(spanio::ipv4::loopback(), port), use_awaitable);
    ssl::stream stream(std::move(socket), tls);
    const std::error_code named = stream.set_verify_hostname(kHost);
    SPANIO_CHECK(!named);
    co_await stream.async_handshake(use_awaitable);
    for (int i = 0; i < kEchoes; ++i)
    {
        const std::string data(kEchoSize, static_cast<char>('a' + (seed + i) % 26));
        std::string reply(kEchoSize, '\0');
        co_await stream.async_write(spanio::buffer(data), use_awaitable);
        co_await stream.async_read(spanio::buffer(reply), use_awaitable);
        SPANIO_CHECK(reply == data);
    }
    co_await stream.async_shutdown(use_awaitable);
    // The server answers with its own close message
    char rest[kSmallBuffer];
    auto [ec, bytes] = co_await stream.async_read_some(spanio::buffer(rest), as_tuple(use_awaitable));
    SPANIO_CHECK(ec == misc_errors::eof);
    SPANIO_CHECK(bytes == 0);
}

// Send chunks, asking for new keys now and then
// Parameter stream: connection
// Parameter data:   everything to send
awaitable<void> send_with_updates(ssl::stream& stream, const std::string& data)
{
    for (std::size_t i = 0; i < kUpdateRounds; ++i)
    {
        if (i % kUpdateEvery == 0)
        {
            // One thread runs the context, so the handle may be used here.
            // The peer has to answer, so its read makes it write
            SPANIO_CHECK(::SSL_key_update(stream.native_handle(), SSL_KEY_UPDATE_REQUESTED) == 1);
        }
        co_await stream.async_write(spanio::buffer(data.data() + i * kUpdateChunk, kUpdateChunk), use_awaitable);
    }
}

// Receive everything
// Parameter stream: connection
// Parameter got:    buffer of the expected size
awaitable<void> receive_all(ssl::stream& stream, std::string& got)
{
    co_await stream.async_read(spanio::buffer(got), use_awaitable);
}

// Object that owns a stream and is kept alive by its handlers, the pattern
// that would leak if handlers of a destroyed context were kept
struct self_owner
{
    // Parameter socket: connection
    // Parameter tls:    settings
    // Parameter alive:  counts live objects
    self_owner(tcp::socket&& socket, ssl::context& tls, std::atomic<int>& alive)
        : m_stream(std::move(socket), tls)
        , m_alive(&alive)
    {
        ++*m_alive;
    }

    // Count down
    ~self_owner()
    {
        --*m_alive;
    }

    // The connection
    ssl::stream m_stream;
    // Receive buffer
    char m_data[kSmallBuffer] = {};
    // Number of live objects
    std::atomic<int>* m_alive;
};

}

// Case 1: handshake with a verified certificate, then an echo, once with
// callbacks and once with coroutines
SPANIO_TEST(handshake_and_echo)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        {
            stream_pair streams(*context, tls);
            callback_echo echo(streams);
            echo.start();
            run_to_end(*context);
            SPANIO_CHECK(echo.done());
            SPANIO_CHECK(echo.reply() == kHello);
        }
        stream_pair streams(*context, tls);
        std::string reply;
        std::future<void> server = spawn(*context, echo_once(streams.m_server, kHello.size()));
        std::future<void> client = spawn(*context, ask_once(streams.m_client, kHello, reply));
        run_to_end(*context);
        server.get();
        client.get();
        SPANIO_CHECK(reply == kHello);
    }
}

// Case 2: 1 MB each way, one direction after the other, on TLS 1.2 and 1.3
SPANIO_TEST(large_transfer)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        for (const ssl::tls_version version : {ssl::tls_version::tls1_2, ssl::tls_version::tls1_3})
        {
            std::unique_ptr<io_context> context = make_context(backend);
            context_pair tls;
            SPANIO_REQUIRE(!tls.m_client.set_max_version(version));
            SPANIO_REQUIRE(!tls.m_client.set_min_version(version));
            stream_pair streams(*context, tls);
            const std::string data = pattern(kLargeSize);
            std::string reply;
            std::future<void> server = spawn(*context, echo_once(streams.m_server, data.size()));
            std::future<void> client = spawn(*context, ask_once(streams.m_client, data, reply));
            run_to_end(*context);
            server.get();
            client.get();
            SPANIO_CHECK(reply == data);
            SPANIO_CHECK(::SSL_version(streams.m_client.native_handle()) == static_cast<int>(version));
        }
    }
}

// Case 3: an untrusted, an expired and a wrongly named certificate fail the
// handshake with the reason; an IP address in the certificate is accepted
SPANIO_TEST(verify_failures)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        const identity stranger = make_identity("other authority", nullptr);
        const identity untrusted = make_identity(kHost, &stranger);
        const identity expired = make_identity(kHost, &authority(), -kDay - kDay, -kDay);

        // Certificate, expected name, expected reason
        const std::vector<std::tuple<const identity*, std::string, int>> table = {
            {&untrusted, kHost, X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY},
            {&expired, kHost, X509_V_ERR_CERT_HAS_EXPIRED},
            {&server_identity(), "other.test", X509_V_ERR_HOSTNAME_MISMATCH},
            {&server_identity(), "127.0.0.2", X509_V_ERR_IP_ADDRESS_MISMATCH},
        };
        for (const auto& [who, host, reason] : table)
        {
            context_pair tls(*who);
            stream_pair streams(*context, tls, host);
            const std::pair<std::error_code, std::error_code> result = handshake(*context, streams);
            SPANIO_CHECK(result.first == std::error_code(reason, spanio::error::ssl_verify_category()));
            SPANIO_CHECK(!result.first.message().empty());
            // The alert tells the server why
            SPANIO_CHECK(result.second.category() == spanio::error::ssl_category());
        }

        context_pair tls;
        stream_pair by_address(*context, tls, "127.0.0.1");
        establish(*context, by_address);
        stream_pair unnamed(*context, tls);
        SPANIO_CHECK(unnamed.m_client.set_verify_hostname("") == std::errc::invalid_argument);
    }
}

// Case 4: after async_shutdown the peer reads eof and may still send; a TCP
// end without the close message is stream_truncated
SPANIO_TEST(closing)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        char data[kSmallBuffer] = {};
        {
            stream_pair streams(*context, tls);
            establish(*context, streams);
            status_record shut;
            status_record read;
            streams.m_client.async_shutdown(status_handler{&shut});
            streams.m_server.async_read_some(spanio::buffer(data), transfer_handler{&read});
            run_to_end(*context);
            SPANIO_CHECK(shut.m_calls == 1 && !shut.m_ec);
            SPANIO_CHECK(read.m_calls == 1 && read.m_ec == misc_errors::eof);

            // The other direction is still open
            status_record sent;
            status_record late;
            streams.m_server.async_write(spanio::buffer(kHello), transfer_handler{&sent});
            streams.m_client.async_read_some(spanio::buffer(data), transfer_handler{&late});
            run_to_end(*context);
            SPANIO_CHECK(!sent.m_ec && !late.m_ec);
            SPANIO_CHECK(std::string(data, kHello.size()) == kHello);
        }
        stream_pair streams(*context, tls);
        establish(*context, streams);
        status_record read;
        streams.m_server.async_read_some(spanio::buffer(data), transfer_handler{&read});
        streams.m_client.close();
        run_to_end(*context);
        SPANIO_CHECK(read.m_calls == 1 && read.m_ec == ssl_errors::stream_truncated);
    }
}

// Case 5: closing during the handshake, and with a read and a write pending,
// completes each operation once with operation_canceled; later operations
// fail with bad_file_descriptor
SPANIO_TEST(cancellation)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        {
            stream_pair streams(*context, tls);
            status_record shaken;
            streams.m_server.async_handshake(status_handler{&shaken});
            streams.m_server.close();
            SPANIO_CHECK(shaken.m_calls == 0);
            run_to_end(*context);
            SPANIO_CHECK(shaken.m_calls == 1 && shaken.m_ec == std::errc::operation_canceled);
        }
        stream_pair streams(*context, tls);
        establish(*context, streams);
        char data[kSmallBuffer];
        const std::string large = pattern(kLargeSize * kPatternPeriod / kSmallBuffer);
        status_record read;
        status_record second;
        status_record write;
        streams.m_client.async_read_some(spanio::buffer(data), transfer_handler{&read});
        streams.m_client.async_read_some(spanio::buffer(data), transfer_handler{&second});
        // Nobody reads this, so it cannot finish
        streams.m_client.async_write(spanio::buffer(large), transfer_handler{&write});
        ssl::stream& client = streams.m_client;
        auto close_client = [&client](std::error_code /*ec*/)
        {
            client.close();
        };
        spanio::steady_timer timer(*context, std::chrono::milliseconds(kSmallBuffer));
        timer.async_wait(close_client);
        run_to_end(*context);
        SPANIO_CHECK(read.m_calls == 1 && read.m_ec == std::errc::operation_canceled);
        SPANIO_CHECK(second.m_calls == 1 && second.m_ec == std::errc::operation_in_progress);
        SPANIO_CHECK(write.m_calls == 1 && write.m_ec == std::errc::operation_canceled);

        status_record late;
        streams.m_client.async_read_some(spanio::buffer(data), transfer_handler{&late});
        SPANIO_CHECK(late.m_calls == 0);
        run_to_end(*context);
        SPANIO_CHECK(late.m_calls == 1 && late.m_ec == std::errc::bad_file_descriptor);
    }
}

// Case 6: a read and a write pending on both ends at once, 1 MB each way
SPANIO_TEST(full_duplex)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        stream_pair streams(*context, tls);
        establish(*context, streams);
        const std::string data = pattern(kLargeSize);
        std::string at_client;
        std::string at_server;
        int done = 0;
        exchange(streams.m_client, data, at_client, done);
        exchange(streams.m_server, data, at_server, done);
        run_to_end(*context);
        SPANIO_CHECK(done == 4);
        SPANIO_CHECK(at_client == data);
        SPANIO_CHECK(at_server == data);
    }
}

// Case 7: bad files, keys and PEM text are reported as error codes
SPANIO_TEST(context_errors)
{
    ssl::context settings(ssl::role::server);
    SPANIO_CHECK(settings.use_certificate_chain_file("no such file.pem") == std::errc::no_such_file_or_directory);
    SPANIO_CHECK(settings.use_private_key_file("no such file.pem") == std::errc::no_such_file_or_directory);
    SPANIO_CHECK(settings.load_verify_file("no such file.pem") == std::errc::no_such_file_or_directory);

    const std::string broken = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    for (const std::string& text : {std::string(), std::string("not PEM"), broken})
    {
        const std::error_code certificate_ec = settings.use_certificate(text);
        const std::error_code key_ec = settings.use_private_key(text);
        const std::error_code authority_ec = settings.add_certificate_authority(text);
        SPANIO_CHECK(certificate_ec && !certificate_ec.message().empty());
        SPANIO_CHECK(key_ec && !key_ec.message().empty());
        SPANIO_CHECK(static_cast<bool>(authority_ec));
    }

    // The certificate comes first
    ssl::context no_certificate(ssl::role::server);
    SPANIO_CHECK(static_cast<bool>(no_certificate.use_private_key(server_identity().m_key_pem)));

    // A key of another certificate
    SPANIO_REQUIRE(!settings.use_certificate(server_identity().m_certificate_pem));
    SPANIO_CHECK(settings.use_private_key(authority().m_key_pem).category() == spanio::error::ssl_category());

    // An encrypted key fails instead of asking for its password
    const ssl_ptr<BIO> text(::BIO_new(::BIO_s_mem()));
    const std::string password = "secret";
    SPANIO_REQUIRE(::PEM_write_bio_PrivateKey(text.get(), server_identity().m_key.get(), ::EVP_aes_128_cbc(),
                                              reinterpret_cast<const unsigned char*>(password.data()),
                                              static_cast<int>(password.size()), nullptr, nullptr)
                   == 1);
    SPANIO_CHECK(static_cast<bool>(settings.use_private_key(text_of(text.get()))));

    // Failures leave the context usable
    SPANIO_CHECK(!settings.use_private_key(server_identity().m_key_pem));
}

// Case 8: destroying the io_context with operations pending leaks nothing,
// even when the handlers own the streams
SPANIO_TEST(context_destroyed)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::atomic<int> alive{0};
        // One pending handshake, then one pending read after a handshake
        for (const bool established : {false, true})
        {
            std::unique_ptr<io_context> context = make_context(backend);
            context_pair tls;
            socket_pair sockets(*context);
            std::shared_ptr<self_owner> client =
                std::make_shared<self_owner>(std::move(sockets.m_client), tls.m_client, alive);
            std::shared_ptr<self_owner> server =
                std::make_shared<self_owner>(std::move(sockets.m_server), tls.m_server, alive);
            if (established)
            {
                SPANIO_REQUIRE(!client->m_stream.set_verify_hostname(kHost));
                status_record client_done;
                status_record server_done;
                client->m_stream.async_handshake(status_handler{&client_done});
                server->m_stream.async_handshake(status_handler{&server_done});
                run_to_end(*context);
                SPANIO_REQUIRE(!client_done.m_ec && !server_done.m_ec);
                context->restart();
                auto keep_client = [client](std::error_code /*ec*/, std::size_t /*bytes*/)
                {
                };
                auto keep_server = [server](std::error_code /*ec*/, std::size_t /*bytes*/)
                {
                };
                client->m_stream.async_read_some(spanio::buffer(client->m_data), std::move(keep_client));
                server->m_stream.async_read_some(spanio::buffer(server->m_data), std::move(keep_server));
            }
            else
            {
                auto keep_server = [server](std::error_code /*ec*/)
                {
                };
                server->m_stream.async_handshake(std::move(keep_server));
            }
            // As documented for io_context: sockets are closed first
            client->m_stream.close();
            server->m_stream.close();
            client.reset();
            server.reset();
            context.reset();
            SPANIO_CHECK(alive == 0);
        }
    }
}

// Case 9: handshake, write and read through futures
SPANIO_TEST(futures)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        stream_pair streams(*context, tls);
        std::string data(kHello.size(), '\0');
        std::future<void> client = streams.m_client.async_handshake(use_future);
        std::future<void> server = streams.m_server.async_handshake(use_future);
        std::future<std::size_t> written = streams.m_client.async_write(spanio::buffer(kHello), use_future);
        std::future<std::size_t> read = streams.m_server.async_read(spanio::buffer(data), use_future);
        run_to_end(*context);
        client.get();
        server.get();
        SPANIO_CHECK(written.get() == kHello.size());
        SPANIO_CHECK(read.get() == kHello.size());
        SPANIO_CHECK(data == kHello);

        std::future<void> shut = streams.m_client.async_shutdown(use_future);
        std::future<std::size_t> last = streams.m_server.async_read_some(spanio::buffer(data), use_future);
        run_to_end(*context);
        shut.get();
        bool thrown = false;
        try
        {
            last.get();
        }
        catch (const std::system_error& e)
        {
            thrown = e.code() == misc_errors::eof;
        }
        SPANIO_CHECK(thrown);
    }
}

// Case 10: several connections on several threads, clients on strands
SPANIO_TEST(threads_and_strands)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        tls_echo_fixture server(backend, kThreads);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        std::vector<std::future<void>> clients;
        for (int i = 0; i < kConnections; ++i)
        {
            clients.push_back(spanio::co_spawn(spanio::make_strand(*context),
                                               echo_client(*context, server.port(), tls.m_client, i), use_future));
        }
        io_context& running = *context;
        auto run_context = [&running]()
        {
            running.run();
        };
        std::vector<std::thread> threads;
        for (int i = 0; i < kThreads; ++i)
        {
            threads.emplace_back(run_context);
        }
        for (std::thread& thread : threads)
        {
            thread.join();
        }
        for (std::future<void>& client : clients)
        {
            client.get();
        }
    }
}

// Case 11: the examples talk to each other; the client refuses an unknown
// authority, times out on a silent server and checks its command line
SPANIO_TEST(examples)
{
    const temporary_file authority_file("test_ssl_ca.pem", authority().m_certificate_pem);
    const temporary_file stranger_file("test_ssl_other.pem", make_identity("other", nullptr).m_certificate_pem);
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        tls_echo_fixture server(backend);
        tls_client_example::options opts;
        opts.m_host = "127.0.0.1";
        opts.m_port = std::to_string(static_cast<unsigned>(server.port()));
        opts.m_message = kHello;
        opts.m_authority = authority_file.name();
        std::string reply;
        SPANIO_CHECK(!tls_client_example::run_tls_client(opts, reply, kLimit));
        SPANIO_CHECK(reply == kHello);
        opts.m_authority = stranger_file.name();
        const std::error_code refused = tls_client_example::run_tls_client(opts, reply, kLimit);
        SPANIO_CHECK(refused.category() == spanio::error::ssl_verify_category());
    }

    socket_guard silent(spanio::test::make_loopback_listener());
    tls_client_example::options opts;
    opts.m_host = "127.0.0.1";
    opts.m_port = std::to_string(
        static_cast<unsigned>(ntohs(spanio::test::local_address(silent.get()).sin_port)));
    opts.m_message = kHello;
    opts.m_authority = authority_file.name();
    std::string reply;
    SPANIO_CHECK(tls_client_example::run_tls_client(opts, reply, std::chrono::milliseconds(kPatternPeriod))
                 == std::errc::timed_out);

    const std::vector<std::vector<const char*>> bad_client = {
        {"c"}, {"c", "h", "1"}, {"c", "h", "0", "m"}, {"c", "h", "70000", "m"}, {"c", "h", "x", "m"},
        {"c", "", "1", "m"}, {"c", "h", "1", ""}, {"c", "h", "1", "m", "ca", "extra"}};
    std::string error;
    for (const std::vector<const char*>& args : bad_client)
    {
        SPANIO_CHECK(!tls_client_example::parse_arguments(static_cast<int>(args.size()), args.data(), opts, error));
    }
    const std::vector<const char*> good_client = {"c", "h", "1", "m", "ca"};
    SPANIO_CHECK(tls_client_example::parse_arguments(static_cast<int>(good_client.size()), good_client.data(), opts,
                                                     error));
    SPANIO_CHECK(opts.m_authority == "ca");

    tls_echo::options server_opts;
    const std::vector<std::vector<const char*>> bad_server = {
        {"s"}, {"s", "cert"}, {"s", "cert", "key", "0"}, {"s", "cert", "key", "1x"}, {"s", "a", "b", "1", "extra"}};
    for (const std::vector<const char*>& args : bad_server)
    {
        SPANIO_CHECK(!tls_echo::parse_arguments(static_cast<int>(args.size()), args.data(), server_opts, error));
    }
    const std::vector<const char*> good_server = {"s", "cert", "key"};
    SPANIO_CHECK(tls_echo::parse_arguments(static_cast<int>(good_server.size()), good_server.data(), server_opts,
                                           error));
    SPANIO_CHECK(server_opts.m_port == tls_echo::kDefaultPort);
}

// Case 12: TLS 1.3 key updates in the middle of a full-duplex transfer do
// not disturb the data
SPANIO_TEST(key_update)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        stream_pair streams(*context, tls);
        establish(*context, streams);
        SPANIO_REQUIRE(::SSL_version(streams.m_client.native_handle()) == TLS1_3_VERSION);
        const std::string data = pattern(kUpdateChunk * kUpdateRounds);
        std::string at_client(data.size(), '\0');
        std::string at_server(data.size(), '\0');
        std::vector<std::future<void>> steps;
        steps.push_back(spawn(*context, send_with_updates(streams.m_client, data)));
        steps.push_back(spawn(*context, send_with_updates(streams.m_server, data)));
        steps.push_back(spawn(*context, receive_all(streams.m_client, at_client)));
        steps.push_back(spawn(*context, receive_all(streams.m_server, at_server)));
        run_to_end(*context);
        for (std::future<void>& step : steps)
        {
            step.get();
        }
        SPANIO_CHECK(at_client == data);
        SPANIO_CHECK(at_server == data);
    }
}

// Case 13: a client that does not speak TLS fails its own handshake only
SPANIO_TEST(peer_without_tls)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        {
            std::unique_ptr<io_context> context = make_context(backend);
            context_pair tls;
            socket_pair sockets(*context);
            ssl::stream server(std::move(sockets.m_server), tls.m_server);
            status_record shaken;
            status_record sent;
            server.async_handshake(status_handler{&shaken});
            const std::string garbage = "GET / HTTP/1.0\r\n\r\n";
            sockets.m_client.async_write(spanio::buffer(garbage), transfer_handler{&sent});
            run_to_end(*context);
            SPANIO_CHECK(shaken.m_calls == 1);
            SPANIO_CHECK(shaken.m_ec.category() == spanio::error::ssl_category());

            // The failed stream reports errors instead of hanging
            char data[kSmallBuffer];
            status_record read;
            server.async_read_some(spanio::buffer(data), transfer_handler{&read});
            run_to_end(*context);
            SPANIO_CHECK(read.m_calls == 1 && read.m_ec);
        }

        tls_echo_fixture server(backend);
        socket_guard noise;
        SPANIO_REQUIRE(spanio::test::connect_client(server.port(), noise));
        const std::string garbage = pattern(kSmallBuffer);
        SPANIO_REQUIRE(spanio::test::send_all(noise.get(), garbage.data(), garbage.size()));

        std::unique_ptr<io_context> context = make_context(backend);
        context_pair tls;
        std::future<void> good = spawn(*context, echo_client(*context, server.port(), tls.m_client, 0));
        run_to_end(*context);
        good.get();
    }
}

// Case 14: no common version fails the handshake; below TLS 1.2 is refused
// by default
SPANIO_TEST(version_limits)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        std::unique_ptr<io_context> context = make_context(backend);
        {
            context_pair tls;
            SPANIO_REQUIRE(!tls.m_client.set_max_version(ssl::tls_version::tls1_2));
            SPANIO_REQUIRE(!tls.m_server.set_min_version(ssl::tls_version::tls1_3));
            stream_pair streams(*context, tls);
            const std::pair<std::error_code, std::error_code> result = handshake(*context, streams);
            SPANIO_CHECK(result.first && result.second);
        }
        context_pair tls;
        SPANIO_CHECK(SSL_CTX_get_min_proto_version(tls.m_client.native_handle()) == TLS1_2_VERSION);
        SPANIO_CHECK(SSL_CTX_get_min_proto_version(tls.m_server.native_handle()) == TLS1_2_VERSION);
        // Force an old client past its own default
        SSL_CTX_set_min_proto_version(tls.m_client.native_handle(), TLS1_VERSION);
        SPANIO_REQUIRE(SSL_CTX_set_max_proto_version(tls.m_client.native_handle(), TLS1_1_VERSION) == 1);
        stream_pair streams(*context, tls);
        const std::pair<std::error_code, std::error_code> result = handshake(*context, streams);
        SPANIO_CHECK(result.first && result.second);
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
