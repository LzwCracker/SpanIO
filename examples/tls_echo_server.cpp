// ============================================================================
// File purpose: TLS echo server written with C++20 coroutines: one coroutine
//               accepts, one per connection does the handshake and sends back
//               what it receives. Shows ssl::context and ssl::stream.
// Usage:        tls_echo_server <certificate> <key> [port]
//                 certificate  PEM file: the server's certificate, followed
//                              by any intermediate ones
//                 key          PEM file: its private key, not encrypted
//                 port         listening port, 1-65535 (default 9443)
//               The server listens on 0.0.0.0, runs on one thread and prints
//               why a connection failed. A certificate for trying it out:
//                 openssl req -x509 -newkey rsa:2048 -nodes -days 30
//                     -subj /CN=localhost -addext subjectAltName=DNS:localhost
//                     -keyout key.pem -out cert.pem          (one line)
//                 tls_client localhost 9443 hello cert.pem
//               Like the other echo examples it sets no time limits: a client
//               that never finishes the handshake keeps its connection.
//
//               Needs C++20 and OpenSSL; CMake builds it with
//               -DSPANIO_ENABLE_SSL=ON. Single-file build on Linux:
//                 g++ -std=c++20 -Iinclude examples/tls_echo_server.cpp -o tls_echo_server -lssl -lcrypto -lpthread
//
//               The unit tests include this file with
//               SPANIO_TLS_ECHO_SERVER_NO_MAIN defined.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-25
// ============================================================================

#include "spanio/coroutine.h"
#include "spanio/io_context.h"
#include "spanio/ssl.h"
#include "spanio/tcp.h"

#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <system_error>
#include <utility>

namespace tls_echo {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9443;

// Smallest port a server may listen on (0 would mean "any port")
constexpr unsigned long kMinPort = 1;

// Largest TCP port
constexpr unsigned long kMaxPort = 65535;

// Fewest command-line arguments, including the program name
constexpr int kMinArgumentCount = 3;

// Most command-line arguments: the port is optional
constexpr int kMaxArgumentCount = 4;

// Position of the certificate argument in argv
constexpr int kCertificateArgument = 1;

// Position of the key argument in argv
constexpr int kKeyArgument = 2;

// Position of the optional port argument in argv
constexpr int kPortArgument = 3;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Bytes read from a connection at a time
constexpr std::size_t kChunkSize = 4096;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Settings taken from the command line
struct options
{
    // PEM file of the certificate chain
    std::string m_certificate;
    // PEM file of the private key
    std::string m_key;
    // Port to listen on
    std::uint16_t m_port = kDefaultPort;
};

// Read the command line: tls_echo_server <certificate> <key> [port]
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    if (argc < kMinArgumentCount || argc > kMaxArgumentCount)
    {
        error = "expected <certificate> <key> [port]";
        return false;
    }
    options parsed;
    parsed.m_certificate = argv[kCertificateArgument];
    parsed.m_key = argv[kKeyArgument];
    if (argc == kMaxArgumentCount)
    {
        const char* port = argv[kPortArgument];
        errno = 0;
        char* end = nullptr;
        const unsigned long value = std::strtoul(port, &end, kDecimalBase);
        if (!std::isdigit(static_cast<unsigned char>(port[0])) || errno != 0 || *end != '\0' || value < kMinPort
            || value > kMaxPort)
        {
            error = "port must be a number from 1 to 65535";
            return false;
        }
        parsed.m_port = static_cast<std::uint16_t>(value);
    }
    out = parsed;
    return true;
}

// Serve one connection until the client closes it
// Parameter stream: the connection; owned by this coroutine
inline spanio::awaitable<void> session(spanio::ssl::stream stream)
{
    try
    {
        co_await stream.async_handshake(spanio::use_awaitable);
        char data[kChunkSize];
        for (;;)
        {
            auto [ec, bytes] =
                co_await stream.async_read_some(spanio::buffer(data), spanio::as_tuple(spanio::use_awaitable));
            if (ec == spanio::error::misc_errors::eof)
            {
                // The client said goodbye: answer, then close
                co_await stream.async_shutdown(spanio::use_awaitable);
                co_return;
            }
            if (ec)
            {
                throw std::system_error(ec);
            }
            co_await stream.async_write(spanio::buffer(data, bytes), spanio::use_awaitable);
        }
    }
    catch (const std::system_error& e)
    {
        // One bad connection must not affect the others
        std::fprintf(stderr, "tls_echo_server: connection failed: %s\n", e.code().message().c_str());
    }
}

// Accept connections until the acceptor is closed, starting a session
// coroutine for each
// Parameter acceptor: listening acceptor; must outlive the coroutine
// Parameter settings: certificate and key; must outlive the coroutine
inline spanio::awaitable<void> accept_loop(spanio::tcp::acceptor& acceptor, spanio::ssl::context& settings)
{
    for (;;)
    {
        auto [ec, peer] = co_await acceptor.async_accept(spanio::as_tuple(spanio::use_awaitable));
        if (ec)
        {
            // operation_canceled: the acceptor was closed, so the server stops
            if (ec != std::errc::operation_canceled)
            {
                std::fprintf(stderr, "tls_echo_server: accept failed: %s\n", ec.message().c_str());
            }
            co_return;
        }
        spanio::io_context& context = peer.context();
        spanio::co_spawn(context, session(spanio::ssl::stream(std::move(peer), settings)), spanio::detached);
    }
}

// Start serving on a listening acceptor; the caller runs the context
// Parameter context:  context of the acceptor
// Parameter acceptor: listening acceptor; must outlive the server
// Parameter settings: certificate and key; must outlive the server
inline void start(spanio::io_context& context, spanio::tcp::acceptor& acceptor, spanio::ssl::context& settings)
{
    spanio::co_spawn(context, accept_loop(acceptor, settings), spanio::detached);
}

}

#if !defined(SPANIO_TLS_ECHO_SERVER_NO_MAIN)

// Parse the command line and run the server
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               server could not start
int main(int argc, char** argv)
{
    tls_echo::options opts;
    std::string error;
    if (!tls_echo::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "tls_echo_server: %s\nusage: tls_echo_server <certificate> <key> [port]\n",
                     error.c_str());
        return tls_echo::kExitUsage;
    }
    try
    {
        spanio::ssl::context settings(spanio::ssl::role::server);
        std::error_code ec = settings.use_certificate_chain_file(opts.m_certificate);
        if (!ec)
        {
            ec = settings.use_private_key_file(opts.m_key);
        }
        if (ec)
        {
            std::fprintf(stderr, "tls_echo_server: certificate or key: %s\n", ec.message().c_str());
            return tls_echo::kExitFailure;
        }
        spanio::io_context context;
        spanio::tcp::acceptor acceptor(context, spanio::tcp::endpoint(spanio::ipv4::any(), opts.m_port));
        tls_echo::start(context, acceptor, settings);
        std::printf("tls_echo_server on port %u, backend %s\n", static_cast<unsigned>(opts.m_port),
                    context.backend_name());
        std::fflush(stdout);
        context.run();
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "tls_echo_server: %s\n", e.what());
        return tls_echo::kExitFailure;
    }
    return tls_echo::kExitSuccess;
}

#endif
