// ============================================================================
// File purpose: TLS client example: resolves a host name, connects, verifies
//               the server's certificate and name, sends a message and reads
//               back as many bytes as it sent. Shows ssl::context,
//               ssl::stream and use_future: the calling thread waits for each
//               step while another one runs the io_context.
// Usage:        tls_client <host> <port> <message> [ca-file]
//                 host     name or numeric address the certificate is for
//                 port     1-65535
//                 message  text to send; the reply must be as long
//                 ca-file  PEM file of the authority to trust (default:
//                          OpenSSL's default location)
//               Try it against tls_echo_server:
//                 tls_echo_server cert.pem key.pem 9443
//                 tls_client localhost 9443 hello cert.pem   -> prints "hello"
//               The exchange is limited to 5 seconds.
//
//               Needs OpenSSL; CMake builds it with -DSPANIO_ENABLE_SSL=ON.
//               Single-file build on Linux, from the repository root:
//                 g++ -std=c++17 -Iinclude examples/tls_client.cpp -o tls_client -lssl -lcrypto -lpthread
//
//               The unit tests include this file with
//               SPANIO_TLS_CLIENT_NO_MAIN defined and call run_tls_client().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-25
// ============================================================================

#include "spanio/async_result.h"
#include "spanio/io_context.h"
#include "spanio/resolver.h"
#include "spanio/ssl.h"
#include "spanio/tcp.h"

#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <future>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace tls_client_example {

// Longest the whole exchange may take when run from the command line
constexpr std::chrono::milliseconds kDefaultLimit(5000);

// Fewest command-line arguments, including the program name
constexpr int kMinArgumentCount = 4;

// Most command-line arguments: the trusted authority is optional
constexpr int kMaxArgumentCount = 5;

// Position of the host argument in argv
constexpr int kHostArgument = 1;

// Position of the port argument in argv
constexpr int kPortArgument = 2;

// Position of the message argument in argv
constexpr int kMessageArgument = 3;

// Position of the optional ca-file argument in argv
constexpr int kAuthorityArgument = 4;

// Smallest port a client may connect to
constexpr unsigned long kMinPort = 1;

// Largest TCP port
constexpr unsigned long kMaxPort = 65535;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: the reply arrived
constexpr int kExitSuccess = 0;

// Process exit code: a step of the exchange failed
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Settings taken from the command line
struct options
{
    // Host name or numeric address
    std::string m_host;
    // Port, as the decimal text the resolver takes
    std::string m_port;
    // Text to send
    std::string m_message;
    // PEM file of the authority to trust; empty for the default ones
    std::string m_authority;
};

// Read the command line: tls_client <host> <port> <message> [ca-file]
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    if (argc < kMinArgumentCount || argc > kMaxArgumentCount)
    {
        error = "expected <host> <port> <message> [ca-file]";
        return false;
    }
    const char* port = argv[kPortArgument];
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(port, &end, kDecimalBase);
    if (!std::isdigit(static_cast<unsigned char>(port[0])) || errno != 0 || *end != '\0' || parsed < kMinPort
        || parsed > kMaxPort)
    {
        error = "port must be a number from 1 to 65535";
        return false;
    }
    out.m_host = argv[kHostArgument];
    out.m_port = port;
    out.m_message = argv[kMessageArgument];
    out.m_authority = argc == kMaxArgumentCount ? argv[kAuthorityArgument] : "";
    if (out.m_host.empty() || out.m_message.empty())
    {
        error = "host and message must not be empty";
        return false;
    }
    return true;
}

// Runs an io_context on a thread of its own; the destructor stops it, which
// also abandons a step that did not finish in time
class context_runner
{
public:
    // Parameter context: context to run; must outlive this object
    explicit context_runner(spanio::io_context& context)
        : m_context(&context)
        , m_guard(spanio::make_work_guard(context))
    {
        spanio::io_context* const target = m_context;
        auto run_context = [target]()
        {
            target->run();
        };
        m_thread = std::thread(run_context);
    }

    // Stop the context and join the thread
    ~context_runner()
    {
        m_context->stop();
        m_thread.join();
    }

    context_runner(const context_runner&) = delete;
    context_runner& operator=(const context_runner&) = delete;

private:
    // Context being run
    spanio::io_context* m_context;
    // Keeps run() going between two steps
    spanio::executor_work_guard m_guard;
    // Thread calling run()
    std::thread m_thread;
};

// Wait for a step, at most until a deadline
// Parameter step:     result of the step
// Parameter deadline: when to give up
// Return value: the value of the step; throws std::system_error when the
//               step failed, with errc::timed_out when the deadline passed
template <typename T>
T wait_for(std::future<T> step, std::chrono::steady_clock::time_point deadline)
{
    if (step.wait_until(deadline) != std::future_status::ready)
    {
        throw std::system_error(std::make_error_code(std::errc::timed_out));
    }
    return step.get();
}

// Throw if a setup call failed
// Parameter ec: result of the call
inline void check(const std::error_code& ec)
{
    if (ec)
    {
        throw std::system_error(ec);
    }
}

// Run one exchange to the end
// Parameter opts:  what to connect to and send
// Parameter reply: receives the bytes read back
// Parameter limit: longest the exchange may take
// Return value: empty on success, errc::timed_out when the limit passed,
//               otherwise the error of the failed step
inline std::error_code run_tls_client(const options& opts, std::string& reply, std::chrono::milliseconds limit)
{
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + limit;
    // Outlives the context below, whose thread writes to it
    std::string received(opts.m_message.size(), '\0');
    try
    {
        spanio::ssl::context tls(spanio::ssl::role::client);
        check(opts.m_authority.empty() ? tls.set_default_verify_paths() : tls.load_verify_file(opts.m_authority));

        spanio::io_context context;
        spanio::tcp::resolver resolver(context);
        spanio::tcp::socket socket(context);
        context_runner runner(context);

        std::vector<spanio::tcp::endpoint> endpoints =
            wait_for(resolver.async_resolve(opts.m_host, opts.m_port, spanio::use_future), deadline);
        wait_for(spanio::tcp::async_connect(socket, std::move(endpoints), spanio::use_future), deadline);

        spanio::ssl::stream stream(std::move(socket), tls);
        check(stream.set_verify_hostname(opts.m_host));
        wait_for(stream.async_handshake(spanio::use_future), deadline);
        wait_for(stream.async_write(spanio::buffer(opts.m_message), spanio::use_future), deadline);
        wait_for(stream.async_read(spanio::buffer(received), spanio::use_future), deadline);
        reply = received;
        // Say goodbye; the reply counts whether or not that works
        std::future<void> closed = stream.async_shutdown(spanio::use_future);
        closed.wait_until(deadline);
    }
    catch (const std::system_error& e)
    {
        return e.code();
    }
    return std::error_code();
}

}

#if !defined(SPANIO_TLS_CLIENT_NO_MAIN)

// Parse the command line, run one exchange and print the reply
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               exchange failed
int main(int argc, char** argv)
{
    tls_client_example::options opts;
    std::string error;
    if (!tls_client_example::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "tls_client: %s\nusage: tls_client <host> <port> <message> [ca-file]\n", error.c_str());
        return tls_client_example::kExitUsage;
    }
    try
    {
        std::string reply;
        const std::error_code ec =
            tls_client_example::run_tls_client(opts, reply, tls_client_example::kDefaultLimit);
        if (ec)
        {
            std::fprintf(stderr, "tls_client: %s\n", ec.message().c_str());
            return tls_client_example::kExitFailure;
        }
        std::printf("%s\n", reply.c_str());
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "tls_client: %s\n", e.what());
        return tls_client_example::kExitFailure;
    }
    return tls_client_example::kExitSuccess;
}

#endif
