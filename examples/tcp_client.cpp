// ============================================================================
// File purpose: TCP client example: resolves a host name, connects to the
//               first address that accepts, sends a message and reads back as
//               many bytes as it sent (the reply of an echo server). Shows
//               tcp::resolver, tcp::async_connect over an endpoint list,
//               async_write, async_read and a steady_timer limiting the whole
//               exchange.
// Usage:        tcp_client <host> <port> <message>
//                 host     name or numeric address, e.g. localhost
//                 port     1-65535
//                 message  text to send; the reply must be as long
//               Try it against the echo server:
//                 echo_server 8080
//                 tcp_client localhost 8080 hello      -> prints "hello"
//               The exchange is limited to 5 seconds; after that the client
//               gives up with "timed out".
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/tcp_client.cpp -o tcp_client -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\tcp_client.cpp ws2_32.lib
//
//               The unit tests include this file with
//               SPANIO_TCP_CLIENT_NO_MAIN defined and call run_tcp_client().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

#include "spanio/spanio.h"

#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <system_error>
#include <vector>

namespace tcp_client_example {

// Longest the whole exchange may take when run from the command line
constexpr std::chrono::milliseconds kDefaultLimit(5000);

// Number of command-line arguments, including the program name
constexpr int kArgumentCount = 4;

// Position of the host argument in argv
constexpr int kHostArgument = 1;

// Position of the port argument in argv
constexpr int kPortArgument = 2;

// Position of the message argument in argv
constexpr int kMessageArgument = 3;

// Smallest port a client may connect to
constexpr unsigned long kMinPort = 1;

// Largest TCP port
constexpr unsigned long kMaxPort = 65535;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: the reply arrived
constexpr int kExitSuccess = 0;

// Process exit code: resolving, connecting, sending or receiving failed
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
};

// Read the command line: tcp_client <host> <port> <message>
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    if (argc != kArgumentCount)
    {
        error = "expected <host> <port> <message>";
        return false;
    }
    const char* port = argv[kPortArgument];
    if (!std::isdigit(static_cast<unsigned char>(port[0])))
    {
        error = "port must be a number from 1 to 65535";
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(port, &end, kDecimalBase);
    if (errno != 0 || *end != '\0' || parsed < kMinPort || parsed > kMaxPort)
    {
        error = "port must be a number from 1 to 65535";
        return false;
    }
    const std::string message = argv[kMessageArgument];
    if (message.empty() || std::string(argv[kHostArgument]).empty())
    {
        error = "host and message must not be empty";
        return false;
    }
    out.m_host = argv[kHostArgument];
    out.m_port = port;
    out.m_message = message;
    return true;
}

// One exchange: resolve, connect, send, read the reply. Every step starts
// the next from its handler; a timer limits the whole chain. All handlers
// run on the thread that calls run(), so the members need no lock.
class client_session
{
public:
    // Parameter context: context to run on
    // Parameter reply:   receives the bytes read back
    client_session(spanio::io_context& context, std::string& reply)
        : m_resolver(context)
        , m_socket(context)
        , m_timer(context)
        , m_reply(&reply)
        , m_done(false)
        , m_timed_out(false)
    {
    }

    // Start the exchange
    // Parameter host:    host name or numeric address
    // Parameter port:    port as decimal text
    // Parameter message: bytes to send; must stay valid until the end
    // Parameter limit:   longest the exchange may take
    void start(const std::string& host, const std::string& port, const std::string& message,
               std::chrono::milliseconds limit)
    {
        m_message = &message;
        auto on_timeout = [this](std::error_code ec)
        {
            if (!ec && !m_done)
            {
                // Cancel whatever step is running; its handler then ends
                // the exchange with timed_out
                m_timed_out = true;
                m_resolver.cancel();
                m_socket.close();
            }
        };
        m_timer.expires_after(limit);
        m_timer.async_wait(on_timeout);

        auto on_resolved = [this](std::error_code ec, std::vector<spanio::tcp::endpoint> endpoints)
        {
            if (ec)
            {
                finish(ec);
                return;
            }
            connect(std::move(endpoints));
        };
        spanio::resolve_options numeric_port;
        numeric_port.m_numeric_service = true;
        m_resolver.async_resolve(host, port, on_resolved, numeric_port);
    }

    // How the exchange ended
    // Return value: empty on success, errc::timed_out when the limit passed,
    //               otherwise the error of the failed step
    std::error_code result() const
    {
        return m_result;
    }

private:
    // Try each resolved address until one accepts
    // Parameter endpoints: addresses of the host
    void connect(std::vector<spanio::tcp::endpoint> endpoints)
    {
        auto on_connected = [this](std::error_code ec, const spanio::tcp::endpoint& /*peer*/)
        {
            if (ec)
            {
                finish(ec);
                return;
            }
            send();
        };
        spanio::tcp::async_connect(m_socket, std::move(endpoints), on_connected);
    }

    // Send the whole message
    void send()
    {
        auto on_written = [this](std::error_code ec, std::size_t /*bytes*/)
        {
            if (ec)
            {
                finish(ec);
                return;
            }
            receive();
        };
        m_socket.async_write(spanio::buffer(*m_message), on_written);
    }

    // Read exactly as many bytes as were sent
    void receive()
    {
        m_reply->assign(m_message->size(), '\0');
        auto on_read = [this](std::error_code ec, std::size_t bytes)
        {
            // On end of file only the bytes that arrived are kept
            m_reply->resize(bytes);
            finish(ec);
        };
        m_socket.async_read(spanio::buffer(*m_reply), on_read);
    }

    // End the exchange once: record the result and release everything, so
    // run() returns
    // Parameter ec: result of the last step
    void finish(std::error_code ec)
    {
        if (m_done)
        {
            return;
        }
        m_done = true;
        m_result = m_timed_out ? std::make_error_code(std::errc::timed_out) : ec;
        m_timer.cancel();
        m_socket.close();
    }

    // Looks up the host
    spanio::tcp::resolver m_resolver;
    // The connection
    spanio::tcp::socket m_socket;
    // Limits the whole exchange
    spanio::steady_timer m_timer;
    // Bytes to send; set by start()
    const std::string* m_message = nullptr;
    // Destination of the reply
    std::string* m_reply;
    // Set once finish() ran
    bool m_done;
    // Set when the timer cancelled the exchange
    bool m_timed_out;
    // How the exchange ended
    std::error_code m_result;
};

// Run one exchange to the end on a context. The context must not be running
// on another thread; it is restarted first, so it may be reused.
// Parameter context: context to run on
// Parameter host:    host name or numeric address
// Parameter port:    port as decimal text
// Parameter message: bytes to send
// Parameter reply:   receives the bytes read back (fewer if the peer closed)
// Parameter limit:   longest the exchange may take
// Return value: empty on success, errc::timed_out when the limit passed,
//               otherwise the error of the failed step
inline std::error_code run_tcp_client(spanio::io_context& context, const std::string& host, const std::string& port,
                                      const std::string& message, std::string& reply,
                                      std::chrono::milliseconds limit)
{
    context.restart();
    client_session session(context, reply);
    session.start(host, port, message, limit);
    context.run();
    return session.result();
}

}

#if !defined(SPANIO_TCP_CLIENT_NO_MAIN)

// Parse the command line, run one exchange and print the reply
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               exchange failed
int main(int argc, char** argv)
{
    tcp_client_example::options opts;
    std::string error;
    if (!tcp_client_example::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "tcp_client: %s\nusage: tcp_client <host> <port> <message>\n", error.c_str());
        return tcp_client_example::kExitUsage;
    }
    try
    {
        spanio::io_context context;
        std::string reply;
        const std::error_code ec = tcp_client_example::run_tcp_client(
            context, opts.m_host, opts.m_port, opts.m_message, reply, tcp_client_example::kDefaultLimit);
        if (ec)
        {
            std::fprintf(stderr, "tcp_client: %s\n", ec.message().c_str());
            return tcp_client_example::kExitFailure;
        }
        std::printf("%s\n", reply.c_str());
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "tcp_client: %s\n", e.what());
        return tcp_client_example::kExitFailure;
    }
    return tcp_client_example::kExitSuccess;
}

#endif
