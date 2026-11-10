// ============================================================================
// File purpose: UDP client example: resolves a host name, sends one datagram
//               and waits for one reply (the reply of an echo server). Shows
//               the synchronous udp::resolver::resolve, async_send_to,
//               async_receive_from and a steady_timer, which UDP needs
//               because a lost datagram never produces an error.
// Usage:        udp_client <host> <port> <message>
//                 host     name or numeric address, e.g. localhost
//                 port     1-65535
//                 message  text to send
//               Try it against the UDP echo server:
//                 udp_echo_server 9000
//                 udp_client localhost 9000 hello      -> prints "hello"
//               Without a reply within 2 seconds the client gives up with
//               "timed out".
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/udp_client.cpp -o udp_client -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\udp_client.cpp ws2_32.lib
//
//               The unit tests include this file with
//               SPANIO_UDP_CLIENT_NO_MAIN defined and call run_udp_client().
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

namespace udp_client_example {

// How long to wait for the reply when run from the command line
constexpr std::chrono::milliseconds kDefaultLimit(2000);

// Number of command-line arguments, including the program name
constexpr int kArgumentCount = 4;

// Position of the host argument in argv
constexpr int kHostArgument = 1;

// Position of the port argument in argv
constexpr int kPortArgument = 2;

// Position of the message argument in argv
constexpr int kMessageArgument = 3;

// Smallest port a client may send to
constexpr unsigned long kMinPort = 1;

// Largest UDP port
constexpr unsigned long kMaxPort = 65535;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Process exit code: the reply arrived
constexpr int kExitSuccess = 0;

// Process exit code: resolving, sending or receiving failed
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

// Read the command line: udp_client <host> <port> <message>
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
    if (std::string(argv[kHostArgument]).empty())
    {
        error = "host must not be empty";
        return false;
    }
    out.m_host = argv[kHostArgument];
    out.m_port = port;
    out.m_message = argv[kMessageArgument];
    return true;
}

// One exchange: send a datagram, wait for one back. A timer limits the
// wait. All handlers run on the thread that calls run(), so the members
// need no lock.
class client_session
{
public:
    // Parameter context: context to run on
    // Parameter reply:   receives the datagram that came back
    client_session(spanio::io_context& context, std::string& reply)
        : m_socket(context)
        , m_timer(context)
        , m_buffer(spanio::kMaxUdpPayload)
        , m_reply(&reply)
        , m_done(false)
        , m_timed_out(false)
    {
    }

    // Start the exchange
    // Parameter peer:    where to send
    // Parameter message: bytes to send; must stay valid until the end
    // Parameter limit:   longest to wait
    void start(const spanio::udp::endpoint& peer, const std::string& message, std::chrono::milliseconds limit)
    {
        auto on_timeout = [this](std::error_code ec)
        {
            if (!ec && !m_done)
            {
                // No reply: closing cancels the receive, whose handler then
                // ends the exchange with timed_out
                m_timed_out = true;
                m_socket.close();
            }
        };
        m_timer.expires_after(limit);
        m_timer.async_wait(on_timeout);

        auto on_sent = [this](std::error_code ec, std::size_t /*bytes*/)
        {
            if (ec)
            {
                finish(ec);
                return;
            }
            receive();
        };
        // async_send_to opens the socket for the peer's address family
        m_socket.async_send_to(spanio::buffer(message), peer, on_sent);
    }

    // How the exchange ended
    // Return value: empty on success, errc::timed_out without a reply,
    //               otherwise the error of the failed step
    std::error_code result() const
    {
        return m_result;
    }

private:
    // Wait for one datagram
    void receive()
    {
        auto on_received = [this](std::error_code ec, std::size_t bytes, const spanio::udp::endpoint& /*from*/)
        {
            if (!ec)
            {
                m_reply->assign(m_buffer.data(), bytes);
            }
            finish(ec);
        };
        m_socket.async_receive_from(spanio::buffer(m_buffer), on_received);
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

    // The socket, opened by the first send
    spanio::udp::socket m_socket;
    // Limits the wait for the reply
    spanio::steady_timer m_timer;
    // Receive buffer, large enough for any datagram
    std::vector<char> m_buffer;
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
// Parameter reply:   receives the datagram that came back
// Parameter limit:   longest to wait for the reply
// Return value: empty on success, errc::timed_out without a reply, a
//               spanio.netdb error if the host is unknown, otherwise the
//               error of the failed step
inline std::error_code run_udp_client(spanio::io_context& context, const std::string& host, const std::string& port,
                                      const std::string& message, std::string& reply,
                                      std::chrono::milliseconds limit)
{
    // A blocking lookup is fine before the exchange starts
    spanio::udp::resolver resolver(context);
    spanio::resolve_options numeric_port;
    numeric_port.m_numeric_service = true;
    std::error_code ec;
    const std::vector<spanio::udp::endpoint> endpoints = resolver.resolve(host, port, ec, numeric_port);
    if (ec)
    {
        return ec;
    }
    // UDP cannot try the addresses one after another the way TCP connects
    // do: a datagram to an address nobody listens on just goes unanswered.
    // Prefer IPv4, because "localhost" often lists ::1 first while servers
    // commonly bind 0.0.0.0 only.
    spanio::udp::endpoint peer = endpoints.front();
    for (const spanio::udp::endpoint& candidate : endpoints)
    {
        if (candidate.family() == AF_INET)
        {
            peer = candidate;
            break;
        }
    }
    context.restart();
    client_session session(context, reply);
    session.start(peer, message, limit);
    context.run();
    return session.result();
}

}

#if !defined(SPANIO_UDP_CLIENT_NO_MAIN)

// Parse the command line, run one exchange and print the reply
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               exchange failed
int main(int argc, char** argv)
{
    udp_client_example::options opts;
    std::string error;
    if (!udp_client_example::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "udp_client: %s\nusage: udp_client <host> <port> <message>\n", error.c_str());
        return udp_client_example::kExitUsage;
    }
    try
    {
        spanio::io_context context;
        std::string reply;
        const std::error_code ec = udp_client_example::run_udp_client(
            context, opts.m_host, opts.m_port, opts.m_message, reply, udp_client_example::kDefaultLimit);
        if (ec)
        {
            std::fprintf(stderr, "udp_client: %s\n", ec.message().c_str());
            return udp_client_example::kExitFailure;
        }
        std::printf("%s\n", reply.c_str());
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "udp_client: %s\n", e.what());
        return udp_client_example::kExitFailure;
    }
    return udp_client_example::kExitSuccess;
}

#endif
