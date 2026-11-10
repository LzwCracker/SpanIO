// ============================================================================
// File purpose: Multi-user TCP chat server, the M2 example. It is built on
//               tcp_server, which owns the connections, send queues, framing
//               and threads, so this file holds only the chat logic.
// Usage:        chat_server [port] [threads]
//                 port     listening port, 1-65535 (default 9000)
//                 threads  threads running the server, 1-256
//                          (default: number of CPU cores)
//               Try it with two terminals: telnet localhost 9000
//
//               Protocol: lines of text ending in "\n" or "\r\n".
//               - The first line is the nickname: not empty, at most 32
//                 bytes, not used by anyone online. The server answers
//                 "* welcome <nick>", or "error: <reason>" and disconnects.
//               - Every later line goes to everyone else as "<nick>: <text>".
//                 Empty lines are ignored.
//               - Everyone else sees "* <nick> joined" and "* <nick> left".
//               - All users see messages in the same order.
//               A line over 1024 bytes ends the connection, and so does
//               falling more than 1 MiB behind on reading, so one slow client
//               cannot hold up the others or exhaust memory.
//
//               The server listens on 0.0.0.0 and runs until the process is
//               killed; Ctrl+C handling needs a signal wrapper (planned).
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/chat_server.cpp -o chat_server -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\chat_server.cpp ws2_32.lib
//               CMake builds it as the chat_server target when
//               SPANIO_BUILD_EXAMPLES is on (the default at top level).
//
//               The unit tests include this file with
//               SPANIO_CHAT_SERVER_NO_MAIN defined, so they exercise exactly
//               this code without its main().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#include "spanio/tcp_server.h"

#include <any>
#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>

namespace chat {

// Port used when none is given on the command line
constexpr std::uint16_t kDefaultPort = 9000;

// Smallest port a server may listen on (0 would mean "any port")
constexpr unsigned long kMinPort = 1;

// Largest TCP port
constexpr unsigned long kMaxPort = 65535;

// Fewest threads that can run the server
constexpr unsigned long kMinThreads = 1;

// Most threads accepted on the command line; a sanity limit
constexpr unsigned long kMaxThreads = 256;

// Thread count used when the number of CPU cores cannot be determined
constexpr unsigned kFallbackThreads = 1;

// Most command-line arguments, including the program name
constexpr int kMaxArguments = 3;

// Position of the port argument in argv
constexpr int kPortArgument = 1;

// Position of the thread count argument in argv
constexpr int kThreadsArgument = 2;

// Numbers on the command line are decimal
constexpr int kDecimalBase = 10;

// Longest accepted nickname
constexpr std::size_t kMaxNicknameBytes = 32;

// Longest accepted line
constexpr std::size_t kMaxLineBytes = 1024;

// How far behind a client may fall on reading before it is disconnected
constexpr std::size_t kMaxSendQueueBytes = 1024 * 1024;

// Process exit code: normal end
constexpr int kExitSuccess = 0;

// Process exit code: the server could not start
constexpr int kExitFailure = 1;

// Process exit code: bad command line
constexpr int kExitUsage = 2;

// Settings taken from the command line
struct options
{
    // Port to listen on
    std::uint16_t m_port = kDefaultPort;
    // Threads running the server
    unsigned m_threads = kFallbackThreads;
};

// Parse an unsigned decimal number within a range. Signs, spaces and
// trailing characters are rejected.
// Parameter text:  the argument
// Parameter low:   smallest accepted value
// Parameter high:  largest accepted value
// Parameter value: receives the number on success
// Return value: true if text is a number in [low, high]
inline bool parse_number(const char* text, unsigned long low, unsigned long high, unsigned long& value)
{
    if (text == nullptr || !std::isdigit(static_cast<unsigned char>(text[0])))
    {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, kDecimalBase);
    if (errno != 0 || end == text || *end != '\0' || parsed < low || parsed > high)
    {
        return false;
    }
    value = parsed;
    return true;
}

// Number of threads to use when none is given: one per CPU core
// Return value: at least kFallbackThreads
inline unsigned default_threads()
{
    const unsigned cores = std::thread::hardware_concurrency();
    return cores == 0 ? kFallbackThreads : cores;
}

// Read the command line: chat_server [port] [threads]
// Parameter argc:  argument count, as passed to main()
// Parameter argv:  arguments, as passed to main()
// Parameter out:   receives the settings on success
// Parameter error: receives a description of the problem on failure
// Return value: true if the command line is valid
inline bool parse_arguments(int argc, const char* const* argv, options& out, std::string& error)
{
    options parsed;
    parsed.m_threads = default_threads();
    if (argc > kMaxArguments)
    {
        error = "too many arguments";
        return false;
    }
    if (argc > kPortArgument)
    {
        unsigned long port = 0;
        if (!parse_number(argv[kPortArgument], kMinPort, kMaxPort, port))
        {
            error = "port must be a number from 1 to 65535";
            return false;
        }
        parsed.m_port = static_cast<std::uint16_t>(port);
    }
    if (argc > kThreadsArgument)
    {
        unsigned long threads = 0;
        if (!parse_number(argv[kThreadsArgument], kMinThreads, kMaxThreads, threads))
        {
            error = "threads must be a number from 1 to 256";
            return false;
        }
        parsed.m_threads = static_cast<unsigned>(threads);
    }
    out = parsed;
    return true;
}

// Limits a chat server uses unless told otherwise
// Return value: options with the chat line and send queue limits
inline spanio::tcp_server_options default_server_options()
{
    spanio::tcp_server_options server_options;
    server_options.m_max_message_size = kMaxLineBytes;
    server_options.m_max_send_queue_bytes = kMaxSendQueueBytes;
    return server_options;
}

// The users online. One lock covers the member list and every broadcast, so
// messages reach all users in the same order. Broadcasting under the lock is
// cheap because tcp_connection::send() only queues the data, and it is safe
// because send() never runs a callback.
//
// Members are weak pointers: a connection's handlers keep it alive, and the
// room is reachable from the server's callbacks, so a strong pointer would
// form a cycle through the connection's reference to the server state.
class chat_room
{
public:
    // Add a user, tell them they are in, and tell everyone else
    // Parameter nickname:   requested nickname
    // Parameter connection: the user's connection
    // Parameter error:      receives the reason when the nickname is refused
    // Return value: true if the user joined
    bool join(const std::string& nickname, const spanio::tcp_connection_ptr& connection, std::string& error)
    {
        if (nickname.empty())
        {
            error = "nickname must not be empty";
            return false;
        }
        if (nickname.size() > kMaxNicknameBytes)
        {
            error = "nickname is too long";
            return false;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_members.count(nickname) != 0)
        {
            error = "nickname is taken";
            return false;
        }
        connection->send("* welcome " + nickname + "\n");
        send_to_all_locked("* " + nickname + " joined\n");
        m_members.emplace(nickname, connection);
        return true;
    }

    // Remove a user and tell everyone else
    // Parameter nickname: nickname of a user who joined
    void leave(const std::string& nickname)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_members.erase(nickname) != 0)
        {
            send_to_all_locked("* " + nickname + " left\n");
        }
    }

    // Send a user's message to everyone else
    // Parameter sender: nickname of the sender, who does not get a copy
    // Parameter text:   the message
    void broadcast(const std::string& sender, std::string_view text)
    {
        std::string line = sender + ": ";
        line.append(text.data(), text.size());
        line += "\n";

        std::lock_guard<std::mutex> lock(m_mutex);
        for (const auto& member : m_members)
        {
            if (member.first != sender)
            {
                send_to_member(member.second, line);
            }
        }
    }

    // Number of users who joined and have not left
    // Return value: the count
    std::size_t online_count() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_members.size();
    }

private:
    // Send a line to one member, if its connection still exists. A client
    // too far behind is disconnected by send(); its on_close removes it.
    // Parameter member: the member's connection
    // Parameter line:   the text to send
    static void send_to_member(const std::weak_ptr<spanio::tcp_connection>& member, const std::string& line)
    {
        const spanio::tcp_connection_ptr connection = member.lock();
        if (connection != nullptr)
        {
            connection->send(line);
        }
    }

    // Send a line to every member. Caller holds m_mutex.
    // Parameter line: the text to send
    void send_to_all_locked(const std::string& line)
    {
        for (const auto& member : m_members)
        {
            send_to_member(member.second, line);
        }
    }

    // Guards m_members and the order of broadcasts
    mutable std::mutex m_mutex;
    // Users online, by nickname
    std::unordered_map<std::string, std::weak_ptr<spanio::tcp_connection>> m_members;
};

// The chat service: connects the tcp_server callbacks to a chat_room. A
// connection's context holds its nickname, empty until it joined.
class chat_server
{
public:
    // Listen on 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port:           port to listen on
    // Parameter server_options: limits
    explicit chat_server(std::uint16_t port,
                         const spanio::tcp_server_options& server_options = default_server_options())
        : m_room(std::make_shared<chat_room>())
        , m_server(port, server_options)
    {
        install_callbacks();
    }

    // Listen on an address, on a context the caller runs; use start(), not
    // run(). Throws std::system_error if the address cannot be bound.
    // Parameter context:        context to run on; must outlive the server
    // Parameter local:          address and port to listen on
    // Parameter server_options: limits
    chat_server(spanio::io_context& context, const spanio::tcp::endpoint& local,
                const spanio::tcp_server_options& server_options = default_server_options())
        : m_room(std::make_shared<chat_room>())
        , m_server(context, local, server_options)
    {
        install_callbacks();
    }

    // Start accepting, without blocking
    void start()
    {
        m_server.start();
    }

    // Run on the owned context until stop()
    // Parameter threads: threads to run on, including the calling one
    void run(unsigned threads)
    {
        m_server.run(threads);
    }

    // Disconnect everyone and stop accepting; safe from any thread
    void stop()
    {
        m_server.stop();
    }

    // Address the server listens on
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    spanio::tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_server.local_endpoint(ec);
    }

    // Number of users who joined and have not left
    // Return value: the count
    std::size_t online_count() const
    {
        return m_room->online_count();
    }

    // Number of open connections, including those that have not joined yet
    // Return value: the count
    std::size_t connection_count() const
    {
        return m_server.connection_count();
    }

    // Name of the I/O backend, for the startup message
    // Return value: static string
    const char* backend_name()
    {
        return m_server.context().backend_name();
    }

private:
    // Give each connection an empty nickname
    // Parameter connection: the new connection
    static void on_connect(const spanio::tcp_connection_ptr& connection)
    {
        connection->user_data() = std::string();
    }

    // Handle one line: the nickname, or else a message
    // Parameter room:       the chat room
    // Parameter connection: the sender's connection
    // Parameter line:       the line, without its line end
    static void on_line(chat_room& room, const spanio::tcp_connection_ptr& connection, std::string_view line)
    {
        std::string& nickname = std::any_cast<std::string&>(connection->user_data());
        if (!nickname.empty())
        {
            if (!line.empty())
            {
                room.broadcast(nickname, line);
            }
            return;
        }
        const std::string requested(line);
        std::string error;
        if (room.join(requested, connection, error))
        {
            nickname = requested;
            return;
        }
        connection->send("error: " + error + "\n");
        connection->close();
    }

    // Remove a user whose connection ended
    // Parameter room:       the chat room
    // Parameter connection: the connection
    static void on_closed(chat_room& room, const spanio::tcp_connection_ptr& connection)
    {
        const std::string& nickname = std::any_cast<const std::string&>(connection->user_data());
        if (!nickname.empty())
        {
            room.leave(nickname);
        }
    }

    // Connect the server's callbacks to the room. They hold the room through
    // a shared_ptr, so a callback that runs after this object is gone (on an
    // external context) is still safe.
    void install_callbacks()
    {
        std::shared_ptr<chat_room> room = m_room;
        auto line_received = [room](const spanio::tcp_connection_ptr& connection, std::string_view line)
        {
            on_line(*room, connection, line);
        };
        auto connection_closed = [room](const spanio::tcp_connection_ptr& connection, std::error_code)
        {
            on_closed(*room, connection);
        };
        m_server.on_connect(&chat_server::on_connect);
        m_server.on_message(line_received);
        m_server.on_close(connection_closed);
        m_server.set_framer(spanio::line_framer());
    }

    // The users online
    std::shared_ptr<chat_room> m_room;
    // Connections, framing and threads
    spanio::tcp_server m_server;
};

}

#if !defined(SPANIO_CHAT_SERVER_NO_MAIN)

// Parse the command line and run the server
// Parameter argc: argument count
// Parameter argv: arguments; see the file header
// Return value: kExitUsage for a bad command line, kExitFailure if the
//               server could not start
int main(int argc, char** argv)
{
    chat::options opts;
    std::string error;
    if (!chat::parse_arguments(argc, argv, opts, error))
    {
        std::fprintf(stderr, "chat_server: %s\nusage: chat_server [port] [threads]\n", error.c_str());
        return chat::kExitUsage;
    }
    try
    {
        chat::chat_server server(opts.m_port);
        std::printf("chat_server listening on port %u with %u thread(s), backend %s\n",
                    static_cast<unsigned>(opts.m_port), opts.m_threads, server.backend_name());
        std::fflush(stdout);
        server.run(opts.m_threads);
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "chat_server: %s\n", e.what());
        return chat::kExitFailure;
    }
    return chat::kExitSuccess;
}

#endif
