// ============================================================================
// File purpose: Tests of the chat_server example (examples/chat_server.cpp).
//               The example is included with SPANIO_CHAT_SERVER_NO_MAIN
//               defined, so the tests run exactly the shipped code. The
//               server runs on its own run() threads; clients are plain
//               blocking sockets, as an external program would use. Cases
//               that do I/O run once per proactor backend of the platform.
//               All traffic stays on 127.0.0.1.
// Usage:        Built as the test_chat_server executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_chat_server slow`.
//
//               Declaration order in each case: fixture, then clients.
//               Clients close first, then the fixture stops the server and
//               joins its threads.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#define SPANIO_CHAT_SERVER_NO_MAIN
#include "../examples/chat_server.cpp"

#include "test_framework.h"
#include "test_net_helpers.h"
#include "test_proactor_backends.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using spanio::io_context;
using spanio::test::announce;
using spanio::test::backend_factory;
using spanio::test::backends;
using spanio::test::connect_client;
using spanio::test::connect_to;
using spanio::test::loopback_address;
using spanio::test::make_context;
using spanio::test::recv_exact;
using spanio::test::send_all;
using spanio::test::socket_guard;

namespace {

// Port 0: let bind() pick a free port
constexpr std::uint16_t kAnyPort = 0;

// Threads running the server in the single-threaded cases
constexpr int kOneThread = 1;

// Threads running the server in the concurrency cases
constexpr int kFourThreads = 4;

// Longest wait for something the server is expected to do
constexpr int kWaitLimitMs = 5000;

// Longest time the slow-client case may flood before the client is dropped
constexpr int kFloodLimitMs = 30000;

// Pause between two checks of a condition being waited for
constexpr int kPollIntervalMs = 2;

// Pause that makes the first part of a split line arrive on its own
constexpr int kSplitPauseMs = 50;

// Bytes read from a socket at a time by chat_client
constexpr std::size_t kClientChunkSize = 4096;

// Users in the concurrency case
constexpr int kConcurrentUsers = 8;

// Messages each of them sends
constexpr int kMessagesPerUser = 20;

// Lines each of them receives: the messages of all the others
constexpr int kLinesPerUser = (kConcurrentUsers - 1) * kMessagesPerUser;

// Send queue limit in the slow-client case
constexpr std::size_t kSmallSendQueue = 64 * 1024;

// Length of each flood message in the slow-client case
constexpr std::size_t kFloodLineSize = 1000;

// Most lines the flooding client may be ahead of the reading client, so the
// reading client stays well within the send queue limit
constexpr int kFloodWindow = 32;

// Bytes sent without a line end in the overlong-line case: over the limit
constexpr std::size_t kOverlongSize = chat::kMaxLineBytes * 2;

// Port given on the command line in the argument case ("7000")
constexpr std::uint16_t kExplicitPort = 7000;

// Thread count given on the command line in the argument case ("3")
constexpr unsigned kExplicitThreads = 3;

// Clock used for time measurements
using test_clock = std::chrono::steady_clock;

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Wait until a condition holds
// Parameter condition: function object returning bool
// Return value: true if it held within kWaitLimitMs
template <typename Condition>
bool wait_until(Condition condition)
{
    const test_clock::time_point start = test_clock::now();
    while (!condition())
    {
        if (elapsed_ms(start) > kWaitLimitMs)
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
    }
    return true;
}

// Number of entries of an argv-style array, as main() would get in argc
// Parameter args: the array
// Return value: N
template <std::size_t N>
int argc_of(const char* const (&args)[N])
{
    static_cast<void>(args);
    return static_cast<int>(N);
}

// A chat user: a blocking socket that speaks lines. Sending and reading may
// run on two threads, but each only on one.
class chat_client
{
public:
    // Connect without joining
    // Parameter port: server port
    // Return value: true on success
    bool connect(std::uint16_t port)
    {
        return connect_client(port, m_socket);
    }

    // Connect, send a nickname, and expect the welcome line
    // Parameter port:     server port
    // Parameter nickname: nickname to use
    // Return value: true if the server welcomed the user
    bool join(std::uint16_t port, const std::string& nickname)
    {
        std::string line;
        return connect(port) && send_line(nickname) && read_line(line) && line == "* welcome " + nickname;
    }

    // Send one line
    // Parameter text: line content, without its line end
    // Return value: true on success
    bool send_line(const std::string& text)
    {
        return send_raw(text + "\n");
    }

    // Send bytes as they are
    // Parameter data: bytes to send
    // Return value: true on success
    bool send_raw(const std::string& data)
    {
        return send_all(m_socket.get(), data.data(), data.size());
    }

    // Read the next line
    // Parameter line: receives the line, without its "\n"
    // Return value: false if the connection closed or the read timed out
    bool read_line(std::string& line)
    {
        while (true)
        {
            const std::size_t end = m_buffer.find('\n');
            if (end != std::string::npos)
            {
                line = m_buffer.substr(0, end);
                m_buffer.erase(0, end + 1);
                return true;
            }
            char chunk[kClientChunkSize];
            const int result = ::recv(m_socket.get(), chunk, static_cast<int>(sizeof(chunk)),
                                      spanio::test::kNoSocketFlags);
            if (result <= spanio::test::kSocketClosedResult)
            {
                return false;
            }
            m_buffer.append(chunk, static_cast<std::size_t>(result));
        }
    }

    // Whether the next read returns exactly this line
    // Parameter expected: the line
    // Return value: true on a match
    bool expect_line(const std::string& expected)
    {
        std::string line;
        return read_line(line) && line == expected;
    }

    // Read lines until one matches
    // Parameter expected: the line to wait for
    // Return value: false if it did not arrive
    bool read_until(const std::string& expected)
    {
        std::string line;
        while (read_line(line))
        {
            if (line == expected)
            {
                return true;
            }
        }
        return false;
    }

    // Whether the server closes the connection after whatever is pending
    // Return value: true if the connection ended before the read timeout
    bool sees_close()
    {
        const test_clock::time_point start = test_clock::now();
        std::string line;
        while (read_line(line))
        {
        }
        return elapsed_ms(start) < spanio::test::kClientReceiveTimeoutMs;
    }

    // Close the connection
    void close()
    {
        m_socket.reset();
    }

private:
    // The connection
    socket_guard m_socket;
    // Bytes received and not yet returned as lines
    std::string m_buffer;
};

// A chat_server on an external context of a given backend, with its run()
// threads. The destructor stops the server and joins them.
class chat_fixture
{
public:
    // Parameter backend:        proactor backend to use
    // Parameter server_options: server limits
    explicit chat_fixture(const backend_factory& backend,
                          const spanio::tcp_server_options& server_options = chat::default_server_options())
        : m_context(make_context(backend))
        , m_server(new chat::chat_server(
              *m_context, spanio::tcp::endpoint(spanio::ipv4::loopback(), kAnyPort), server_options))
    {
    }

    // Stop and join
    ~chat_fixture()
    {
        stop_and_join();
    }

    chat_fixture(const chat_fixture&) = delete;
    chat_fixture& operator=(const chat_fixture&) = delete;

    // The server
    // Return value: the server
    chat::chat_server& server()
    {
        return *m_server;
    }

    // Port the server listens on, or fail the case
    // Return value: the port
    std::uint16_t port() const
    {
        std::error_code ec;
        const spanio::tcp::endpoint local = m_server->local_endpoint(ec);
        SPANIO_REQUIRE(!ec);
        return local.port();
    }

    // Start the server and the run() threads
    // Parameter threads: number of threads
    void start(int threads)
    {
        m_server->start();
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

    // Stop the server and wait for every run() to return
    void stop_and_join()
    {
        m_server->stop();
        for (std::thread& thread : m_threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }

private:
    // Context the server runs on; outlives the server
    std::unique_ptr<io_context> m_context;
    // The server
    std::unique_ptr<chat::chat_server> m_server;
    // Threads calling run()
    std::vector<std::thread> m_threads;
};

// Wait until the server has a given number of users online
// Parameter server:   server to watch
// Parameter expected: count to wait for
// Return value: true if the count was reached in time
bool wait_for_online(const chat::chat_server& server, std::size_t expected)
{
    auto reached = [&server, expected]()
    {
        return server.online_count() == expected;
    };
    return wait_until(reached);
}

// Wait until the server has a given number of open connections
// Parameter server:   server to watch
// Parameter expected: count to wait for
// Return value: true if the count was reached in time
bool wait_for_connections(const chat::chat_server& server, std::size_t expected)
{
    auto reached = [&server, expected]()
    {
        return server.connection_count() == expected;
    };
    return wait_until(reached);
}

// Nickname of a user in the concurrency case
// Parameter id: user number
// Return value: the nickname
std::string user_name(int id)
{
    return "u" + std::to_string(id);
}

// Line another user receives for a message of the concurrency case
// Parameter id:      sender's user number
// Parameter message: message number
// Return value: the line
std::string user_line(int id, int message)
{
    return user_name(id) + ": " + std::to_string(message);
}

// Lines of a receiver that came from neither of two users, so two receivers
// can be compared on the messages both of them got
// Parameter lines: lines the receiver got
// Parameter first: user number to leave out
// Parameter second: user number to leave out
// Return value: the remaining lines, in order
std::vector<std::string> common_lines(const std::vector<std::string>& lines, int first, int second)
{
    const std::string first_prefix = user_name(first) + ": ";
    const std::string second_prefix = user_name(second) + ": ";
    std::vector<std::string> kept;
    for (const std::string& line : lines)
    {
        if (line.compare(0, first_prefix.size(), first_prefix) != 0
            && line.compare(0, second_prefix.size(), second_prefix) != 0)
        {
            kept.push_back(line);
        }
    }
    return kept;
}

}

// Case 1: a message reaches everyone else, and not its sender
SPANIO_TEST(broadcast_reaches_others)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        chat_client bob;
        chat_client carol;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));
        SPANIO_REQUIRE(alice.expect_line("* bob joined"));
        SPANIO_REQUIRE(carol.join(port, "carol"));
        SPANIO_REQUIRE(alice.expect_line("* carol joined"));
        SPANIO_REQUIRE(bob.expect_line("* carol joined"));

        SPANIO_REQUIRE(alice.send_line("hi"));
        SPANIO_CHECK(bob.expect_line("alice: hi"));
        SPANIO_CHECK(carol.expect_line("alice: hi"));

        // If alice had got a copy of her own message, it would come first
        SPANIO_REQUIRE(bob.send_line("hello"));
        SPANIO_CHECK(alice.expect_line("bob: hello"));
    }
}

// Case 2: joining and leaving are announced; a client that never joined
// leaves no trace
SPANIO_TEST(join_and_leave_notices)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        chat_client bob;
        chat_client anonymous;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));
        SPANIO_REQUIRE(alice.expect_line("* bob joined"));

        SPANIO_REQUIRE(anonymous.connect(port));
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), 3));
        anonymous.close();
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), 2));

        // Had the anonymous client been announced, that line would come first
        bob.close();
        SPANIO_CHECK(alice.expect_line("* bob left"));
    }
}

// Case 3: lines split across sends, several lines in one send and "\r\n"
// all work, and empty lines are not broadcast
SPANIO_TEST(line_handling)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        chat_client bob;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));
        SPANIO_REQUIRE(alice.expect_line("* bob joined"));

        SPANIO_REQUIRE(alice.send_raw("he"));
        std::this_thread::sleep_for(std::chrono::milliseconds(kSplitPauseMs));
        SPANIO_REQUIRE(alice.send_raw("llo\r\nsecond\n\n\r\nthird\n"));

        SPANIO_CHECK(bob.expect_line("alice: hello"));
        SPANIO_CHECK(bob.expect_line("alice: second"));
        SPANIO_CHECK(bob.expect_line("alice: third"));
    }
}

// Case 4: an empty, too long or taken nickname gets an error and a
// disconnect; the user who holds the name is not affected
SPANIO_TEST(invalid_nickname_rejected)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        SPANIO_REQUIRE(alice.join(port, "alice"));

        const std::vector<std::string> bad_names = {
            "", std::string(chat::kMaxNicknameBytes + 1, 'n'), "alice"};
        const std::vector<std::string> reasons = {
            "error: nickname must not be empty", "error: nickname is too long", "error: nickname is taken"};
        for (std::size_t i = 0; i < bad_names.size(); ++i)
        {
            chat_client rejected;
            SPANIO_REQUIRE(rejected.connect(port));
            SPANIO_REQUIRE(rejected.send_line(bad_names[i]));
            SPANIO_CHECK(rejected.expect_line(reasons[i]));
            SPANIO_CHECK(rejected.sees_close());
        }
        SPANIO_CHECK(fixture.server().online_count() == 1);

        // The longest allowed name works, and alice still chats. Any notice
        // about the rejected clients would arrive before this one.
        chat_client bob;
        const std::string longest(chat::kMaxNicknameBytes, 'b');
        SPANIO_REQUIRE(bob.join(port, longest));
        SPANIO_CHECK(alice.expect_line("* " + longest + " joined"));
        SPANIO_REQUIRE(alice.send_line("still here"));
        SPANIO_CHECK(bob.expect_line("alice: still here"));
    }
}

// Case 5: eight users on four server threads send at once; every line is
// whole, each sender's order holds, and receivers agree on the order of the
// messages they share
SPANIO_TEST(concurrent_broadcast_same_order)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kFourThreads);
        const std::uint16_t port = fixture.port();

        std::vector<chat_client> users(static_cast<std::size_t>(kConcurrentUsers));
        for (int id = 0; id < kConcurrentUsers; ++id)
        {
            chat_client& user = users[static_cast<std::size_t>(id)];
            SPANIO_REQUIRE(user.join(port, user_name(id)));
            // Everyone already online hears about this user
            for (int earlier = 0; earlier < id; ++earlier)
            {
                SPANIO_REQUIRE(users[static_cast<std::size_t>(earlier)].expect_line(
                    "* " + user_name(id) + " joined"));
            }
        }

        std::atomic<int> failures(0);
        std::vector<std::vector<std::string>> received(static_cast<std::size_t>(kConcurrentUsers));
        auto run_user = [&](int id)
        {
            chat_client& user = users[static_cast<std::size_t>(id)];
            for (int m = 0; m < kMessagesPerUser; ++m)
            {
                if (!user.send_line(std::to_string(m)))
                {
                    ++failures;
                    return;
                }
            }
            std::vector<std::string>& lines = received[static_cast<std::size_t>(id)];
            std::string line;
            for (int i = 0; i < kLinesPerUser; ++i)
            {
                if (!user.read_line(line))
                {
                    ++failures;
                    return;
                }
                lines.push_back(line);
            }
        };
        std::vector<std::thread> threads;
        for (int id = 0; id < kConcurrentUsers; ++id)
        {
            threads.emplace_back(run_user, id);
        }
        for (std::thread& thread : threads)
        {
            thread.join();
        }
        SPANIO_REQUIRE(failures.load() == 0);

        // Each receiver has each sender's messages, whole and in order
        int bad_lines = 0;
        for (int receiver = 0; receiver < kConcurrentUsers; ++receiver)
        {
            std::vector<int> next(static_cast<std::size_t>(kConcurrentUsers), 0);
            for (const std::string& line : received[static_cast<std::size_t>(receiver)])
            {
                bool matched = false;
                for (int sender = 0; sender < kConcurrentUsers && !matched; ++sender)
                {
                    int& expected = next[static_cast<std::size_t>(sender)];
                    matched = sender != receiver && line == user_line(sender, expected);
                    if (matched)
                    {
                        ++expected;
                    }
                }
                if (!matched)
                {
                    ++bad_lines;
                }
            }
        }
        SPANIO_CHECK(bad_lines == 0);

        // Any two receivers saw the messages they share in the same order
        int order_mismatches = 0;
        for (int first = 0; first < kConcurrentUsers; ++first)
        {
            for (int second = first + 1; second < kConcurrentUsers; ++second)
            {
                if (common_lines(received[static_cast<std::size_t>(first)], first, second)
                    != common_lines(received[static_cast<std::size_t>(second)], first, second))
                {
                    ++order_mismatches;
                }
            }
        }
        SPANIO_CHECK(order_mismatches == 0);
    }
}

// Case 6: a user who stops reading is dropped when their send queue
// overflows; everyone hears they left and keeps chatting
SPANIO_TEST(slow_client_is_dropped)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        spanio::tcp_server_options server_options = chat::default_server_options();
        server_options.m_max_send_queue_bytes = kSmallSendQueue;
        chat_fixture fixture(backend, server_options);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client talker;
        chat_client listener;
        chat_client silent;
        SPANIO_REQUIRE(talker.join(port, "talker"));
        SPANIO_REQUIRE(listener.join(port, "listener"));
        SPANIO_REQUIRE(talker.expect_line("* listener joined"));
        SPANIO_REQUIRE(silent.join(port, "silent"));
        SPANIO_REQUIRE(talker.expect_line("* silent joined"));
        SPANIO_REQUIRE(listener.expect_line("* silent joined"));

        // The listener reads on its own thread and the talker stays within
        // kFloodWindow lines of it, so only the silent user falls behind
        std::atomic<int> lines_read(0);
        std::atomic<bool> listener_ok(false);
        auto run_listener = [&]()
        {
            std::string line;
            while (listener.read_line(line))
            {
                if (line == "* silent left")
                {
                    listener_ok = true;
                    return;
                }
                ++lines_read;
            }
        };
        std::atomic<bool> flooding(true);
        std::atomic<bool> send_failed(false);
        const std::string flood_line(kFloodLineSize, 'f');
        auto run_flood = [&]()
        {
            const test_clock::time_point start = test_clock::now();
            int sent = 0;
            while (flooding.load() && elapsed_ms(start) < kFloodLimitMs)
            {
                if (sent - lines_read.load() >= kFloodWindow)
                {
                    std::this_thread::yield();
                    continue;
                }
                if (!talker.send_line(flood_line))
                {
                    send_failed = true;
                    return;
                }
                ++sent;
            }
        };
        std::thread listener_thread(run_listener);
        std::thread flood_thread(run_flood);

        auto dropped = [&fixture]()
        {
            return fixture.server().online_count() == 2;
        };
        const test_clock::time_point start = test_clock::now();
        bool was_dropped = dropped();
        while (!was_dropped && elapsed_ms(start) < kFloodLimitMs)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
            was_dropped = dropped();
        }
        flooding = false;
        flood_thread.join();
        listener_thread.join();

        SPANIO_CHECK(was_dropped);
        SPANIO_CHECK(listener_ok.load());
        SPANIO_CHECK(!send_failed.load());
        SPANIO_CHECK(talker.expect_line("* silent left"));
        SPANIO_REQUIRE(talker.send_line("still here"));
        SPANIO_CHECK(listener.read_until("talker: still here"));
    }
}

// Case 7: a line over the limit ends that connection; the others hear the
// user left
SPANIO_TEST(overlong_line_disconnects)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        chat_client bob;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));
        SPANIO_REQUIRE(alice.expect_line("* bob joined"));

        SPANIO_REQUIRE(bob.send_raw(std::string(kOverlongSize, 'x')));
        SPANIO_CHECK(alice.expect_line("* bob left"));
        SPANIO_CHECK(bob.sees_close());
    }
}

// Case 8: online_count covers only users who joined, and returns to zero
SPANIO_TEST(online_count_follows_connections)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kOneThread);
        const std::uint16_t port = fixture.port();
        SPANIO_CHECK(fixture.server().online_count() == 0);

        chat_client pending;
        SPANIO_REQUIRE(pending.connect(port));
        SPANIO_REQUIRE(wait_for_connections(fixture.server(), 1));
        SPANIO_CHECK(fixture.server().online_count() == 0);

        chat_client alice;
        chat_client bob;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));
        SPANIO_CHECK(fixture.server().online_count() == 2);

        alice.close();
        SPANIO_CHECK(wait_for_online(fixture.server(), 1));
        bob.close();
        SPANIO_CHECK(wait_for_online(fixture.server(), 0));
        pending.close();
        SPANIO_CHECK(wait_for_connections(fixture.server(), 0));
    }
}

// Case 9: stop() with users online disconnects them, run() returns, and new
// connections are refused
SPANIO_TEST(stop_closes_all_users)
{
    for (const backend_factory& backend : backends())
    {
        announce(backend);
        chat_fixture fixture(backend);
        fixture.start(kFourThreads);
        const std::uint16_t port = fixture.port();

        chat_client alice;
        chat_client bob;
        SPANIO_REQUIRE(alice.join(port, "alice"));
        SPANIO_REQUIRE(bob.join(port, "bob"));

        const test_clock::time_point start = test_clock::now();
        fixture.stop_and_join();
        SPANIO_CHECK(elapsed_ms(start) < kWaitLimitMs);
        SPANIO_CHECK(fixture.server().online_count() == 0);
        SPANIO_CHECK(alice.sees_close());
        SPANIO_CHECK(bob.sees_close());

        std::error_code ec;
        socket_guard late(spanio::detail::create_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, ec));
        SPANIO_REQUIRE(!ec);
        SPANIO_CHECK(connect_to(late.get(), loopback_address(port)) != 0);
        SPANIO_CHECK(spanio::detail::last_error() == std::errc::connection_refused);
    }
}

// Case 10: the command line gives defaults, honours explicit values, and
// rejects bad ports, bad thread counts and extra arguments
SPANIO_TEST(parse_arguments)
{
    chat::options opts;
    std::string error;

    const char* const defaults[] = {"chat_server"};
    SPANIO_CHECK(chat::parse_arguments(argc_of(defaults), defaults, opts, error));
    SPANIO_CHECK(opts.m_port == chat::kDefaultPort);
    SPANIO_CHECK(opts.m_threads >= chat::kMinThreads);

    const char* const explicit_values[] = {"chat_server", "7000", "3"};
    SPANIO_CHECK(chat::parse_arguments(argc_of(explicit_values), explicit_values, opts, error));
    SPANIO_CHECK(opts.m_port == kExplicitPort);
    SPANIO_CHECK(opts.m_threads == kExplicitThreads);

    const char* const bad_ports[] = {"0", "70000", "abc", "9000x", "-1", "", " 80"};
    for (const char* port : bad_ports)
    {
        const char* const argv[] = {"chat_server", port};
        error.clear();
        SPANIO_CHECK(!chat::parse_arguments(argc_of(argv), argv, opts, error));
        SPANIO_CHECK(!error.empty());
    }

    const char* const bad_threads[] = {"0", "x", "257"};
    for (const char* threads : bad_threads)
    {
        const char* const argv[] = {"chat_server", "9000", threads};
        error.clear();
        SPANIO_CHECK(!chat::parse_arguments(argc_of(argv), argv, opts, error));
        SPANIO_CHECK(!error.empty());
    }

    const char* const too_many[] = {"chat_server", "9000", "2", "extra"};
    error.clear();
    SPANIO_CHECK(!chat::parse_arguments(argc_of(too_many), too_many, opts, error));
    SPANIO_CHECK(!error.empty());
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
