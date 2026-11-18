// ============================================================================
// File purpose: Unit tests of what is specific to the io_uring backend
//               (io_uring_proactor.h) and of the run-time fallback in
//               platform_proactor.h. The io_proactor contract itself is
//               covered for every backend, io_uring included, by
//               test_io_proactor; these cases drive the paths only io_uring
//               has: a tiny ring whose completion queue overflows, the
//               wake-up NOP, ASYNC_CANCEL, recvmsg truncation and the
//               EXT_ARG poll timeout.
// Usage:        Built on every platform. Without SPANIO_ENABLE_IO_URING (or
//               off Linux) only the factory and fallback cases run; with it,
//               the io_uring cases are skipped with a note when the kernel
//               refuses io_uring.
//               test_io_uring_proactor [case-name-filter]
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-18
// ============================================================================

#include "spanio/detail/io_uring_proactor.h"
#include "spanio/detail/platform_proactor.h"
#include "test_framework.h"
#include "test_net_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <thread>
#include <vector>

using namespace spanio::detail;
using spanio::test::socket_guard;

namespace {

// Clock used for all time measurements
using test_clock = std::chrono::steady_clock;

// Overall limit for operations that should finish quickly
constexpr int kCompleteWaitMs = 5000;

// How long to watch for completions that must not appear
constexpr int kQuietWaitMs = 200;

// Timeout given to poll() by the timeout case
constexpr int kPollTimeoutMs = 100;

// Upper bound for a poll(0) that has nothing to return
constexpr long long kImmediateReturnMs = 50;

// How long the waker thread lets the poller block before waking it
constexpr int kBlockBeforeWakeMs = 100;

// Submission ring size of the overflow case
constexpr unsigned kTinySqEntries = 4;

// Completion ring size of the overflow case: far below the completions it
// receives, so the kernel must park most of them on its overflow list
constexpr unsigned kTinyCqEntries = 8;

// Connected pairs in the overflow case; each yields a receive and a send
constexpr int kOverflowPairs = 32;

// Operations of the overflow case: one receive and one send per pair
constexpr std::size_t kOverflowOps = static_cast<std::size_t>(kOverflowPairs) * 2;

// Number of create/destroy cycles with an operation in between
constexpr int kLifecycleCycles = 50;

// Receive buffer size of the data cases
constexpr std::size_t kSmallBufferSize = 64;

// Datagram sent by the truncation case; longer than kTruncatedBufferSize
constexpr char kLongDatagram[] = "0123456789abcdef";

// Length of kLongDatagram without the terminating zero
constexpr std::size_t kLongDatagramLength = sizeof(kLongDatagram) - 1;

// Receive buffer of the truncation case; smaller than the datagram
constexpr std::size_t kTruncatedBufferSize = 4;

// Short message used by the data cases
constexpr char kMessage[] = "hello";

// Length of kMessage without the terminating zero
constexpr std::size_t kMessageLength = sizeof(kMessage) - 1;

// recv/send flags for raw socket calls: none
constexpr int kNoFlags = 0;

// Remaining milliseconds until a deadline, never negative
// Parameter deadline: end of the waiting period
// Return value: milliseconds left
int remaining_ms(test_clock::time_point deadline)
{
    const long long left = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - test_clock::now()).count();
    return left > 0 ? static_cast<int>(left) : 0;
}

// Poll until every expected operation came back, checking each comes back
// exactly once and nothing else does
// Parameter proactor:   proactor to poll
// Parameter expected:   operations that must come back
// Parameter timeout_ms: overall time limit
// Return value: true if all came back in time
bool wait_all(io_proactor& proactor, const std::vector<io_operation*>& expected, int timeout_ms)
{
    std::map<io_operation*, int> counts;
    for (io_operation* op : expected)
    {
        counts[op] = 0;
    }
    std::size_t returned = 0;
    const auto deadline = test_clock::now() + std::chrono::milliseconds(timeout_ms);
    completion_list out;
    while (returned < expected.size() && remaining_ms(deadline) > 0)
    {
        std::error_code ec;
        proactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        for (io_operation* op : out)
        {
            SPANIO_CHECK(counts.count(op) == 1);
            ++counts[op];
            ++returned;
        }
    }
    for (const auto& entry : counts)
    {
        SPANIO_CHECK(entry.second == 1);
    }
    return returned == expected.size();
}

// Factory that always fails, standing in for a kernel without io_uring
// Parameter ec: set to errc::function_not_supported
// Return value: always nullptr
io_proactor_ptr failing_factory(std::error_code& ec)
{
    ec = std::make_error_code(std::errc::function_not_supported);
    return nullptr;
}

} // namespace

// Case 1: the factory matches the build: without the backend it reports
// function_not_supported; with it, it either works or names a reason
SPANIO_TEST(io_uring_factory_matches_build)
{
    std::error_code ec;
    io_proactor_ptr proactor = make_io_uring_proactor(ec);
#if defined(SPANIO_HAS_IO_URING)
    if (proactor != nullptr)
    {
        SPANIO_CHECK(!ec);
        SPANIO_CHECK(std::strcmp(proactor->name(), "io-uring-proactor") == 0);
    }
    else
    {
        std::printf("    [kernel refuses io_uring: %s]\n", ec.message().c_str());
        SPANIO_CHECK(static_cast<bool>(ec));
    }
#else
    SPANIO_CHECK(proactor == nullptr);
    SPANIO_CHECK(ec == std::errc::function_not_supported);
#endif
}

// Case 2: when the preferred backend fails, the fallback is created and
// works (simulates a kernel without io_uring on every platform)
SPANIO_TEST(preferred_proactor_falls_back_when_preferred_fails)
{
    std::error_code ec;
    io_proactor_ptr proactor = make_preferred_proactor(&failing_factory, &make_reactor_proactor, ec);
    SPANIO_REQUIRE(!ec);
    SPANIO_REQUIRE(proactor != nullptr);
    SPANIO_CHECK(std::strcmp(proactor->name(), "reactor-proactor") == 0);

    io_operation op{};
    prepare_post(op);
    proactor->post(&op);
    SPANIO_CHECK(wait_all(*proactor, {&op}, kCompleteWaitMs));
    SPANIO_CHECK(!op.m_ec);

    // Both failing: the fallback's error is reported
    io_proactor_ptr none = make_preferred_proactor(&failing_factory, &failing_factory, ec);
    SPANIO_CHECK(none == nullptr);
    SPANIO_CHECK(ec == std::errc::function_not_supported);
}

#if defined(SPANIO_HAS_IO_URING)

namespace {

// Milliseconds elapsed since a starting point
// Parameter start: time the measurement began
// Return value: elapsed milliseconds
long long elapsed_ms(test_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(test_clock::now() - start).count();
}

// Poll for a while and count every operation returned
// Parameter proactor: proactor to poll
// Parameter wait_ms:  how long to keep polling
// Return value: number of operations returned in that time
std::size_t count_completions(io_proactor& proactor, int wait_ms)
{
    const auto deadline = test_clock::now() + std::chrono::milliseconds(wait_ms);
    completion_list out;
    std::size_t total = 0;
    do
    {
        std::error_code ec;
        proactor.poll(out, remaining_ms(deadline), ec);
        SPANIO_REQUIRE(!ec);
        total += out.size();
    } while (remaining_ms(deadline) > 0);
    return total;
}

// Create an io_uring proactor, or print why the kernel refuses it
// Parameter entries:    submission ring size
// Parameter cq_entries: completion ring size, 0 for the default
// Return value: the proactor, or nullptr (the case then skips)
std::unique_ptr<io_uring_proactor> make_uring(unsigned entries = kUringDefaultEntries,
                                              unsigned cq_entries = 0)
{
    std::error_code ec;
    std::unique_ptr<io_uring_proactor> proactor = io_uring_proactor::create(entries, ec, cq_entries);
    if (proactor == nullptr)
    {
        std::printf("    [skipped: io_uring unavailable: %s]\n", ec.message().c_str());
    }
    return proactor;
}

// A connected TCP pair registered with a proactor; unregisters on scope exit
struct registered_pair
{
    // Proactor both ends are registered with
    io_proactor* m_proactor;
    // Connecting side
    socket_guard m_client;
    // Accepted side
    socket_guard m_server;

    // Connect a pair and register both ends
    // Parameter proactor: proactor to register with
    explicit registered_pair(io_proactor& proactor)
        : m_proactor(&proactor)
    {
        spanio::test::make_tcp_pair(m_client, m_server);
        SPANIO_REQUIRE(!m_proactor->register_handle(m_client.get()));
        SPANIO_REQUIRE(!m_proactor->register_handle(m_server.get()));
    }

    // Unregister both ends (cancelling their I/O); the guards then close them
    ~registered_pair()
    {
        m_proactor->unregister_handle(m_client.get());
        m_proactor->unregister_handle(m_server.get());
    }

    registered_pair(const registered_pair&) = delete;
    registered_pair& operator=(const registered_pair&) = delete;
};

} // namespace

// Case 3: with a 4-entry submission ring and an 8-entry completion ring, 64
// operations whose completions arrive before anyone polls all come back
// exactly once with their data (submission ring reuse + CQ overflow list)
SPANIO_TEST(io_uring_tiny_rings_return_every_operation_once)
{
    std::unique_ptr<io_uring_proactor> proactor = make_uring(kTinySqEntries, kTinyCqEntries);
    if (proactor == nullptr)
    {
        return;
    }
    std::vector<std::unique_ptr<registered_pair>> pairs;
    std::vector<io_operation> ops(kOverflowOps);
    std::vector<std::vector<char>> buffers(kOverflowPairs, std::vector<char>(kSmallBufferSize));
    std::vector<io_operation*> expected;
    for (int i = 0; i < kOverflowPairs; ++i)
    {
        pairs.emplace_back(new registered_pair(*proactor));
        io_operation& recv_op = ops[static_cast<std::size_t>(i) * 2];
        io_operation& send_op = ops[static_cast<std::size_t>(i) * 2 + 1];
        prepare_recv(recv_op, pairs.back()->m_server.get(), buffers[static_cast<std::size_t>(i)].data(),
                     kSmallBufferSize);
        proactor->start(&recv_op);
        prepare_send(send_op, pairs.back()->m_client.get(), kMessage, kMessageLength);
        proactor->start(&send_op);
        expected.push_back(&recv_op);
        expected.push_back(&send_op);
    }

    SPANIO_REQUIRE(wait_all(*proactor, expected, kCompleteWaitMs));
    for (int i = 0; i < kOverflowPairs; ++i)
    {
        const io_operation& recv_op = ops[static_cast<std::size_t>(i) * 2];
        const io_operation& send_op = ops[static_cast<std::size_t>(i) * 2 + 1];
        SPANIO_CHECK(!recv_op.m_ec);
        SPANIO_CHECK(!send_op.m_ec);
        SPANIO_CHECK(send_op.m_bytes == kMessageLength);
        SPANIO_CHECK(recv_op.m_bytes == kMessageLength);
        SPANIO_CHECK(std::memcmp(buffers[static_cast<std::size_t>(i)].data(), kMessage, kMessageLength) == 0);
    }
    SPANIO_CHECK(count_completions(*proactor, kQuietWaitMs) == 0);
}

// Case 4: a poll() blocked without a time limit is woken by interrupt()
// and by post() from another thread
SPANIO_TEST(io_uring_cross_thread_interrupt_and_post_wake_blocked_poll)
{
    std::unique_ptr<io_uring_proactor> proactor = make_uring();
    if (proactor == nullptr)
    {
        return;
    }

    io_uring_proactor* const raw = proactor.get();
    auto interrupter = [raw]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kBlockBeforeWakeMs));
        raw->interrupt();
    };
    std::thread waker(interrupter);
    completion_list out;
    std::error_code ec;
    const auto start = test_clock::now();
    proactor->poll(out, -1, ec);
    waker.join();
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(out.empty());
    SPANIO_CHECK(elapsed_ms(start) < kCompleteWaitMs);

    io_operation op{};
    prepare_post(op);
    io_operation* const op_ptr = &op;
    auto poster = [raw, op_ptr]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kBlockBeforeWakeMs));
        raw->post(op_ptr);
    };
    std::thread posting(poster);
    // An interrupt left over from above may wake the first poll() early, so
    // keep polling until the post arrives
    const bool returned = wait_all(*proactor, {&op}, kCompleteWaitMs);
    posting.join();
    SPANIO_CHECK(returned);
    SPANIO_CHECK(!op.m_ec);
}

// Case 5: cancel() returns a pending receive once as cancelled; cancelling
// twice and then unregistering adds nothing; cancelling a socket whose
// receive already completed produces no second completion
SPANIO_TEST(io_uring_cancel_returns_each_operation_once)
{
    std::unique_ptr<io_uring_proactor> proactor = make_uring();
    if (proactor == nullptr)
    {
        return;
    }
    registered_pair pair(*proactor);
    char buffer[kSmallBufferSize] = {};

    io_operation pending{};
    prepare_recv(pending, pair.m_server.get(), buffer, sizeof(buffer));
    proactor->start(&pending);
    proactor->cancel(pair.m_server.get());
    proactor->cancel(pair.m_server.get());
    SPANIO_REQUIRE(wait_all(*proactor, {&pending}, kCompleteWaitMs));
    SPANIO_CHECK(pending.m_ec == std::errc::operation_canceled);

    // Data first, then the receive: it completes; a later cancel finds
    // nothing and must not produce a second completion
    SPANIO_REQUIRE(::send(pair.m_client.get(), kMessage, kMessageLength, kNoFlags)
                   == static_cast<long>(kMessageLength));
    io_operation done{};
    prepare_recv(done, pair.m_server.get(), buffer, sizeof(buffer));
    proactor->start(&done);
    SPANIO_REQUIRE(wait_all(*proactor, {&done}, kCompleteWaitMs));
    SPANIO_CHECK(!done.m_ec);
    SPANIO_CHECK(done.m_bytes == kMessageLength);
    proactor->cancel(pair.m_server.get());
    SPANIO_CHECK(!proactor->unregister_handle(pair.m_server.get()));
    SPANIO_CHECK(count_completions(*proactor, kQuietWaitMs) == 0);
    // Registered again so the pair's destructor finds it
    SPANIO_CHECK(!proactor->register_handle(pair.m_server.get()));
}

// Case 6: a datagram larger than the buffer reports message_size with the
// part that fit (recvmsg + MSG_TRUNC), and the sender's address
SPANIO_TEST(io_uring_truncated_datagram_reports_message_size)
{
    std::unique_ptr<io_uring_proactor> proactor = make_uring();
    if (proactor == nullptr)
    {
        return;
    }
    std::error_code ec;
    socket_guard receiver(create_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, ec));
    SPANIO_REQUIRE(!ec);
    socket_guard sender(create_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, ec));
    SPANIO_REQUIRE(!ec);
    const sockaddr_in any = spanio::test::loopback_address(spanio::test::kAnyPort);
    SPANIO_REQUIRE(::bind(receiver.get(), reinterpret_cast<const sockaddr*>(&any), sizeof(any)) == 0);
    SPANIO_REQUIRE(::bind(sender.get(), reinterpret_cast<const sockaddr*>(&any), sizeof(any)) == 0);
    const sockaddr_in to = spanio::test::local_address(receiver.get());
    const sockaddr_in from = spanio::test::local_address(sender.get());
    SPANIO_REQUIRE(!proactor->register_handle(receiver.get()));
    SPANIO_REQUIRE(!proactor->register_handle(sender.get()));

    char buffer[kTruncatedBufferSize] = {};
    io_operation receive_op{};
    prepare_receive_from(receive_op, receiver.get(), buffer, sizeof(buffer));
    proactor->start(&receive_op);
    io_operation send_op{};
    prepare_send_to(send_op, sender.get(), kLongDatagram, kLongDatagramLength,
                    reinterpret_cast<const sockaddr*>(&to), static_cast<socklen_type>(sizeof(to)));
    proactor->start(&send_op);

    SPANIO_REQUIRE(wait_all(*proactor, {&receive_op, &send_op}, kCompleteWaitMs));
    SPANIO_CHECK(!send_op.m_ec);
    SPANIO_CHECK(send_op.m_bytes == kLongDatagramLength);
    SPANIO_CHECK(receive_op.m_ec == std::errc::message_size);
    SPANIO_CHECK(receive_op.m_bytes == kTruncatedBufferSize);
    SPANIO_CHECK(std::memcmp(buffer, kLongDatagram, kTruncatedBufferSize) == 0);
    const sockaddr_in* const source = reinterpret_cast<const sockaddr_in*>(&receive_op.m_address);
    SPANIO_CHECK(receive_op.m_address_len == static_cast<socklen_type>(sizeof(sockaddr_in)));
    SPANIO_CHECK(source->sin_port == from.sin_port);

    proactor->unregister_handle(receiver.get());
    proactor->unregister_handle(sender.get());
}

// Case 7: poll() with nothing to do waits for its whole timeout (never
// returns early), returns at once with timeout 0, and does not spin
SPANIO_TEST(io_uring_poll_honours_timeout)
{
    std::unique_ptr<io_uring_proactor> proactor = make_uring();
    if (proactor == nullptr)
    {
        return;
    }
    completion_list out;
    std::error_code ec;

    auto start = test_clock::now();
    SPANIO_CHECK(proactor->poll(out, kPollTimeoutMs, ec) == 0);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(elapsed_ms(start) >= kPollTimeoutMs);
    SPANIO_CHECK(elapsed_ms(start) < kCompleteWaitMs);

    start = test_clock::now();
    SPANIO_CHECK(proactor->poll(out, 0, ec) == 0);
    SPANIO_CHECK(!ec);
    SPANIO_CHECK(elapsed_ms(start) < kImmediateReturnMs);
}

// Case 8: creating a proactor, running a cancelled operation through it
// and destroying it, many times, leaks neither rings nor descriptors
// (checked by the sanitizer runs) and never hangs
SPANIO_TEST(io_uring_create_cancel_destroy_cycles)
{
    for (int i = 0; i < kLifecycleCycles; ++i)
    {
        std::unique_ptr<io_uring_proactor> proactor = make_uring(kTinySqEntries);
        if (proactor == nullptr)
        {
            return;
        }
        socket_guard client;
        socket_guard server;
        spanio::test::make_tcp_pair(client, server);
        SPANIO_REQUIRE(!proactor->register_handle(server.get()));
        char buffer[kSmallBufferSize] = {};
        io_operation op{};
        prepare_recv(op, server.get(), buffer, sizeof(buffer));
        proactor->start(&op);
        SPANIO_REQUIRE(!proactor->unregister_handle(server.get()));
        // The kernel already cancelled the receive, so closing now is safe
        server.reset();
        SPANIO_REQUIRE(wait_all(*proactor, {&op}, kCompleteWaitMs));
        SPANIO_CHECK(op.m_ec == std::errc::operation_canceled);
    }
}

#endif // SPANIO_HAS_IO_URING

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
