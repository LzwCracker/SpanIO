// ============================================================================
// File purpose: tcp_server, the high-level way to write a TCP service. It
//               owns the accept loop, connection lifetimes, send queues,
//               message framing and (optionally) the run() threads, so a
//               service only supplies callbacks.
// Usage:        spanio::tcp_server server(8080);
//               auto on_message = [](const spanio::tcp_connection_ptr& conn,
//                                    std::string_view data)
//               {
//                   conn->send(data);
//               };
//               server.on_message(on_message);
//               server.run(4);      // blocks until stop(), on 4 threads
//
//               Callback guarantees:
//               - on_connect returns before the first read starts, and
//                 on_close runs exactly once, after every other callback of
//                 that connection.
//               - Callbacks of one connection never run at the same time.
//                 Callbacks of different connections may, so state shared
//                 between connections needs a lock unless run(1) is used.
//               - The string_view given to on_message is valid only during
//                 the call.
//               - An exception from on_connect or on_message closes that
//                 connection with error::misc_errors::handler_failed; the server keeps
//                 running. An exception from on_close is only logged.
//
//               on_close reports why the connection ended: empty for a local
//               close(), abort() or stop(); error::misc_errors::eof when the peer closed;
//               error::misc_errors::message_too_large, error::misc_errors::send_queue_full,
//               error::misc_errors::framing_error, error::misc_errors::handler_failed,
//               error::misc_errors::idle_timeout or error::misc_errors::close_timeout when a limit
//               or callback ended it; otherwise the system error.
//
//               Without a framer, on_message gets whatever each read
//               returned. With one (framing.h) it gets whole messages.
//
//               Rules:
//               - Set callbacks and the framer before start() / run().
//               - A server starts once; after stop(), create a new one.
//               - stop() closes connections at once and drops unsent data;
//                 stop_gracefully(grace) lets them send what is queued first.
//               - close() waits for queued data at most m_close_timeout, and
//                 m_idle_timeout (off by default) closes silent connections.
//               - A tcp_connection_ptr must not outlive its tcp_server.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05
// ============================================================================

#ifndef SPANIO_TCP_SERVER_H
#define SPANIO_TCP_SERVER_H

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/framing.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/tcp.h"
#include "spanio/timer.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/run_threads.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace spanio { namespace detail {

// Default connection limit
constexpr std::size_t kDefaultMaxConnections = 10000;

// Default limit for one framed message: 1 MiB
constexpr std::size_t kDefaultMaxMessageSize = 1024 * 1024;

// Default limit for output queued on one connection: 16 MiB
constexpr std::size_t kDefaultMaxSendQueueBytes = 16 * 1024 * 1024;

// Default size of each connection's read buffer: 64 KiB
constexpr std::size_t kDefaultReadBufferSize = 64 * 1024;

// Idle timeout used unless set: off
constexpr std::chrono::milliseconds kDefaultIdleTimeout(0);

// Longest a close() waits for queued data unless set: 30 seconds
constexpr std::chrono::milliseconds kDefaultCloseTimeout(30000);

// Longest a connection waits for the peer to close after everything was
// sent, before closing anyway (see tcp_connection::finish_flush_locked)
constexpr std::chrono::milliseconds kLingerTimeout(2000);

}}

namespace spanio {

// Limits of a tcp_server
struct tcp_server_options
{
    // Connections served at once; further ones are accepted and closed at
    // once, without on_connect. 0 means unlimited.
    std::size_t m_max_connections = detail::kDefaultMaxConnections;
    // Largest framed message, and the most input buffered while waiting for
    // one. Exceeding it closes the connection. Ignored without a framer.
    std::size_t m_max_message_size = detail::kDefaultMaxMessageSize;
    // Most bytes queued for sending on one connection. Exceeding it closes
    // the connection, so a peer that stops reading cannot exhaust memory.
    std::size_t m_max_send_queue_bytes = detail::kDefaultMaxSendQueueBytes;
    // Bytes read from a socket at a time; must not be 0
    std::size_t m_read_buffer_size = detail::kDefaultReadBufferSize;
    // Close a connection that had no activity for this long: no data
    // received and no send progress, so a client that only downloads is not
    // idle. on_close gets error::misc_errors::idle_timeout. 0 turns it off.
    std::chrono::milliseconds m_idle_timeout = detail::kDefaultIdleTimeout;
    // Longest a close() may wait for queued data to go out; after that the
    // connection is closed without it and on_close gets error::misc_errors::close_timeout.
    // 0 waits for as long as it takes.
    std::chrono::milliseconds m_close_timeout = detail::kDefaultCloseTimeout;
};

class tcp_connection;

// Shared handle of a connection, passed to the callbacks
using tcp_connection_ptr = std::shared_ptr<tcp_connection>;

// Called for a new connection: void(const tcp_connection_ptr&)
using tcp_connect_callback = std::function<void(const tcp_connection_ptr&)>;

// Called for received data: void(const tcp_connection_ptr&, std::string_view)
using tcp_message_callback = std::function<void(const tcp_connection_ptr&, std::string_view)>;

// Called when a connection ended: void(const tcp_connection_ptr&, std::error_code)
using tcp_close_callback = std::function<void(const tcp_connection_ptr&, std::error_code)>;

}

namespace spanio { namespace detail {

class tcp_server_state;

}}

namespace spanio {

// One client connection of a tcp_server. Created by the server and handed
// to the callbacks. All methods are safe from any thread, except context(),
// which is unsynchronized and meant for use inside this connection's
// callbacks (they never overlap).
//
// Design: a chain of reads runs for as long as the connection does, and
// every callback comes from that chain. That is what keeps the callbacks of
// one connection from overlapping. Writes run separately, one at a time, and
// never call the user. on_close fires once the read chain has ended and no
// write is in flight, so nothing can follow it.
class tcp_connection : public std::enable_shared_from_this<tcp_connection>
{
public:
    tcp_connection(const tcp_connection&) = delete;
    tcp_connection& operator=(const tcp_connection&) = delete;

    // Queue data for sending
    // Parameter data: bytes to send; copied, so they need not stay valid
    // Return value: true if queued (or empty); false if the connection is
    //               closing or closed, or the queue limit was exceeded, in
    //               which case the connection is closed with
    //               error::misc_errors::send_queue_full
    bool send(std::string_view data)
    {
        return enqueue(std::string(data));
    }

    // Queue a zero-terminated string for sending
    // Parameter data: text to send, without its terminating zero
    // Return value: same as send(std::string_view)
    bool send(const char* data)
    {
        return enqueue(std::string(data));
    }

    // Queue data for sending without copying it
    // Parameter data: bytes to send; moved into the queue
    // Return value: same as send(std::string_view)
    bool send(std::string&& data)
    {
        return enqueue(std::move(data));
    }

    // Send everything already queued, then close. No further on_message is
    // delivered, and send() returns false. Waits at most
    // tcp_server_options::m_close_timeout for the queue to drain (0 waits
    // without limit); after that the connection is closed without the rest
    // and on_close gets error::misc_errors::close_timeout.
    void close();

    // Close now, dropping queued data
    void abort();

    // Address of the peer; still valid after the connection closed
    // Return value: the endpoint
    tcp::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    // Whether the connection still accepts data to send
    // Return value: false once closing or closed
    bool is_open() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_phase == phase::kOpen;
    }

    // Slot for user data about this connection, e.g. a session object
    // Return value: reference to the slot; empty until set
    std::any& user_data() noexcept
    {
        return m_user_data;
    }

private:
    friend class detail::tcp_server_state;

    // Life cycle of a connection
    enum class phase
    {
        // Normal operation
        kOpen,
        // No more new output: sending what is queued, then closing
        kClosing,
        // The socket is closed
        kClosed
    };

    // Parameter state:  shared server state
    // Parameter socket: the accepted connection
    // Parameter remote: address of the peer
    tcp_connection(std::shared_ptr<detail::tcp_server_state> state, tcp::socket socket,
                   const tcp::endpoint& remote);

    // Run on_connect, then start the read chain
    void begin();

    // Add data to the send queue; see send()
    // Parameter data: non-owning view is not possible here: the queue owns it
    // Return value: see send()
    bool enqueue(std::string data);

    // Start the next read, or end the read chain if the socket is closed
    void continue_reading();

    // Completion of a read
    // Parameter ec:    read result
    // Parameter bytes: bytes read into m_read_buffer
    void on_read(std::error_code ec, std::size_t bytes);

    // End the read chain because of a read result
    // Parameter ec: the error that ended it, e.g. error::misc_errors::eof
    void handle_read_end(const std::error_code& ec);

    // Pass received bytes to on_message, through the framer if there is one
    // Parameter bytes: number of bytes in m_read_buffer
    void deliver(std::size_t bytes);

    // Call on_message for one message, turning exceptions into a close
    // Parameter data: the message
    void deliver_one(std::string_view data);

    // Start sending the front of the queue. Caller holds m_mutex.
    void start_write_locked();

    // Completion of a write
    // Parameter ec:    write result
    // Parameter bytes: bytes sent
    void on_written(std::error_code ec, std::size_t bytes);

    // Close with a reason
    // Parameter reason: reason reported to on_close
    void fail(const std::error_code& reason);

    // Close now, unless already closed. Caller holds m_mutex.
    // Parameter reason: reason reported to on_close, if none was set yet
    void fail_locked(const std::error_code& reason);

    // Remember why the connection ends; the first reason wins. Caller holds
    // m_mutex.
    // Parameter reason: the reason
    void record_reason_locked(const std::error_code& reason);

    // Everything queued was sent: shut down sending, then keep reading (and
    // discarding) until the peer closes too, at most the linger limit.
    // Closing while received data is still unread would make the system send
    // RST instead of FIN, and the peer could lose the data just sent. Caller
    // holds m_mutex.
    // Parameter limit: close limit in effect; the linger wait is the smaller
    //                  of it and detail::kLingerTimeout (0 means detail::kLingerTimeout)
    void finish_flush_locked(std::chrono::milliseconds limit);

    // Close the socket and drop queued data. Caller holds m_mutex.
    void close_socket_locked();

    // Like close(), with a given limit for sending what is queued. If the
    // connection is already closing, only an earlier deadline is applied.
    // Parameter limit: longest wait; 0 waits for as long as it takes
    void close_within(std::chrono::milliseconds limit);

    // Start or move the close deadline. Caller holds m_mutex.
    // Parameter limit: time from now; 0 sets no deadline
    void arm_close_timer_locked(std::chrono::milliseconds limit);

    // Close deadline reached: give up on the queued data
    // Parameter ec: wait result; cancelled when the close finished in time
    void on_close_timer(const std::error_code& ec);

    // Wait for the idle deadline. Caller holds m_mutex.
    // Parameter deadline: when the connection counts as idle
    void wait_for_idle_locked(steady_timer::time_point deadline);

    // Idle deadline reached: close, unless there was activity since it was
    // set, in which case wait again from the last activity
    // Parameter ec: wait result; cancelled once the connection closed
    void on_idle_timer(const std::error_code& ec);

    // Remember that the connection was active now, for the idle timeout
    void note_activity() noexcept;

    // Run on_close if the read chain has ended and no write is in flight
    void try_finish();

    // Shared server state: callbacks, limits, connection registry
    std::shared_ptr<detail::tcp_server_state> m_state;
    // Address of the peer, kept for use after the socket closed
    tcp::endpoint m_remote;
    // Guards every field below except m_read_buffer, m_input and m_user_data,
    // which only the read chain and the callbacks touch
    mutable std::mutex m_mutex;
    // The connection
    tcp::socket m_socket;
    // Where the connection is in its life cycle
    phase m_phase;
    // Output not sent yet. The front element is in flight while m_writing,
    // so it stays in place until its write comes back.
    std::deque<std::string> m_send_queue;
    // Bytes in m_send_queue, counted against the queue limit
    std::size_t m_queued_bytes;
    // Bytes of the front element already sent
    std::size_t m_write_offset;
    // A write is in flight
    bool m_writing;
    // The read chain has ended
    bool m_read_done;
    // on_close has been started
    bool m_close_fired;
    // m_close_reason has been set
    bool m_has_reason;
    // Value passed to on_close
    std::error_code m_close_reason;
    // Destination of the reads
    std::vector<char> m_read_buffer;
    // Bytes waiting for the framer to find a complete message
    std::string m_input;
    // Ends an idle connection; guarded by m_mutex like the socket
    steady_timer m_idle_timer;
    // Ends a close() that waits too long for queued data; guarded by m_mutex
    steady_timer m_close_timer;
    // m_close_timer has a wait pending
    bool m_close_timer_armed;
    // Everything queued was sent and sending is shut down; the connection
    // only waits for the peer to close
    bool m_flushed;
    // Time of the last data received or sent, as steady_clock ticks; written
    // without m_mutex by the read and write paths
    std::atomic<steady_timer::clock::rep> m_last_activity;
    // User data; see context()
    std::any m_user_data;
};

}

namespace spanio { namespace detail {

// State shared by a tcp_server, its handlers and its connections. Handlers
// keep it alive through a shared_ptr, so one that runs after the tcp_server
// object is gone (on an external context) is still safe. The registry holds
// weak pointers: connections keep this state alive, so strong ones would
// form a cycle.
class tcp_server_state : public std::enable_shared_from_this<tcp_server_state>
{
public:
    // Open, bind and listen; throws std::system_error on failure
    // Parameter context: context the server runs on
    // Parameter local:   address and port to listen on
    // Parameter options: limits; std::invalid_argument if a read buffer of
    //                    size 0 is requested
    tcp_server_state(io_context& context, const tcp::endpoint& local, const tcp_server_options& options)
        : m_options(options)
        , m_acceptor(context, local)
        , m_started(false)
        , m_stopped(false)
    {
        if (options.m_read_buffer_size == 0)
        {
            throw std::invalid_argument("spanio::tcp_server: read buffer size must not be 0");
        }
    }

    // Start the accept loop, once. Throws std::logic_error if the server was
    // already started or stopped.
    void start()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_started || m_stopped)
        {
            throw std::logic_error("spanio::tcp_server: a server starts only once");
        }
        m_started = true;
        accept_next_locked();
    }

    // Start the accept loop unless that already happened. Throws
    // std::logic_error if the server was stopped.
    void ensure_started()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            throw std::logic_error("spanio::tcp_server: a stopped server cannot run again");
        }
        if (!m_started)
        {
            m_started = true;
            accept_next_locked();
        }
    }

    // Stop accepting and close() every connection, giving them a limited
    // time to send what is queued
    // Parameter grace: longest wait; 0 waits for as long as it takes
    void stop_gracefully(std::chrono::milliseconds grace)
    {
        std::vector<tcp_connection_ptr> live = stop_accepting();
        for (const tcp_connection_ptr& connection : live)
        {
            connection->close_within(grace);
        }
    }

    // Stop accepting and close every connection at once
    void stop()
    {
        std::vector<tcp_connection_ptr> live = stop_accepting();
        for (const tcp_connection_ptr& connection : live)
        {
            connection->abort();
        }
    }

    // Close the acceptor and collect the connections still alive
    // Return value: the connections
    std::vector<tcp_connection_ptr> stop_accepting()
    {
        std::vector<tcp_connection_ptr> live;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stopped = true;
            m_acceptor.close();
            live.reserve(m_connections.size());
            for (const auto& entry : m_connections)
            {
                tcp_connection_ptr connection = entry.second.lock();
                if (connection != nullptr)
                {
                    live.push_back(std::move(connection));
                }
            }
        }
        return live;
    }

    // Whether start() was called
    // Return value: true once started
    bool started() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_started;
    }

    // Throw unless callbacks may still be changed
    void check_not_started() const
    {
        if (started())
        {
            throw std::logic_error("spanio::tcp_server: set callbacks before start()");
        }
    }

    // Local listening address
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_acceptor.local_endpoint(ec);
    }

    // Number of connections being served
    // Return value: the count
    std::size_t connection_count() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_connections.size();
    }

    // A connection ended: forget it and run on_close
    // Parameter connection: the connection
    // Parameter reason:     why it ended
    void connection_closed(const tcp_connection_ptr& connection, const std::error_code& reason)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_connections.erase(connection.get());
        }
        if (!m_on_close)
        {
            return;
        }
        try
        {
            m_on_close(connection, reason);
        }
        catch (...)
        {
            log_error("tcp_server: on_close threw an exception",
                      make_error_code(error::misc_errors::handler_failed));
        }
    }

    // Limits of the server; fixed after construction
    const tcp_server_options m_options;
    // Called for a new connection; fixed after start()
    tcp_connect_callback m_on_connect;
    // Called for received data; fixed after start()
    tcp_message_callback m_on_message;
    // Called when a connection ended; fixed after start()
    tcp_close_callback m_on_close;
    // Splits input into messages, if set; fixed after start()
    framer m_framer;

private:
    // Start the next accept. Caller holds m_mutex.
    void accept_next_locked()
    {
        if (m_stopped || !m_acceptor.is_open())
        {
            return;
        }
        std::shared_ptr<tcp_server_state> self = shared_from_this();
        auto accept_done = [self](std::error_code ec, tcp::socket peer)
        {
            self->on_accept(ec, std::move(peer));
        };
        m_acceptor.async_accept(accept_done);
    }

    // Serve a new connection and accept the next one. The next accept
    // starts first, so a slow on_connect does not hold up new clients.
    // Parameter ec:   accept result
    // Parameter peer: the new connection on success
    void on_accept(std::error_code ec, tcp::socket peer)
    {
        if (ec == std::errc::operation_canceled)
        {
            return;
        }
        std::shared_ptr<tcp_connection> connection;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopped)
            {
                return;
            }
            if (ec)
            {
                log_error("tcp_server: accept", ec);
            }
            else if (m_options.m_max_connections == 0
                     || m_connections.size() < m_options.m_max_connections)
            {
                connection = make_connection(std::move(peer));
            }
            // Over the limit: peer goes out of scope, which closes it
            accept_next_locked();
        }
        if (connection != nullptr)
        {
            connection->begin();
        }
    }

    // Create a connection and add it to the registry. Caller holds m_mutex.
    // Parameter peer: the accepted socket
    // Return value: the connection, or nullptr if its address is unavailable
    //               because the peer is already gone
    tcp_connection_ptr make_connection(tcp::socket peer)
    {
        std::error_code ec;
        const tcp::endpoint remote = peer.remote_endpoint(ec);
        if (ec)
        {
            log_error("tcp_server: remote_endpoint", ec);
            return nullptr;
        }
        tcp_connection_ptr connection(new tcp_connection(shared_from_this(), std::move(peer), remote));
        m_connections.emplace(connection.get(), connection);
        return connection;
    }

    // Guards the fields below
    mutable std::mutex m_mutex;
    // Listening socket
    tcp::acceptor m_acceptor;
    // Connections being served
    std::unordered_map<tcp_connection*, std::weak_ptr<tcp_connection>> m_connections;
    // start() was called
    bool m_started;
    // stop() was called
    bool m_stopped;
};

}}

namespace spanio {

inline tcp_connection::tcp_connection(std::shared_ptr<detail::tcp_server_state> state, tcp::socket socket,
                                      const tcp::endpoint& remote)
    : m_state(std::move(state))
    , m_remote(remote)
    , m_socket(std::move(socket))
    , m_phase(phase::kOpen)
    , m_queued_bytes(0)
    , m_write_offset(0)
    , m_writing(false)
    , m_read_done(false)
    , m_close_fired(false)
    , m_has_reason(false)
    , m_read_buffer(m_state->m_options.m_read_buffer_size)
    , m_idle_timer(m_socket.context())
    , m_close_timer(m_socket.context())
    , m_close_timer_armed(false)
    , m_flushed(false)
    , m_last_activity(steady_timer::clock::now().time_since_epoch().count())
{
}

inline void tcp_connection::close()
{
    close_within(m_state->m_options.m_close_timeout);
}

inline void tcp_connection::close_within(std::chrono::milliseconds limit)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_phase == phase::kClosing)
    {
        arm_close_timer_locked(limit);
        return;
    }
    if (m_phase != phase::kOpen)
    {
        return;
    }
    record_reason_locked(std::error_code());
    if (m_writing)
    {
        m_phase = phase::kClosing;
        arm_close_timer_locked(limit);
    }
    else
    {
        finish_flush_locked(limit);
    }
}

inline void tcp_connection::arm_close_timer_locked(std::chrono::milliseconds limit)
{
    if (limit.count() <= 0)
    {
        return;
    }
    const steady_timer::time_point deadline = steady_timer::clock::now() + limit;
    if (m_close_timer_armed && deadline >= m_close_timer.expiry())
    {
        return;
    }
    // Moving the deadline cancels the earlier wait; its handler then sees
    // operation_canceled and does nothing
    m_close_timer.expires_at(deadline);
    tcp_connection_ptr self = shared_from_this();
    auto close_deadline = [self](std::error_code ec)
    {
        self->on_close_timer(ec);
    };
    m_close_timer.async_wait(close_deadline);
    m_close_timer_armed = true;
}

inline void tcp_connection::on_close_timer(const std::error_code& ec)
{
    if (ec)
    {
        return;
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    m_close_timer_armed = false;
    if (m_phase != phase::kClosing)
    {
        return;
    }
    if (m_flushed)
    {
        // Everything was sent; only the peer's close did not come in time.
        // That is not a failure, so the recorded reason stays.
        close_socket_locked();
        return;
    }
    // Overrides the reason close() recorded: the data was not all sent
    m_close_reason = make_error_code(error::misc_errors::close_timeout);
    m_has_reason = true;
    detail::log_error("tcp_server: close timed out", m_close_reason);
    close_socket_locked();
}

inline void tcp_connection::wait_for_idle_locked(steady_timer::time_point deadline)
{
    m_idle_timer.expires_at(deadline);
    tcp_connection_ptr self = shared_from_this();
    auto idle_deadline = [self](std::error_code ec)
    {
        self->on_idle_timer(ec);
    };
    m_idle_timer.async_wait(idle_deadline);
}

inline void tcp_connection::on_idle_timer(const std::error_code& ec)
{
    if (ec)
    {
        return;
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_phase != phase::kOpen)
    {
        return;
    }
    // The timer is not moved on every read or write; it is checked here
    // against the last activity instead, which keeps the I/O paths cheap
    const steady_timer::time_point last(steady_timer::duration(m_last_activity.load()));
    const steady_timer::time_point deadline = last + m_state->m_options.m_idle_timeout;
    if (steady_timer::clock::now() >= deadline)
    {
        fail_locked(error::misc_errors::idle_timeout);
        return;
    }
    wait_for_idle_locked(deadline);
}

inline void tcp_connection::note_activity() noexcept
{
    if (m_state->m_options.m_idle_timeout.count() > 0)
    {
        m_last_activity.store(steady_timer::clock::now().time_since_epoch().count());
    }
}

inline void tcp_connection::abort()
{
    fail(std::error_code());
}

inline void tcp_connection::begin()
{
    if (m_state->m_on_connect)
    {
        try
        {
            m_state->m_on_connect(shared_from_this());
        }
        catch (...)
        {
            detail::log_error("tcp_server: on_connect threw an exception",
                              make_error_code(error::misc_errors::handler_failed));
            fail(error::misc_errors::handler_failed);
        }
    }
    const std::chrono::milliseconds idle = m_state->m_options.m_idle_timeout;
    if (idle.count() > 0)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_phase == phase::kOpen)
        {
            note_activity();
            wait_for_idle_locked(steady_timer::clock::now() + idle);
        }
    }
    continue_reading();
}

inline bool tcp_connection::enqueue(std::string data)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_phase != phase::kOpen)
    {
        return false;
    }
    if (data.empty())
    {
        return true;
    }
    if (m_queued_bytes + data.size() > m_state->m_options.m_max_send_queue_bytes)
    {
        fail_locked(error::misc_errors::send_queue_full);
        return false;
    }
    m_queued_bytes += data.size();
    m_send_queue.push_back(std::move(data));
    if (!m_writing)
    {
        m_writing = true;
        start_write_locked();
    }
    return true;
}

inline void tcp_connection::continue_reading()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_phase != phase::kClosed)
        {
            tcp_connection_ptr self = shared_from_this();
            auto read_done = [self](std::error_code ec, std::size_t bytes)
            {
                self->on_read(ec, bytes);
            };
            m_socket.async_read_some(spanio::buffer(m_read_buffer.data(), m_read_buffer.size()), read_done);
            return;
        }
        m_read_done = true;
    }
    try_finish();
}

inline void tcp_connection::on_read(std::error_code ec, std::size_t bytes)
{
    if (ec)
    {
        handle_read_end(ec);
        return;
    }
    note_activity();
    deliver(bytes);
    continue_reading();
}

inline void tcp_connection::handle_read_end(const std::error_code& ec)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_read_done = true;
        if (m_phase != phase::kClosed)
        {
            record_reason_locked(ec);
            // After eof the peer may still be reading (it only shut down its
            // sending side), so what is queued still goes out
            if (ec == error::misc_errors::eof && m_writing)
            {
                m_phase = phase::kClosing;
                arm_close_timer_locked(m_state->m_options.m_close_timeout);
            }
            else
            {
                close_socket_locked();
            }
        }
    }
    try_finish();
}

inline void tcp_connection::deliver(std::size_t bytes)
{
    const std::string_view received(m_read_buffer.data(), bytes);
    if (!is_open())
    {
        return;
    }
    const framer& split = m_state->m_framer;
    if (!split)
    {
        deliver_one(received);
        return;
    }

    const std::size_t limit = m_state->m_options.m_max_message_size;
    m_input.append(received.data(), received.size());
    std::size_t offset = 0;
    while (offset < m_input.size() && is_open())
    {
        const std::string_view rest = std::string_view(m_input).substr(offset);
        frame_result frame;
        try
        {
            frame = split(rest);
        }
        catch (...)
        {
            detail::log_error("tcp_server: framer threw an exception",
                              make_error_code(error::misc_errors::handler_failed));
            fail(error::misc_errors::handler_failed);
            return;
        }
        if (frame.m_error || frame.m_consumed > rest.size())
        {
            fail(error::misc_errors::framing_error);
            return;
        }
        if (frame.m_consumed == 0)
        {
            break;
        }
        if (frame.m_payload.size() > limit)
        {
            fail(error::misc_errors::message_too_large);
            return;
        }
        offset += frame.m_consumed;
        deliver_one(frame.m_payload);
    }
    m_input.erase(0, offset);
    if (m_input.size() > limit)
    {
        fail(error::misc_errors::message_too_large);
    }
}

inline void tcp_connection::deliver_one(std::string_view data)
{
    if (!m_state->m_on_message)
    {
        return;
    }
    try
    {
        m_state->m_on_message(shared_from_this(), data);
    }
    catch (...)
    {
        detail::log_error("tcp_server: on_message threw an exception",
                          make_error_code(error::misc_errors::handler_failed));
        fail(error::misc_errors::handler_failed);
    }
}

inline void tcp_connection::start_write_locked()
{
    const std::string& front = m_send_queue.front();
    tcp_connection_ptr self = shared_from_this();
    auto write_done = [self](std::error_code ec, std::size_t bytes)
    {
        self->on_written(ec, bytes);
    };
    // One send at a time under m_mutex, rather than async_write: a partial
    // send must not continue on a socket another thread has just closed
    m_socket.async_write_some(spanio::buffer(front.data() + m_write_offset, front.size() - m_write_offset),
                              write_done);
}

inline void tcp_connection::on_written(std::error_code ec, std::size_t bytes)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_writing = false;
        if (m_phase == phase::kClosed)
        {
            m_send_queue.clear();
        }
        else if (ec || bytes == 0)
        {
            // A send of a non-empty buffer never reports 0 bytes without an
            // error; closing avoids looping forever if it ever does
            fail_locked(ec ? ec : std::make_error_code(std::errc::io_error));
        }
        else
        {
            note_activity();
            m_write_offset += bytes;
            if (m_write_offset == m_send_queue.front().size())
            {
                m_queued_bytes -= m_send_queue.front().size();
                m_send_queue.pop_front();
                m_write_offset = 0;
            }
            if (!m_send_queue.empty())
            {
                m_writing = true;
                start_write_locked();
                return;
            }
            if (m_phase == phase::kClosing)
            {
                finish_flush_locked(m_state->m_options.m_close_timeout);
            }
        }
    }
    try_finish();
}

inline void tcp_connection::fail(const std::error_code& reason)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    fail_locked(reason);
}

inline void tcp_connection::fail_locked(const std::error_code& reason)
{
    if (m_phase == phase::kClosed)
    {
        return;
    }
    record_reason_locked(reason);
    close_socket_locked();
}

inline void tcp_connection::record_reason_locked(const std::error_code& reason)
{
    if (!m_has_reason)
    {
        m_has_reason = true;
        m_close_reason = reason;
    }
}

inline void tcp_connection::finish_flush_locked(std::chrono::milliseconds limit)
{
    const std::error_code ec = m_socket.shutdown(tcp::shutdown_type::send);
    if (ec)
    {
        detail::log_error("tcp_server: shutdown", ec);
    }
    // Nothing left to wait for when the peer already closed or the
    // connection is broken
    if (m_read_done || ec)
    {
        close_socket_locked();
        return;
    }
    // The read chain keeps running and discards what arrives; its end of
    // file closes the socket (handle_read_end)
    m_phase = phase::kClosing;
    m_flushed = true;
    const bool use_limit = limit.count() > 0 && limit < detail::kLingerTimeout;
    arm_close_timer_locked(use_limit ? limit : detail::kLingerTimeout);
}

inline void tcp_connection::close_socket_locked()
{
    m_phase = phase::kClosed;
    if (m_writing)
    {
        // The kernel may still read the element in flight; it is freed when
        // its write comes back
        m_send_queue.erase(m_send_queue.begin() + 1, m_send_queue.end());
    }
    else
    {
        m_send_queue.clear();
    }
    m_socket.close();
    // Pending timer waits keep the connection alive and count as work, so
    // they must end with the connection
    m_idle_timer.cancel();
    m_close_timer.cancel();
    m_close_timer_armed = false;
}

inline void tcp_connection::try_finish()
{
    std::error_code reason;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_read_done || m_writing || m_close_fired)
        {
            return;
        }
        m_close_fired = true;
        reason = m_close_reason;
    }
    m_state->connection_closed(shared_from_this(), reason);
}

// A TCP service built from callbacks. See the file header for the guarantees
// and rules.
class tcp_server
{
public:
    // Listen on 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port:    port to listen on; 0 picks a free one
    // Parameter options: limits
    explicit tcp_server(std::uint16_t port, const tcp_server_options& options = tcp_server_options())
        : tcp_server(tcp::endpoint(ipv4::any(), port), options)
    {
    }

    // Listen on an address, on a context owned by the server. Throws
    // std::system_error if the address cannot be bound.
    // Parameter local:   address and port to listen on
    // Parameter options: limits
    explicit tcp_server(const tcp::endpoint& local, const tcp_server_options& options = tcp_server_options())
        : m_owned_context(new io_context())
        , m_context(m_owned_context.get())
        , m_state(std::make_shared<detail::tcp_server_state>(*m_context, local, options))
    {
    }

    // Listen on an address, on a context the caller runs. Use start(), not
    // run(). Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to listen on
    // Parameter options: limits
    tcp_server(io_context& context, const tcp::endpoint& local,
               const tcp_server_options& options = tcp_server_options())
        : m_context(&context)
        , m_state(std::make_shared<detail::tcp_server_state>(context, local, options))
    {
    }

    tcp_server(const tcp_server&) = delete;
    tcp_server& operator=(const tcp_server&) = delete;

    // Stop the server, closing every connection
    ~tcp_server()
    {
        m_state->stop();
    }

    // Set the callback for new connections; before start() only
    // Parameter callback: void(const tcp_connection_ptr&)
    template <typename Callback>
    void on_connect(Callback&& callback)
    {
        m_state->check_not_started();
        m_state->m_on_connect = std::forward<Callback>(callback);
    }

    // Set the callback for received data; before start() only
    // Parameter callback: void(const tcp_connection_ptr&, std::string_view)
    template <typename Callback>
    void on_message(Callback&& callback)
    {
        m_state->check_not_started();
        m_state->m_on_message = std::forward<Callback>(callback);
    }

    // Set the callback for ended connections; before start() only
    // Parameter callback: void(const tcp_connection_ptr&, std::error_code)
    template <typename Callback>
    void on_close(Callback&& callback)
    {
        m_state->check_not_started();
        m_state->m_on_close = std::forward<Callback>(callback);
    }

    // Set how input is split into messages; before start() only
    // Parameter split: e.g. line_framer(); see framing.h
    template <typename Framer>
    void set_framer(Framer&& split)
    {
        m_state->check_not_started();
        m_state->m_framer = std::forward<Framer>(split);
    }

    // Start accepting, without blocking; the context must be run by someone.
    // Throws std::logic_error if already started or stopped.
    void start()
    {
        m_state->start();
    }

    // Start if needed, then run the owned context until stop(). Throws
    // std::logic_error on an external context or after stop(), and
    // std::invalid_argument for 0 threads.
    // Parameter threads: threads to run on, including the calling one
    void run(unsigned threads = 1)
    {
        if (m_owned_context == nullptr)
        {
            throw std::logic_error("spanio::tcp_server: run() needs the owned context; run yours instead");
        }
        if (threads == 0)
        {
            throw std::invalid_argument("spanio::tcp_server: run() needs at least one thread");
        }
        m_state->ensure_started();

        detail::run_on_threads(*m_owned_context, threads);
    }

    // Stop accepting and close every connection at once, dropping unsent
    // data. run() returns once their on_close callbacks have run. Safe from
    // any thread and from callbacks.
    void stop()
    {
        m_state->stop();
    }

    // Stop accepting at once, and close() every connection: no further
    // on_message, queued data is still sent. Connections not closed within
    // the grace period are closed without their data (on_close gets
    // error::misc_errors::close_timeout). run() returns once all of them have closed.
    // Does not block; safe from any thread and from callbacks. A later
    // stop() ends the wait at once.
    // Parameter grace: longest wait; 0 waits for as long as it takes
    void stop_gracefully(std::chrono::milliseconds grace)
    {
        m_state->stop_gracefully(grace);
    }

    // Address the server listens on, e.g. to learn the port picked for 0
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_state->local_endpoint(ec);
    }

    // Number of connections being served
    // Return value: the count
    std::size_t connection_count() const
    {
        return m_state->connection_count();
    }

    // The context the server runs on
    // Return value: the owned or the external context
    io_context& context() noexcept
    {
        return *m_context;
    }

private:
    // Context owned by the server; null on an external context. Declared
    // first so it is destroyed last: it may still hold connections.
    std::unique_ptr<io_context> m_owned_context;
    // The context in use, owned or external
    io_context* m_context;
    // Everything handlers and connections share with the server
    std::shared_ptr<detail::tcp_server_state> m_state;
};

}

#endif // SPANIO_TCP_SERVER_H
