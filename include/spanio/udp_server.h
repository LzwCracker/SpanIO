// ============================================================================
// File purpose: udp_server, the high-level way to write a UDP service. It
//               owns the receive loop, the send queue and (optionally) the
//               run() threads, so a service only supplies one callback.
// Usage:        spanio::udp_server server(9000);
//               auto on_datagram = [&server](const spanio::udp::endpoint& from,
//                                            std::string_view data)
//               {
//                   server.send_to(from, data);
//               };
//               server.on_datagram(on_datagram);
//               server.run();       // blocks until stop()
//
//               Guarantees:
//               - There is one receive chain, so on_datagram calls never
//                 overlap, whatever the thread count. The next datagram is
//                 read after the callback returns, so keep it short: while
//                 it runs, datagrams wait in the kernel, which drops them
//                 when its queue fills up. More run() threads do not help
//                 this; hand slow work to other threads instead.
//               - from and data are valid only during the call. An empty
//                 datagram is delivered like any other.
//               - send_to() may be called from any thread. Datagrams go out
//                 one at a time, and those from one thread keep their order.
//               - An exception from on_datagram is logged and the server
//                 keeps receiving, as it does after a receive error. Only
//                 stop() ends the receive chain.
//
//               Nothing here ends the service when a datagram fails; stats()
//               counts what happened instead:
//               - m_queue_dropped: send_to() refused it, the queue was full
//               - m_send_failed:   the system rejected it, e.g. too large
//               - m_truncated:     it was over m_max_datagram_size, and was
//                                  dropped without calling on_datagram
//
//               Rules:
//               - Set on_datagram before start() / run().
//               - A server starts once; after stop(), create a new one.
//               - stop() drops queued data; stop_gracefully(grace) stops
//                 receiving but sends what is queued first.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#ifndef SPANIO_UDP_SERVER_H
#define SPANIO_UDP_SERVER_H

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/timer.h"
#include "spanio/udp.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/run_threads.h"

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
#include <utility>
#include <vector>

namespace spanio {

// Largest UDP payload over IPv4: 65535 minus the IP and UDP headers
constexpr std::size_t kMaxUdpPayload = 65507;

// Default limit for output queued on a udp_server: 16 MiB
constexpr std::size_t kDefaultUdpSendQueueBytes = 16 * 1024 * 1024;

// Limits of a udp_server
struct udp_server_options
{
    // Size of the receive buffer; must not be 0. Larger datagrams are
    // dropped and counted in udp_server_stats::m_truncated.
    std::size_t m_max_datagram_size = kMaxUdpPayload;
    // Most bytes queued for sending; send_to() refuses more
    std::size_t m_max_send_queue_bytes = kDefaultUdpSendQueueBytes;
};

// What a udp_server has done so far
struct udp_server_stats
{
    // Datagrams given to on_datagram
    std::uint64_t m_received = 0;
    // Datagrams sent
    std::uint64_t m_sent = 0;
    // Datagrams send_to() refused because the queue was full
    std::uint64_t m_queue_dropped = 0;
    // Datagrams the system rejected when sending
    std::uint64_t m_send_failed = 0;
    // Received datagrams dropped for being over m_max_datagram_size
    std::uint64_t m_truncated = 0;
};

// Called for a received datagram: void(const udp::endpoint&, std::string_view)
using udp_datagram_callback = std::function<void(const udp::endpoint&, std::string_view)>;

}

namespace spanio { namespace detail {

// One datagram waiting to be sent
struct udp_send_item
{
    // Destination
    udp::endpoint m_peer;
    // Bytes to send; may be empty
    std::string m_data;
};

// State shared by a udp_server and its handlers, which keep it alive through
// a shared_ptr, so one that runs after the udp_server object is gone (on an
// external context) is still safe.
class udp_server_state : public std::enable_shared_from_this<udp_server_state>
{
public:
    // Open and bind; throws std::system_error on failure
    // Parameter context: context the server runs on
    // Parameter local:   address and port to bind
    // Parameter options: limits; std::invalid_argument if the datagram size
    //                    is 0
    udp_server_state(io_context& context, const udp::endpoint& local, const udp_server_options& options)
        : m_options(options)
        , m_socket(context, local)
        , m_read_buffer(options.m_max_datagram_size)
        , m_queued_bytes(0)
        , m_writing(false)
        , m_started(false)
        , m_stopped(false)
        , m_closed(false)
        , m_grace_timer(context)
        , m_received(0)
        , m_sent(0)
        , m_queue_dropped(0)
        , m_send_failed(0)
        , m_truncated(0)
    {
        if (options.m_max_datagram_size == 0)
        {
            throw std::invalid_argument("spanio::udp_server: datagram size must not be 0");
        }
    }

    // Start the receive chain, once. Throws std::logic_error if the server
    // was already started or stopped.
    void start()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_started || m_stopped)
        {
            throw std::logic_error("spanio::udp_server: a server starts only once");
        }
        m_started = true;
        receive_next_locked();
    }

    // Start the receive chain unless that already happened. Throws
    // std::logic_error if the server was stopped.
    void ensure_started()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            throw std::logic_error("spanio::udp_server: a stopped server cannot run again");
        }
        if (!m_started)
        {
            m_started = true;
            receive_next_locked();
        }
    }

    // Close the socket and drop queued data; also ends a graceful stop
    void stop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stopped = true;
        close_now_locked();
    }

    // Stop receiving and refuse new datagrams at once, but keep sending
    // those already queued, then close
    // Parameter grace: longest wait for the queue to empty; 0 waits for as
    //                  long as it takes
    void stop_gracefully(std::chrono::milliseconds grace)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            return;
        }
        m_stopped = true;
        if (!m_writing)
        {
            close_now_locked();
            return;
        }
        if (grace.count() > 0)
        {
            m_grace_timer.expires_after(grace);
            std::shared_ptr<udp_server_state> self = shared_from_this();
            auto grace_over = [self](std::error_code ec)
            {
                self->on_grace_over(ec);
            };
            m_grace_timer.async_wait(grace_over);
        }
    }

    // Throw unless the callback may still be changed
    void check_not_started() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_started)
        {
            throw std::logic_error("spanio::udp_server: set on_datagram before start()");
        }
    }

    // Queue a datagram
    // Parameter peer: destination
    // Parameter data: bytes to send, possibly empty
    // Return value: false if stopped or the queue is full
    bool enqueue(const udp::endpoint& peer, std::string data)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopped)
        {
            return false;
        }
        if (m_queued_bytes + data.size() > m_options.m_max_send_queue_bytes)
        {
            ++m_queue_dropped;
            return false;
        }
        m_queued_bytes += data.size();
        m_send_queue.push_back(udp_send_item{peer, std::move(data)});
        if (!m_writing)
        {
            m_writing = true;
            send_next_locked();
        }
        return true;
    }

    // Local address
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    udp::endpoint local_endpoint(std::error_code& ec) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_socket.local_endpoint(ec);
    }

    // Counters so far
    // Return value: a snapshot
    udp_server_stats stats() const noexcept
    {
        udp_server_stats snapshot;
        snapshot.m_received = m_received.load();
        snapshot.m_sent = m_sent.load();
        snapshot.m_queue_dropped = m_queue_dropped.load();
        snapshot.m_send_failed = m_send_failed.load();
        snapshot.m_truncated = m_truncated.load();
        return snapshot;
    }

    // Limits of the server; fixed after construction
    const udp_server_options m_options;
    // Called for received datagrams; fixed after start()
    udp_datagram_callback m_on_datagram;

private:
    // Start the next receive, unless stopped. Caller holds m_mutex.
    void receive_next_locked()
    {
        if (m_stopped)
        {
            return;
        }
        std::shared_ptr<udp_server_state> self = shared_from_this();
        auto receive_done = [self](std::error_code ec, std::size_t bytes, const udp::endpoint& from)
        {
            self->on_received(ec, bytes, from);
        };
        m_socket.async_receive_from(spanio::buffer(m_read_buffer.data(), m_read_buffer.size()), receive_done);
    }

    // Completion of a receive: deliver the datagram, then receive the next
    // Parameter ec:    receive result
    // Parameter bytes: bytes in m_read_buffer
    // Parameter from:  the sender
    void on_received(std::error_code ec, std::size_t bytes, const udp::endpoint& from)
    {
        if (ec == std::errc::operation_canceled)
        {
            return;
        }
        {
            // A graceful stop delivers nothing more, even a datagram that
            // was already on its way
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopped)
            {
                return;
            }
        }
        if (ec == error::datagram_truncated)
        {
            ++m_truncated;
        }
        else if (ec)
        {
            log_error("udp_server: receive", ec);
        }
        else
        {
            ++m_received;
            deliver(from, std::string_view(m_read_buffer.data(), bytes));
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        receive_next_locked();
    }

    // Call on_datagram, keeping an exception from ending the receive chain
    // Parameter from: the sender
    // Parameter data: the datagram
    void deliver(const udp::endpoint& from, std::string_view data)
    {
        if (!m_on_datagram)
        {
            return;
        }
        try
        {
            m_on_datagram(from, data);
        }
        catch (...)
        {
            log_error("udp_server: on_datagram threw an exception", make_error_code(error::handler_failed));
        }
    }

    // Start sending the front of the queue. Caller holds m_mutex.
    void send_next_locked()
    {
        const udp_send_item& front = m_send_queue.front();
        std::shared_ptr<udp_server_state> self = shared_from_this();
        auto send_done = [self](std::error_code ec, std::size_t)
        {
            self->on_sent(ec);
        };
        m_socket.async_send_to(spanio::buffer(front.m_data), front.m_peer, send_done);
    }

    // Completion of a send: count it and start the next one
    // Parameter ec: send result
    void on_sent(const std::error_code& ec)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_writing = false;
        if (m_closed)
        {
            m_send_queue.clear();
            return;
        }
        m_queued_bytes -= m_send_queue.front().m_data.size();
        m_send_queue.pop_front();
        if (ec)
        {
            ++m_send_failed;
            log_error("udp_server: send", ec);
        }
        else
        {
            ++m_sent;
        }
        if (!m_send_queue.empty())
        {
            m_writing = true;
            send_next_locked();
        }
        else if (m_stopped)
        {
            // A graceful stop was waiting for the queue to empty
            close_now_locked();
        }
    }

    // The grace period of a graceful stop ended with data still queued
    // Parameter ec: wait result; cancelled when the queue emptied in time
    void on_grace_over(const std::error_code& ec)
    {
        if (ec)
        {
            return;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_closed)
        {
            log_error("udp_server: graceful stop timed out", make_error_code(error::close_timeout));
            close_now_locked();
        }
    }

    // Close the socket, drop queued data and end the grace period. Caller
    // holds m_mutex.
    void close_now_locked()
    {
        if (m_closed)
        {
            return;
        }
        m_closed = true;
        if (m_writing)
        {
            // The kernel may still read the element in flight; it is freed
            // when its send comes back
            m_send_queue.erase(m_send_queue.begin() + 1, m_send_queue.end());
        }
        else
        {
            m_send_queue.clear();
        }
        m_socket.close();
        m_grace_timer.cancel();
    }

    // Guards the socket, the queue and the flags below it
    mutable std::mutex m_mutex;
    // The socket
    udp::socket m_socket;
    // Destination of the receives; only the receive chain touches it
    std::vector<char> m_read_buffer;
    // Datagrams not sent yet. The front one is in flight while m_writing, so
    // it stays in place until its send comes back.
    std::deque<udp_send_item> m_send_queue;
    // Bytes in m_send_queue, counted against the queue limit
    std::size_t m_queued_bytes;
    // A send is in flight
    bool m_writing;
    // start() was called
    bool m_started;
    // stop() or stop_gracefully() was called: no more receiving or new
    // datagrams
    bool m_stopped;
    // The socket is closed
    bool m_closed;
    // Ends the grace period of stop_gracefully()
    steady_timer m_grace_timer;
    // See udp_server_stats::m_received
    std::atomic<std::uint64_t> m_received;
    // See udp_server_stats::m_sent
    std::atomic<std::uint64_t> m_sent;
    // See udp_server_stats::m_queue_dropped
    std::atomic<std::uint64_t> m_queue_dropped;
    // See udp_server_stats::m_send_failed
    std::atomic<std::uint64_t> m_send_failed;
    // See udp_server_stats::m_truncated
    std::atomic<std::uint64_t> m_truncated;
};

}}

namespace spanio {

// A UDP service built from one callback. See the file header for the
// guarantees and rules.
class udp_server
{
public:
    // Bind 0.0.0.0 on a context owned by the server. Throws
    // std::system_error if the port cannot be bound.
    // Parameter port:    port to use; 0 picks a free one
    // Parameter options: limits
    explicit udp_server(std::uint16_t port, const udp_server_options& options = udp_server_options())
        : udp_server(udp::endpoint(ipv4::any(), port), options)
    {
    }

    // Bind an address on a context owned by the server. Throws
    // std::system_error if the address cannot be bound.
    // Parameter local:   address and port to bind
    // Parameter options: limits
    explicit udp_server(const udp::endpoint& local, const udp_server_options& options = udp_server_options())
        : m_owned_context(new io_context())
        , m_context(m_owned_context.get())
        , m_state(std::make_shared<detail::udp_server_state>(*m_context, local, options))
    {
    }

    // Bind an address on a context the caller runs. Use start(), not run().
    // Throws std::system_error if the address cannot be bound.
    // Parameter context: context to run on; must outlive the server
    // Parameter local:   address and port to bind
    // Parameter options: limits
    udp_server(io_context& context, const udp::endpoint& local,
               const udp_server_options& options = udp_server_options())
        : m_context(&context)
        , m_state(std::make_shared<detail::udp_server_state>(context, local, options))
    {
    }

    udp_server(const udp_server&) = delete;
    udp_server& operator=(const udp_server&) = delete;

    // Stop the server
    ~udp_server()
    {
        m_state->stop();
    }

    // Set the callback for received datagrams; before start() only
    // Parameter callback: void(const udp::endpoint& from, std::string_view data)
    template <typename Callback>
    void on_datagram(Callback&& callback)
    {
        m_state->check_not_started();
        m_state->m_on_datagram = std::forward<Callback>(callback);
    }

    // Queue a datagram for sending; safe from any thread
    // Parameter peer: destination
    // Parameter data: bytes to send, possibly empty; copied
    // Return value: false if the server is stopped or the queue is full
    bool send_to(const udp::endpoint& peer, std::string_view data)
    {
        return m_state->enqueue(peer, std::string(data));
    }

    // Queue a zero-terminated string for sending; safe from any thread
    // Parameter peer: destination
    // Parameter data: text to send, without its terminating zero
    // Return value: same as send_to(peer, std::string_view)
    bool send_to(const udp::endpoint& peer, const char* data)
    {
        return m_state->enqueue(peer, std::string(data));
    }

    // Queue a datagram without copying it; safe from any thread
    // Parameter peer: destination
    // Parameter data: bytes to send, possibly empty; moved into the queue
    // Return value: same as send_to(peer, std::string_view)
    bool send_to(const udp::endpoint& peer, std::string&& data)
    {
        return m_state->enqueue(peer, std::move(data));
    }

    // Start receiving, without blocking; the context must be run by someone.
    // Throws std::logic_error if already started or stopped.
    void start()
    {
        m_state->start();
    }

    // Start if needed, then run the owned context until stop(). Throws
    // std::logic_error on an external context or after stop(), and
    // std::invalid_argument for 0 threads.
    // Parameter threads: threads to run on, including the calling one. More
    //                    than one rarely helps; see the file header.
    void run(unsigned threads = 1)
    {
        if (m_owned_context == nullptr)
        {
            throw std::logic_error("spanio::udp_server: run() needs the owned context; run yours instead");
        }
        if (threads == 0)
        {
            throw std::invalid_argument("spanio::udp_server: run() needs at least one thread");
        }
        m_state->ensure_started();
        detail::run_on_threads(*m_owned_context, threads);
    }

    // Stop receiving and drop queued data. run() then returns. Safe from any
    // thread and from on_datagram.
    void stop()
    {
        m_state->stop();
    }

    // Stop receiving and refuse new datagrams at once, but keep sending the
    // queued ones; close when the queue is empty or the grace period ends,
    // and run() then returns. Does not block; safe from any thread and from
    // on_datagram. A later stop() ends the wait at once.
    // Parameter grace: longest wait; 0 waits for as long as it takes
    void stop_gracefully(std::chrono::milliseconds grace)
    {
        m_state->stop_gracefully(grace);
    }

    // Address the server is bound to, e.g. to learn the port picked for 0
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    udp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_state->local_endpoint(ec);
    }

    // What the server has done so far
    // Return value: a snapshot of the counters
    udp_server_stats stats() const noexcept
    {
        return m_state->stats();
    }

    // The context the server runs on
    // Return value: the owned or the external context
    io_context& context() noexcept
    {
        return *m_context;
    }

private:
    // Context owned by the server; null on an external context. Declared
    // first so it is destroyed last: it may still hold handlers.
    std::unique_ptr<io_context> m_owned_context;
    // The context in use, owned or external
    io_context* m_context;
    // Everything handlers share with the server
    std::shared_ptr<detail::udp_server_state> m_state;
};

}

#endif // SPANIO_UDP_SERVER_H
