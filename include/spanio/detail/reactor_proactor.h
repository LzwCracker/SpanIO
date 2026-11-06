// ============================================================================
// File purpose: io_proactor adapter over any io_reactor ("Reactor emulates
//               Proactor", design doc 5.2). The adapter performs the
//               non-blocking accept/connect/recv/send itself when the reactor
//               reports readiness, and hands the finished operation back
//               through poll(). It is the default on Linux (over epoll) and
//               the alternative on Windows (over IOCP + AFD poll).
//
//               Strategy:
//               - start() first tries the I/O at once (speculative attempt).
//                 Only if it would block is the operation parked on its
//                 socket and readiness requested from the reactor.
//               - A socket is added to the reactor only while it has a parked
//                 operation and removed when it has none. Level-triggered
//                 Error/Hangup events are always reported, so keeping idle
//                 sockets registered would make poll() spin (epoll, for
//                 example, reports EPOLLHUP for any unconnected socket).
// Usage:        Create through make_platform_proactor(), or wrap a reactor
//               explicitly: reactor_proactor::create(std::move(reactor), ec).
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_DETAIL_REACTOR_PROACTOR_H
#define SPANIO_DETAIL_REACTOR_PROACTOR_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/io_reactor.h"
#include "spanio/detail/socket_ops.h"

#if defined(SPANIO_PLATFORM_LINUX)
#include <sys/uio.h>
#endif

#include <climits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace spanio { namespace detail {

#if defined(SPANIO_PLATFORM_LINUX)
// send() flags: MSG_NOSIGNAL turns SIGPIPE on a closed peer into EPIPE
constexpr int kAdapterSendFlags = MSG_NOSIGNAL;
#else
// send() flags: Windows never raises SIGPIPE
constexpr int kAdapterSendFlags = 0;
#endif

#if defined(SPANIO_PLATFORM_LINUX)
// Buffers given to recvmsg() for one datagram
constexpr std::size_t kAdapterDatagramIovecs = 1;
#endif

// recv() flags: none
constexpr int kAdapterRecvFlags = 0;

// Largest transfer one recv()/send() call is asked for; Winsock takes an
// int length, so larger requests are clamped and complete partially
constexpr std::size_t kAdapterMaxTransfer = static_cast<std::size_t>(INT_MAX);

// Operations parked on one socket while it waits for readiness
struct reactor_socket_ops
{
    // Parked accept or recv, or null
    io_operation* m_read_op;
    // Parked connect or send, or null
    io_operation* m_write_op;
    // Interest currently registered with the reactor
    event_mask m_interest;
    // Whether the socket is currently added to the reactor
    bool m_in_reactor;
};

// io_proactor that emulates completions on top of an io_reactor
class reactor_proactor final : public io_proactor
{
public:
    // Wrap a reactor
    // Parameter reactor: backend to wait on; ownership moves to the adapter
    // Parameter ec:      set to errc::invalid_argument if reactor is null,
    //                    cleared otherwise
    // Return value: the new proactor, or nullptr on failure
    static std::unique_ptr<reactor_proactor> create(io_reactor_ptr reactor, std::error_code& ec)
    {
        if (reactor == nullptr)
        {
            ec = std::make_error_code(std::errc::invalid_argument);
            return nullptr;
        }
        ec.clear();
        return std::unique_ptr<reactor_proactor>(new reactor_proactor(std::move(reactor)));
    }

    reactor_proactor(const reactor_proactor&) = delete;
    reactor_proactor& operator=(const reactor_proactor&) = delete;

    // The wrapped reactor, e.g. to read its name in logs and tests
    // Return value: reference valid for the adapter's lifetime
    const io_reactor& reactor() const noexcept
    {
        return *m_reactor;
    }

    // See io_proactor::register_handle. Also switches the socket to
    // non-blocking mode, which the emulation relies on.
    std::error_code register_handle(native_handle_t handle) override
    {
        const std::error_code ec = set_nonblocking(handle, true);
        if (ec)
        {
            return ec;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_sockets.emplace(handle, reactor_socket_ops{}).second)
        {
            return std::make_error_code(std::errc::file_exists);
        }
        return std::error_code();
    }

    // See io_proactor::unregister_handle
    std::error_code unregister_handle(native_handle_t handle) override
    {
        bool completed = false;
        std::error_code ec;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_sockets.find(handle);
            if (it == m_sockets.end())
            {
                return std::make_error_code(std::errc::no_such_file_or_directory);
            }
            completed = cancel_parked(it->second);
            if (it->second.m_in_reactor)
            {
                ec = m_reactor->del(handle);
            }
            m_sockets.erase(it);
        }
        if (completed)
        {
            m_reactor->interrupt();
        }
        return ec;
    }

    // See io_proactor::start
    void start(io_operation* op) override
    {
        if (op->m_kind == op_kind::kPost)
        {
            post(op);
            return;
        }

        bool completed = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_sockets.find(op->m_handle);
            if (it == m_sockets.end())
            {
                op->m_ec = std::make_error_code(std::errc::bad_file_descriptor);
                log_error("reactor_proactor::start(unregistered socket)", op->m_ec);
                m_ready.push_back(op);
                completed = true;
            }
            else if (initiate(*op))
            {
                // Finished at once; delivered by the next poll()
                m_ready.push_back(op);
                completed = true;
            }
            else
            {
                reactor_socket_ops& ops = it->second;
                if (is_read_side(op->m_kind))
                {
                    ops.m_read_op = op;
                }
                else
                {
                    ops.m_write_op = op;
                }
                completed = update_interest(op->m_handle, ops);
            }
        }
        // poll() may be blocked in another thread with nothing to wake it
        if (completed)
        {
            m_reactor->interrupt();
        }
    }

    // See io_proactor::cancel
    void cancel(native_handle_t handle) override
    {
        bool completed = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_sockets.find(handle);
            if (it == m_sockets.end())
            {
                return;
            }
            completed = cancel_parked(it->second);
            update_interest(handle, it->second);
        }
        if (completed)
        {
            m_reactor->interrupt();
        }
    }

    // See io_proactor::post
    void post(io_operation* op) override
    {
        op->m_ec.clear();
        op->m_bytes = 0;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_ready.push_back(op);
        }
        m_reactor->interrupt();
    }

    // See io_proactor::poll
    std::size_t poll(completion_list& out, int timeout_ms, std::error_code& ec) override
    {
        out.clear();
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            take_ready(out);
        }

        // Something is already finished: only collect readiness that is
        // available right now instead of blocking
        const int wait_ms = out.empty() ? timeout_ms : 0;
        m_reactor->poll(m_events, wait_ms, ec);
        if (ec)
        {
            // Operations already taken must still be returned
            return out.size();
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        for (const ready_event& event : m_events)
        {
            dispatch(event, out);
        }
        // Operations finished by other threads while the reactor waited
        take_ready(out);
        return out.size();
    }

    // See io_proactor::interrupt
    void interrupt() noexcept override
    {
        m_reactor->interrupt();
    }

    // See io_proactor::name
    const char* name() const noexcept override
    {
        return "reactor-proactor";
    }

private:
    // Take ownership of a reactor; use create() instead
    // Parameter reactor: non-null backend
    explicit reactor_proactor(io_reactor_ptr reactor) noexcept
        : m_reactor(std::move(reactor))
    {
    }

    // Whether an error only means "try again when the socket is ready"
    // Parameter ec: error from a non-blocking call
    // Return value: true for EAGAIN / EWOULDBLOCK / WSAEWOULDBLOCK / EINTR
    static bool is_retry(const std::error_code& ec) noexcept
    {
#if defined(SPANIO_PLATFORM_LINUX)
        if (ec.value() == EINTR)
        {
            return true;
        }
#endif
        return ec == std::errc::operation_would_block
            || ec == std::errc::resource_unavailable_try_again;
    }

    // Clamp a transfer size to what one recv()/send() call accepts
    // Parameter size: requested size
    // Return value: size, at most kAdapterMaxTransfer
    static std::size_t clamp_transfer(std::size_t size) noexcept
    {
        return size < kAdapterMaxTransfer ? size : kAdapterMaxTransfer;
    }

    // Accept one pending connection without blocking
    // Parameter listener: non-blocking listening socket
    // Return value: the new socket, or kInvalidHandle (see last_error())
    static native_handle_t accept_one(native_handle_t listener) noexcept
    {
#if defined(SPANIO_PLATFORM_LINUX)
        // SOCK_CLOEXEC, as in create_socket(), keeps exec() from leaking it
        return ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
#else
        return ::accept(listener, nullptr, nullptr);
#endif
    }

    // Run the first step of an operation. For connect this issues the
    // non-blocking ::connect; for the others it is a first I/O attempt.
    // Parameter op: operation being started
    // Return value: true if the operation already has its final result
    bool initiate(io_operation& op)
    {
        if (op.m_kind != op_kind::kConnect)
        {
            return perform(op);
        }

        if (::connect(op.m_handle, reinterpret_cast<const sockaddr*>(&op.m_address),
                      op.m_address_len) == 0)
        {
            op.m_ec.clear();
            return true;
        }
        const std::error_code ec = last_error();
        // Winsock reports a pending connect as WSAEWOULDBLOCK, POSIX as
        // EINPROGRESS (or EINTR, after which the connect still proceeds)
        if (ec == std::errc::operation_in_progress || is_retry(ec))
        {
            return false;
        }
        op.m_ec = normalize_error(ec);
        log_error("reactor_proactor connect", op.m_ec);
        return true;
    }

    // Receive one datagram, reporting truncation as errc::message_size on
    // every platform. Windows fails the call with WSAEMSGSIZE. POSIX
    // recvfrom() hides truncation, which is why Linux uses recvmsg(): its
    // MSG_TRUNC flag reveals it.
    // Parameter op:     prepared kRecvFrom operation
    // Parameter result: receives the system call's result when the outcome
    //                   is decided by the common path in perform()
    // Return value: true if the datagram was truncated and op is finished
    bool receive_datagram(io_operation& op, long long& result)
    {
#if defined(SPANIO_PLATFORM_WINDOWS)
        socklen_type length = static_cast<socklen_type>(sizeof(op.m_address));
        const int size = static_cast<int>(clamp_transfer(op.m_size));
        result = ::recvfrom(op.m_handle, op.m_buffer, size, kAdapterRecvFlags,
                            reinterpret_cast<sockaddr*>(&op.m_address), &length);
        op.m_address_len = length;
        if (result == kSocketCallFailed && ::WSAGetLastError() == WSAEMSGSIZE)
        {
            op.m_bytes = static_cast<std::size_t>(size);
            op.m_ec = std::make_error_code(std::errc::message_size);
            return true;
        }
        return false;
#else
        iovec buffer{};
        buffer.iov_base = op.m_buffer;
        buffer.iov_len = clamp_transfer(op.m_size);
        msghdr message{};
        message.msg_name = &op.m_address;
        message.msg_namelen = static_cast<socklen_type>(sizeof(op.m_address));
        message.msg_iov = &buffer;
        message.msg_iovlen = kAdapterDatagramIovecs;
        result = ::recvmsg(op.m_handle, &message, kAdapterRecvFlags);
        op.m_address_len = message.msg_namelen;
        if (result != kSocketCallFailed && (message.msg_flags & MSG_TRUNC) != 0)
        {
            op.m_bytes = static_cast<std::size_t>(result);
            op.m_ec = std::make_error_code(std::errc::message_size);
            return true;
        }
        return false;
#endif
    }

    // Attempt the I/O of an operation once, without blocking
    // Parameter op: operation to advance
    // Return value: true if the operation now has its final result, false
    //               if the socket was not ready
    bool perform(io_operation& op)
    {
        long long result = kSocketCallFailed;
        switch (op.m_kind)
        {
        case op_kind::kAccept:
        {
            const native_handle_t accepted = accept_one(op.m_handle);
            if (accepted != kInvalidHandle)
            {
                op.m_accepted = accepted;
                op.m_ec.clear();
                return true;
            }
            break;
        }
        case op_kind::kRecv:
#if defined(SPANIO_PLATFORM_WINDOWS)
            result = ::recv(op.m_handle, op.m_buffer, static_cast<int>(clamp_transfer(op.m_size)),
                            kAdapterRecvFlags);
#else
            result = ::recv(op.m_handle, op.m_buffer, clamp_transfer(op.m_size), kAdapterRecvFlags);
#endif
            break;
        case op_kind::kSend:
#if defined(SPANIO_PLATFORM_WINDOWS)
            result = ::send(op.m_handle, op.m_buffer, static_cast<int>(clamp_transfer(op.m_size)),
                            kAdapterSendFlags);
#else
            result = ::send(op.m_handle, op.m_buffer, clamp_transfer(op.m_size), kAdapterSendFlags);
#endif
            break;
        case op_kind::kRecvFrom:
            if (receive_datagram(op, result))
            {
                return true;
            }
            break;
        case op_kind::kSendTo:
#if defined(SPANIO_PLATFORM_WINDOWS)
            result = ::sendto(op.m_handle, op.m_buffer, static_cast<int>(clamp_transfer(op.m_size)),
                              kAdapterSendFlags, reinterpret_cast<const sockaddr*>(&op.m_address),
                              op.m_address_len);
#else
            result = ::sendto(op.m_handle, op.m_buffer, clamp_transfer(op.m_size), kAdapterSendFlags,
                              reinterpret_cast<const sockaddr*>(&op.m_address), op.m_address_len);
#endif
            break;
        case op_kind::kConnect:
        {
            // The reactor said the connect finished; SO_ERROR says how
            int error = 0;
            socklen_type length = static_cast<socklen_type>(sizeof(error));
            if (::getsockopt(op.m_handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error),
                             &length) == kSocketCallFailed)
            {
                op.m_ec = last_error();
            }
            else
            {
                op.m_ec = std::error_code(error, std::system_category());
            }
            if (op.m_ec)
            {
                log_error("reactor_proactor connect", op.m_ec);
            }
            return true;
        }
        case op_kind::kPost:
        default:
            op.m_ec.clear();
            return true;
        }

        if (result != kSocketCallFailed)
        {
            op.m_bytes = static_cast<std::size_t>(result);
            op.m_ec.clear();
            return true;
        }
        const std::error_code ec = last_error();
        if (is_retry(ec))
        {
            return false;
        }
        op.m_ec = normalize_error(ec);
        log_error("reactor_proactor I/O", op.m_ec);
        return true;
    }

    // Advance the parked operations of a socket the reactor reported.
    // Caller holds m_mutex.
    // Parameter event: readiness reported for one socket
    // Parameter out:   receives operations that finished
    void dispatch(const ready_event& event, completion_list& out)
    {
        const auto it = m_sockets.find(event.m_handle);
        if (it == m_sockets.end())
        {
            // Unregistered while the reactor was waiting
            return;
        }
        reactor_socket_ops& ops = it->second;
        // Errors and hang-ups wake both sides; the I/O call then reports
        // the actual error (or EOF) for each operation
        if (ops.m_read_op != nullptr && (event.m_events & (kEventRead | kAlwaysReported)) != 0
            && perform(*ops.m_read_op))
        {
            out.push_back(ops.m_read_op);
            ops.m_read_op = nullptr;
        }
        if (ops.m_write_op != nullptr && (event.m_events & (kEventWrite | kAlwaysReported)) != 0
            && perform(*ops.m_write_op))
        {
            out.push_back(ops.m_write_op);
            ops.m_write_op = nullptr;
        }
        update_interest(event.m_handle, ops);
    }

    // Make the reactor registration match the parked operations: add the
    // socket when it starts waiting, change the interest when it changes,
    // remove it when nothing waits any more. Caller holds m_mutex.
    // Parameter handle: the socket
    // Parameter ops:    its parked operations
    // Return value: true if a reactor failure completed parked operations
    bool update_interest(native_handle_t handle, reactor_socket_ops& ops)
    {
        event_mask wanted = kEventNone;
        if (ops.m_read_op != nullptr)
        {
            wanted |= kEventRead;
        }
        if (ops.m_write_op != nullptr)
        {
            wanted |= kEventWrite;
        }

        std::error_code ec;
        if (wanted == kEventNone)
        {
            if (ops.m_in_reactor)
            {
                ec = m_reactor->del(handle);
                ops.m_in_reactor = false;
                ops.m_interest = kEventNone;
            }
            // Nothing is parked, so a failed del() has nobody to report to
            return false;
        }
        if (!ops.m_in_reactor)
        {
            ec = m_reactor->add(handle, wanted);
            if (!ec)
            {
                ops.m_in_reactor = true;
                ops.m_interest = wanted;
            }
        }
        else if (wanted != ops.m_interest)
        {
            ec = m_reactor->mod(handle, wanted);
            if (!ec)
            {
                ops.m_interest = wanted;
            }
        }
        if (!ec)
        {
            return false;
        }

        // The socket cannot be watched: fail what is parked on it
        log_error("reactor_proactor::update_interest", ec);
        complete_parked(ops, ec);
        return true;
    }

    // Finish every parked operation of a socket with one error and queue it
    // for delivery. Caller holds m_mutex.
    // Parameter ops: the socket's parked operations
    // Parameter ec:  result to give them
    // Return value: true if at least one operation was parked
    bool complete_parked(reactor_socket_ops& ops, const std::error_code& ec)
    {
        bool any = false;
        io_operation* const parked[] = {ops.m_read_op, ops.m_write_op};
        for (io_operation* op : parked)
        {
            if (op != nullptr)
            {
                op->m_ec = ec;
                op->m_bytes = 0;
                m_ready.push_back(op);
                any = true;
            }
        }
        ops.m_read_op = nullptr;
        ops.m_write_op = nullptr;
        return any;
    }

    // Cancel every parked operation of a socket. Caller holds m_mutex.
    // Parameter ops: the socket's parked operations
    // Return value: true if at least one operation was cancelled
    bool cancel_parked(reactor_socket_ops& ops)
    {
        return complete_parked(ops, std::make_error_code(std::errc::operation_canceled));
    }

    // Move queued finished operations into a poll() result. Caller holds
    // m_mutex.
    // Parameter out: receives the operations
    void take_ready(completion_list& out)
    {
        out.insert(out.end(), m_ready.begin(), m_ready.end());
        m_ready.clear();
    }

    // Readiness backend the adapter waits on
    io_reactor_ptr m_reactor;
    // Guards m_sockets and m_ready
    std::mutex m_mutex;
    // Registered sockets and their parked operations
    std::unordered_map<native_handle_t, reactor_socket_ops> m_sockets;
    // Finished operations waiting to be returned by poll()
    std::vector<io_operation*> m_ready;
    // Reused output buffer of the reactor; only touched by the polling thread
    event_table m_events;
};

}}

#endif // SPANIO_DETAIL_REACTOR_PROACTOR_H
