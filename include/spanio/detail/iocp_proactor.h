// ============================================================================
// File purpose: Native Windows io_proactor. Operations go straight to
//               Winsock overlapped I/O (AcceptEx, ConnectEx, WSARecv,
//               WSASend) and their results come back through one I/O
//               completion port, so no readiness round trip is needed.
//               Uses only documented Windows APIs.
// Usage:        Create through make_platform_proactor() (the Windows default)
//               or iocp_proactor::create(ec). Sockets must be created with
//               create_socket(), which sets WSA_FLAG_OVERLAPPED. Compiled only
//               on Windows; on other platforms this header is empty.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_DETAIL_IOCP_PROACTOR_H
#define SPANIO_DETAIL_IOCP_PROACTOR_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/socket_ops.h"

#if defined(SPANIO_PLATFORM_WINDOWS)

#include <atomic>
#include <limits>
#include <memory>
#include <mutex>

namespace spanio { namespace detail {

// Completion key of packets produced by Winsock I/O on registered sockets
constexpr ULONG_PTR kProactorSocketKey = 0;

// Completion key of packets queued by post()
constexpr ULONG_PTR kProactorPostKey = 1;

// Completion key of operations that failed while being started; their
// m_ec is already set
constexpr ULONG_PTR kProactorFailedStartKey = 2;

// Completion key of the wake-up packet queued by interrupt()
constexpr ULONG_PTR kProactorWakeupKey = 3;

// NTSTATUS left in OVERLAPPED::Internal by a request that was cancelled
// (STATUS_CANCELLED; ntstatus.h is not included by windows.h)
constexpr ULONG_PTR kProactorStatusCancelled = 0xC0000120;

// Concurrency value for CreateIoCompletionPort: 0 means "number of CPUs"
constexpr DWORD kProactorDefaultConcurrency = 0;

// Most completion packets taken from the port by one poll() call
constexpr ULONG kProactorMaxCompletions = 128;

// Number of WSABUF entries passed to WSARecv / WSASend
constexpr DWORD kProactorBufferCount = 1;

// Largest transfer one WSARecv / WSASend accepts; bigger requests are
// clamped and complete as partial transfers
constexpr std::size_t kProactorMaxTransfer = (std::numeric_limits<ULONG>::max)();

// io_proactor backed directly by Winsock overlapped I/O and an IOCP
class iocp_proactor final : public io_proactor
{
public:
    // Create the completion port
    // Parameter ec: set to the failure reason, or cleared on success
    // Return value: the new proactor, or nullptr on failure
    static std::unique_ptr<iocp_proactor> create(std::error_code& ec)
    {
        ec = ensure_network_initialized();
        if (ec)
        {
            return nullptr;
        }
        const HANDLE iocp = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0,
                                                     kProactorDefaultConcurrency);
        if (iocp == nullptr)
        {
            ec = win32_error("CreateIoCompletionPort");
            return nullptr;
        }
        ec.clear();
        return std::unique_ptr<iocp_proactor>(new iocp_proactor(iocp));
    }

    // Close the completion port. Every operation must have been returned by
    // poll() before this point (see the io_proactor contract).
    ~iocp_proactor() override
    {
        ::CloseHandle(m_iocp);
    }

    iocp_proactor(const iocp_proactor&) = delete;
    iocp_proactor& operator=(const iocp_proactor&) = delete;

    // See io_proactor::register_handle. Binds the socket to the completion
    // port; Windows keeps that binding until the socket is closed.
    std::error_code register_handle(native_handle_t handle) override
    {
        if (::CreateIoCompletionPort(reinterpret_cast<HANDLE>(handle), m_iocp,
                                     kProactorSocketKey, kProactorDefaultConcurrency) == nullptr)
        {
            return win32_error("iocp_proactor::register_handle");
        }
        // Nobody waits on the socket handle itself, so skip signalling it
        if (!::SetFileCompletionNotificationModes(reinterpret_cast<HANDLE>(handle),
                                                  FILE_SKIP_SET_EVENT_ON_HANDLE))
        {
            return win32_error("SetFileCompletionNotificationModes");
        }
        return std::error_code();
    }

    // See io_proactor::unregister_handle. A completion port binding cannot
    // be removed on Windows; it ends when the socket is closed. Outstanding
    // operations are cancelled to honour the contract.
    std::error_code unregister_handle(native_handle_t handle) override
    {
        cancel(handle);
        return std::error_code();
    }

    // See io_proactor::start
    void start(io_operation* op) override
    {
        switch (op->m_kind)
        {
        case op_kind::kAccept:
            start_accept(*op);
            break;
        case op_kind::kConnect:
            start_connect(*op);
            break;
        case op_kind::kRecv:
            start_recv(*op);
            break;
        case op_kind::kSend:
            start_send(*op);
            break;
        case op_kind::kPost:
        default:
            post(op);
            break;
        }
    }

    // See io_proactor::cancel
    void cancel(native_handle_t handle) override
    {
        // Cancels every overlapped request on the socket issued by this
        // process; ERROR_NOT_FOUND only means nothing was pending
        if (!::CancelIoEx(reinterpret_cast<HANDLE>(handle), nullptr))
        {
            const DWORD error = ::GetLastError();
            if (error != ERROR_NOT_FOUND)
            {
                log_error("CancelIoEx", std::error_code(static_cast<int>(error), std::system_category()));
            }
        }
    }

    // See io_proactor::post
    void post(io_operation* op) override
    {
        op->m_ec.clear();
        op->m_bytes = 0;
        if (!::PostQueuedCompletionStatus(m_iocp, 0, kProactorPostKey, &op->m_overlapped))
        {
            // The operation cannot be delivered; there is no way to report
            // this through poll(), so make it loud
            log_error("PostQueuedCompletionStatus(post)",
                      std::error_code(static_cast<int>(::GetLastError()), std::system_category()));
        }
    }

    // See io_proactor::poll
    std::size_t poll(completion_list& out, int timeout_ms, std::error_code& ec) override
    {
        out.clear();
        const DWORD wait_ms = (timeout_ms < 0) ? INFINITE : static_cast<DWORD>(timeout_ms);
        OVERLAPPED_ENTRY entries[kProactorMaxCompletions];
        ULONG count = 0;
        if (!::GetQueuedCompletionStatusEx(m_iocp, entries, kProactorMaxCompletions, &count,
                                           wait_ms, FALSE))
        {
            const DWORD error = ::GetLastError();
            if (error == WAIT_TIMEOUT)
            {
                ec.clear();
                return 0;
            }
            ec = std::error_code(static_cast<int>(error), std::system_category());
            log_error("GetQueuedCompletionStatusEx", ec);
            return 0;
        }

        for (ULONG i = 0; i < count; ++i)
        {
            const OVERLAPPED_ENTRY& entry = entries[i];
            if (entry.lpCompletionKey == kProactorWakeupKey)
            {
                // Allow the next interrupt() to post a new packet
                m_interrupt_pending.store(false);
                continue;
            }

            // m_overlapped is the first member of the standard-layout
            // io_operation, so the two pointers are interconvertible
            io_operation* const op = reinterpret_cast<io_operation*>(entry.lpOverlapped);
            if (entry.lpCompletionKey == kProactorSocketKey)
            {
                finish_io(*op, entry.dwNumberOfBytesTransferred);
            }
            // kProactorPostKey and kProactorFailedStartKey already carry
            // their final result
            out.push_back(op);
        }

        ec.clear();
        return out.size();
    }

    // See io_proactor::interrupt
    void interrupt() noexcept override
    {
        // Only one wake-up packet may be outstanding at a time
        if (!m_interrupt_pending.exchange(true))
        {
            if (!::PostQueuedCompletionStatus(m_iocp, 0, kProactorWakeupKey, nullptr))
            {
                m_interrupt_pending.store(false);
                log_error("PostQueuedCompletionStatus(wakeup)",
                          std::error_code(static_cast<int>(::GetLastError()), std::system_category()));
            }
        }
    }

    // See io_proactor::name
    const char* name() const noexcept override
    {
        return "iocp-proactor";
    }

private:
    // Take ownership of a completion port; use create() instead
    // Parameter iocp: completion port
    explicit iocp_proactor(HANDLE iocp) noexcept
        : m_iocp(iocp)
        , m_interrupt_pending(false)
        , m_accept_ex(nullptr)
        , m_connect_ex(nullptr)
    {
    }

    // Wrap GetLastError() and log it
    // Parameter what: name of the call that failed
    // Return value: the Win32 error as a std::error_code
    static std::error_code win32_error(const char* what) noexcept
    {
        const std::error_code ec(static_cast<int>(::GetLastError()), std::system_category());
        log_error(what, ec);
        return ec;
    }

    // Clamp a buffer size to what one overlapped call can transfer
    // Parameter size: requested size
    // Return value: size as a ULONG, at most kProactorMaxTransfer
    static ULONG clamp_transfer(std::size_t size) noexcept
    {
        return static_cast<ULONG>(size < kProactorMaxTransfer ? size : kProactorMaxTransfer);
    }

    // Report an operation that failed while being started. Winsock posts no
    // packet in that case, so one is queued by hand to keep the "returned
    // exactly once by poll()" promise.
    // Parameter op: operation that failed
    // Parameter ec: reason
    void fail_start(io_operation& op, const std::error_code& ec)
    {
        op.m_ec = normalize_error(ec);
        op.m_bytes = 0;
        log_error("iocp_proactor::start", op.m_ec);
        if (!::PostQueuedCompletionStatus(m_iocp, 0, kProactorFailedStartKey, &op.m_overlapped))
        {
            log_error("PostQueuedCompletionStatus(failed start)",
                      std::error_code(static_cast<int>(::GetLastError()), std::system_category()));
        }
    }

    // Look up a Winsock extension function (AcceptEx, ConnectEx) once and
    // cache it. The pointer depends on the transport provider; SpanIO only
    // uses TCP, whose provider is the same for every socket.
    // Parameter socket: any TCP socket
    // Parameter guid:   WSAID_* of the function
    // Parameter cache:  member that holds the cached pointer
    // Parameter ec:     set to the failure reason, or cleared
    // Return value: the function pointer, or nullptr on failure
    template <typename Fn>
    Fn load_extension(native_handle_t socket, GUID guid, Fn& cache, std::error_code& ec)
    {
        std::lock_guard<std::mutex> lock(m_extension_mutex);
        if (cache == nullptr)
        {
            DWORD bytes = 0;
            if (::WSAIoctl(socket, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid,
                           static_cast<DWORD>(sizeof(guid)), &cache,
                           static_cast<DWORD>(sizeof(cache)), &bytes, nullptr, nullptr)
                == SOCKET_ERROR)
            {
                ec = last_error();
                cache = nullptr;
                return nullptr;
            }
        }
        ec.clear();
        return cache;
    }

    // Start AcceptEx: the accepted socket is created up front, with the
    // listener's address family
    // Parameter op: prepared accept operation
    void start_accept(io_operation& op)
    {
        sockaddr_storage local{};
        socklen_type length = static_cast<socklen_type>(sizeof(local));
        if (::getsockname(op.m_handle, reinterpret_cast<sockaddr*>(&local), &length) == SOCKET_ERROR)
        {
            fail_start(op, last_error());
            return;
        }

        std::error_code ec;
        // WSAID_ACCEPTEX is a brace initializer, so it needs a named object
        const GUID accept_ex_id = WSAID_ACCEPTEX;
        const LPFN_ACCEPTEX accept_ex = load_extension(op.m_handle, accept_ex_id, m_accept_ex, ec);
        if (accept_ex == nullptr)
        {
            fail_start(op, ec);
            return;
        }

        op.m_accepted = create_socket(local.ss_family, SOCK_STREAM, IPPROTO_TCP, ec);
        if (ec)
        {
            fail_start(op, ec);
            return;
        }

        DWORD bytes = 0;
        if (!accept_ex(op.m_handle, op.m_accepted, op.m_accept_buffer, 0,
                       static_cast<DWORD>(kAcceptExAddressLength),
                       static_cast<DWORD>(kAcceptExAddressLength), &bytes, &op.m_overlapped))
        {
            const int error = ::WSAGetLastError();
            if (error != ERROR_IO_PENDING)
            {
                close_socket(op.m_accepted);
                op.m_accepted = kInvalidHandle;
                fail_start(op, std::error_code(error, std::system_category()));
            }
        }
    }

    // ConnectEx refuses unbound sockets: bind to the wildcard address of the
    // destination's family unless the caller already bound the socket
    // Parameter op: prepared connect operation
    // Return value: empty on success, otherwise the bind error
    static std::error_code ensure_bound(const io_operation& op) noexcept
    {
        sockaddr_storage local{};
        socklen_type length = static_cast<socklen_type>(sizeof(local));
        if (::getsockname(op.m_handle, reinterpret_cast<sockaddr*>(&local), &length) != SOCKET_ERROR)
        {
            return std::error_code();
        }

        // A zeroed sockaddr_in / sockaddr_in6 is the wildcard address, port 0
        sockaddr_storage wildcard{};
        wildcard.ss_family = op.m_address.ss_family;
        const socklen_type wildcard_length = static_cast<socklen_type>(
            op.m_address.ss_family == AF_INET6 ? sizeof(sockaddr_in6) : sizeof(sockaddr_in));
        if (::bind(op.m_handle, reinterpret_cast<const sockaddr*>(&wildcard), wildcard_length)
            == SOCKET_ERROR)
        {
            return last_error();
        }
        return std::error_code();
    }

    // Start ConnectEx
    // Parameter op: prepared connect operation
    void start_connect(io_operation& op)
    {
        std::error_code ec = ensure_bound(op);
        if (ec)
        {
            fail_start(op, ec);
            return;
        }
        // WSAID_CONNECTEX is a brace initializer, so it needs a named object
        const GUID connect_ex_id = WSAID_CONNECTEX;
        const LPFN_CONNECTEX connect_ex = load_extension(op.m_handle, connect_ex_id, m_connect_ex, ec);
        if (connect_ex == nullptr)
        {
            fail_start(op, ec);
            return;
        }

        if (!connect_ex(op.m_handle, reinterpret_cast<const sockaddr*>(&op.m_address),
                        op.m_address_len, nullptr, 0, nullptr, &op.m_overlapped))
        {
            const int error = ::WSAGetLastError();
            if (error != ERROR_IO_PENDING)
            {
                fail_start(op, std::error_code(error, std::system_category()));
            }
        }
    }

    // Start WSARecv
    // Parameter op: prepared receive operation
    void start_recv(io_operation& op)
    {
        WSABUF buffer;
        buffer.buf = op.m_buffer;
        buffer.len = clamp_transfer(op.m_size);
        DWORD flags = 0;
        // Even an immediate success posts a packet, because the socket does
        // not use FILE_SKIP_COMPLETION_PORT_ON_SUCCESS
        if (::WSARecv(op.m_handle, &buffer, kProactorBufferCount, nullptr, &flags,
                      &op.m_overlapped, nullptr) == SOCKET_ERROR)
        {
            const int error = ::WSAGetLastError();
            if (error != WSA_IO_PENDING)
            {
                fail_start(op, std::error_code(error, std::system_category()));
            }
        }
    }

    // Start WSASend
    // Parameter op: prepared send operation
    void start_send(io_operation& op)
    {
        WSABUF buffer;
        buffer.buf = op.m_buffer;
        buffer.len = clamp_transfer(op.m_size);
        if (::WSASend(op.m_handle, &buffer, kProactorBufferCount, nullptr, 0,
                      &op.m_overlapped, nullptr) == SOCKET_ERROR)
        {
            const int error = ::WSAGetLastError();
            if (error != WSA_IO_PENDING)
            {
                fail_start(op, std::error_code(error, std::system_category()));
            }
        }
    }

    // Fill in the result of an operation whose Winsock request completed
    // Parameter op:    the operation
    // Parameter bytes: byte count from the completion packet
    void finish_io(io_operation& op, DWORD bytes)
    {
        op.m_ec.clear();
        op.m_bytes = bytes;

        // Internal holds the NTSTATUS of the request. A cancelled request is
        // recognised directly: the usual sequence is unregister_handle()
        // (CancelIoEx) followed at once by closesocket(), so by the time the
        // packet is processed the handle may be closed and
        // WSAGetOverlappedResult would fail with WSAENOTSOCK instead.
        if (op.m_overlapped.Internal == kProactorStatusCancelled)
        {
            op.m_ec = std::make_error_code(std::errc::operation_canceled);
        }
        // Any other failure: WSAGetOverlappedResult turns it into the
        // matching Winsock error (e.g. WSAECONNRESET instead of
        // ERROR_NETNAME_DELETED), so codes match the reactor path
        else if (op.m_overlapped.Internal != 0)
        {
            DWORD transferred = 0;
            DWORD flags = 0;
            if (!::WSAGetOverlappedResult(op.m_handle, &op.m_overlapped, &transferred, FALSE, &flags))
            {
                op.m_ec = normalize_error(last_error());
            }
        }

        if (op.m_kind == op_kind::kAccept)
        {
            op.m_bytes = 0;
            if (!op.m_ec)
            {
                // Lets getsockname/getpeername/shutdown work on the new socket
                if (::setsockopt(op.m_accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                                 reinterpret_cast<const char*>(&op.m_handle),
                                 static_cast<int>(sizeof(op.m_handle))) == SOCKET_ERROR)
                {
                    op.m_ec = last_error();
                }
            }
            if (op.m_ec)
            {
                close_socket(op.m_accepted);
                op.m_accepted = kInvalidHandle;
            }
        }
        else if (op.m_kind == op_kind::kConnect)
        {
            op.m_bytes = 0;
            if (!op.m_ec
                && ::setsockopt(op.m_handle, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0)
                       == SOCKET_ERROR)
            {
                op.m_ec = last_error();
            }
        }

        if (op.m_ec && op.m_ec != std::errc::operation_canceled)
        {
            log_error("iocp_proactor operation", op.m_ec);
        }
    }

    // Completion port that every registered socket is bound to
    HANDLE m_iocp;
    // True while a wake-up packet posted by interrupt() is in the port
    std::atomic<bool> m_interrupt_pending;
    // Guards the lazily loaded extension function pointers below
    std::mutex m_extension_mutex;
    // Cached AcceptEx pointer; null until first needed
    LPFN_ACCEPTEX m_accept_ex;
    // Cached ConnectEx pointer; null until first needed
    LPFN_CONNECTEX m_connect_ex;
};

}}

#endif // SPANIO_PLATFORM_WINDOWS

#endif // SPANIO_DETAIL_IOCP_PROACTOR_H
