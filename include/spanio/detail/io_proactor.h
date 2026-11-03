// ============================================================================
// File purpose: Platform-neutral completion interface (Proactor). A caller
//               starts an asynchronous operation (accept, connect, recv,
//               send, or a plain posted task) and later collects it from
//               poll() with its result already filled in. Backends:
//               - iocp_proactor:    native IOCP (AcceptEx, ConnectEx,
//                                   WSARecv, WSASend) on Windows
//               - reactor_proactor: adapter that performs the I/O itself on
//                                   top of any io_reactor (epoll on Linux,
//                                   IOCP + AFD poll on Windows)
// Usage:        Create one with make_platform_proactor() (platform_proactor.h).
//               register_handle() each socket, fill an io_operation with one
//               of the prepare_*() helpers, start() it, and poll() until the
//               operation is returned.
//
//               Contract shared by every backend:
//               - Every started or posted operation is returned by poll()
//                 exactly once, including failures and cancellations. start()
//                 never completes an operation synchronously.
//               - The caller owns each io_operation and must keep it alive
//                 and untouched until poll() returns it.
//               - Per socket, at most one read-side operation (accept or
//                 recv) and one write-side operation (connect or send) may
//                 be outstanding at a time.
//               - recv reports 0 bytes on end of stream. send may transfer
//                 fewer bytes than requested; callers continue with the rest.
//               - Cancelled operations report errc::operation_canceled.
//               - register/unregister/start/cancel/post/interrupt may be
//                 called from any thread; only one thread may call poll() at
//                 a time.
//               - Before closing a socket, make sure it has no outstanding
//                 operation, then unregister_handle() it. Before destroying a
//                 proactor, every operation must have been returned.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_DETAIL_IO_PROACTOR_H
#define SPANIO_DETAIL_IO_PROACTOR_H

#include "spanio/detail/platform.h"

#include <cstddef>
#include <cstring>
#include <memory>
#include <system_error>
#include <type_traits>
#include <vector>

namespace spanio { namespace detail {

#if defined(SPANIO_PLATFORM_WINDOWS)
// AcceptEx needs 16 bytes of padding after each address it writes
constexpr std::size_t kAcceptExAddressPadding = 16;

// Space AcceptEx needs for one address (local or remote)
constexpr std::size_t kAcceptExAddressLength = sizeof(sockaddr_storage) + kAcceptExAddressPadding;

// Number of addresses AcceptEx writes: the local and the remote one
constexpr std::size_t kAcceptExAddressCount = 2;

// Buffer AcceptEx fills with the local and the remote address
constexpr std::size_t kAcceptExBufferSize = kAcceptExAddressCount * kAcceptExAddressLength;
#endif

// What an io_operation does
enum class op_kind
{
    // Accept one connection on a listening socket (read side)
    kAccept,
    // Connect a socket to m_address (write side)
    kConnect,
    // Receive into m_buffer (read side)
    kRecv,
    // Send from m_buffer (write side)
    kSend,
    // No I/O: completes successfully on the next poll(); used to queue tasks
    kPost
};

// One asynchronous operation. Callers fill it with a prepare_*() helper and
// may derive from it to attach their own data (e.g. a completion handler).
struct io_operation
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // Overlapped block given to Winsock by iocp_proactor. It must stay the
    // first member so a completed OVERLAPPED* converts back to io_operation*.
    OVERLAPPED m_overlapped;
#endif
    // Kind of operation
    op_kind m_kind;
    // Socket the operation runs on (the listener for kAccept)
    native_handle_t m_handle;
    // kRecv: destination buffer. kSend: source data (never written).
    char* m_buffer;
    // Size of m_buffer in bytes
    std::size_t m_size;
    // kConnect: address to connect to
    sockaddr_storage m_address;
    // kConnect: number of meaningful bytes in m_address
    socklen_type m_address_len;
    // kAccept result: the accepted socket, owned by the caller once the
    // operation succeeded; kInvalidHandle otherwise
    native_handle_t m_accepted;
    // Result: empty on success
    std::error_code m_ec;
    // Result: bytes transferred by kRecv / kSend; 0 for other kinds
    std::size_t m_bytes;
#if defined(SPANIO_PLATFORM_WINDOWS)
    // iocp_proactor: address buffer AcceptEx writes into
    char m_accept_buffer[kAcceptExBufferSize];
#endif
};

#if defined(SPANIO_PLATFORM_WINDOWS)
// The OVERLAPPED* <-> io_operation* conversion is only valid for a
// standard-layout type whose first member is m_overlapped
static_assert(std::is_standard_layout<io_operation>::value,
              "io_operation must be standard-layout");
#endif

// Operations returned by one poll() call
using completion_list = std::vector<io_operation*>;

// Reset the common fields of an operation before (re)use
// Parameter op:     operation to reset
// Parameter kind:   kind it is being prepared for
// Parameter handle: socket it will run on
inline void reset_operation(io_operation& op, op_kind kind, native_handle_t handle) noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    std::memset(&op.m_overlapped, 0, sizeof(op.m_overlapped));
#endif
    op.m_kind = kind;
    op.m_handle = handle;
    op.m_buffer = nullptr;
    op.m_size = 0;
    op.m_address_len = 0;
    op.m_accepted = kInvalidHandle;
    op.m_ec.clear();
    op.m_bytes = 0;
}

// Prepare an accept operation
// Parameter op:       operation to fill
// Parameter listener: registered listening socket
inline void prepare_accept(io_operation& op, native_handle_t listener) noexcept
{
    reset_operation(op, op_kind::kAccept, listener);
}

// Prepare a connect operation
// Parameter op:      operation to fill
// Parameter socket:  registered, not yet connected socket
// Parameter address: destination address (copied into the operation)
// Parameter length:  size of *address in bytes; at most sizeof(sockaddr_storage)
inline void prepare_connect(io_operation& op, native_handle_t socket,
                            const sockaddr* address, socklen_type length) noexcept
{
    reset_operation(op, op_kind::kConnect, socket);
    std::memset(&op.m_address, 0, sizeof(op.m_address));
    std::memcpy(&op.m_address, address, static_cast<std::size_t>(length));
    op.m_address_len = length;
}

// Prepare a receive operation
// Parameter op:     operation to fill
// Parameter socket: registered, connected socket
// Parameter buffer: destination; must stay valid until the operation returns
// Parameter size:   capacity of buffer in bytes
inline void prepare_recv(io_operation& op, native_handle_t socket, char* buffer,
                         std::size_t size) noexcept
{
    reset_operation(op, op_kind::kRecv, socket);
    op.m_buffer = buffer;
    op.m_size = size;
}

// Prepare a send operation
// Parameter op:     operation to fill
// Parameter socket: registered, connected socket
// Parameter data:   bytes to send; must stay valid until the operation returns
// Parameter size:   number of bytes to send
inline void prepare_send(io_operation& op, native_handle_t socket, const char* data,
                         std::size_t size) noexcept
{
    reset_operation(op, op_kind::kSend, socket);
    // Stored without const because the field is shared with kRecv; send
    // paths only ever read through it
    op.m_buffer = const_cast<char*>(data);
    op.m_size = size;
}

// Prepare a posted task (no I/O)
// Parameter op: operation to fill
inline void prepare_post(io_operation& op) noexcept
{
    reset_operation(op, op_kind::kPost, kInvalidHandle);
}

// Whether an operation occupies the read side of its socket
// Parameter kind: operation kind
// Return value: true for kAccept and kRecv
inline bool is_read_side(op_kind kind) noexcept
{
    return kind == op_kind::kAccept || kind == op_kind::kRecv;
}

// Map platform-specific "operation aborted" codes to errc::operation_canceled
// so callers can test cancellation the same way on every backend
// Parameter ec: raw error from the system
// Return value: errc::operation_canceled for aborts, ec unchanged otherwise
inline std::error_code normalize_error(const std::error_code& ec) noexcept
{
#if defined(SPANIO_PLATFORM_WINDOWS)
    // WSA_OPERATION_ABORTED and ERROR_OPERATION_ABORTED share one value
    if (ec.value() == WSA_OPERATION_ABORTED)
#else
    if (ec.value() == ECANCELED)
#endif
    {
        return std::make_error_code(std::errc::operation_canceled);
    }
    return ec;
}

// Abstract completion backend. See the file header for the contract every
// implementation follows.
class io_proactor
{
public:
    virtual ~io_proactor() = default;

    // Prepare a socket for asynchronous operations on this proactor
    // Parameter handle: open socket, not registered yet
    // Return value: empty on success, errc::file_exists if already
    //               registered (reactor backend), otherwise the system error
    virtual std::error_code register_handle(native_handle_t handle) = 0;

    // Forget a socket. Outstanding operations on it complete as cancelled.
    // Parameter handle: a registered socket
    // Return value: empty on success, errc::no_such_file_or_directory if not
    //               registered (reactor backend), otherwise the system error
    virtual std::error_code unregister_handle(native_handle_t handle) = 0;

    // Start an operation prepared with a prepare_*() helper. Any failure,
    // even an immediate one, is reported through poll().
    // Parameter op: operation to run; owned by the caller
    virtual void start(io_operation* op) = 0;

    // Cancel every outstanding operation on a socket. They are returned by
    // poll() with errc::operation_canceled, unless they completed first.
    // Parameter handle: a registered socket
    virtual void cancel(native_handle_t handle) = 0;

    // Queue an operation prepared with prepare_post(); the next poll()
    // returns it with an empty error
    // Parameter op: operation to queue; owned by the caller
    virtual void post(io_operation* op) = 0;

    // Wait for finished operations
    // Parameter out:        cleared, then filled with finished operations
    // Parameter timeout_ms: maximum wait in milliseconds; 0 returns at once,
    //                       negative waits without limit
    // Parameter ec:         set to the failure reason, or cleared
    // Return value: number of operations written to out
    virtual std::size_t poll(completion_list& out, int timeout_ms, std::error_code& ec) = 0;

    // Wake up a poll() blocked in another thread; safe from any thread
    virtual void interrupt() noexcept = 0;

    // Short backend name for logs and tests
    // Return value: static string, never null
    virtual const char* name() const noexcept = 0;
};

// Owning pointer to a proactor of any type
using io_proactor_ptr = std::unique_ptr<io_proactor>;

}}

#endif // SPANIO_DETAIL_IO_PROACTOR_H
