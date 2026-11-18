// ============================================================================
// File purpose: Native Linux io_proactor on io_uring (design doc 4.4, M5).
//               Operations are written as SQEs into the submission ring and
//               their results are read from the completion ring, so the
//               kernel performs the I/O itself and no readiness round trip
//               is needed. The rings are set up with the raw io_uring_setup
//               / io_uring_enter system calls and mmap(); no liburing or
//               other third-party library is used.
//
//               Kernel requirements (checked at run time by create()):
//               - io_uring_setup must be allowed (some containers block it)
//               - IORING_FEAT_NODROP (5.5): completions are never lost when
//                 the completion ring is full
//               - IORING_FEAT_EXT_ARG (5.11): poll() waits with a timeout
//                 without a timeout SQE
//               A kernel without them makes create() fail, and
//               make_platform_proactor() falls back to reactor_proactor.
//
//               Design:
//               - Every start() writes one SQE and submits it at once, so a
//                 poll() blocked in io_uring_enter() never waits on an SQE
//                 that the kernel has not seen. If the kernel refuses a
//                 submission (EBUSY/EAGAIN), the SQE stays in the ring and
//                 the next io_uring_enter() of any thread submits it.
//               - post() and immediate failures go to a ready list; the
//                 poller is woken by a NOP SQE (at most one in flight).
//               - Each registered socket remembers its read-side and
//                 write-side operation, so cancel() can target them with
//                 IORING_OP_ASYNC_CANCEL. A cancelled operation reports
//                 errc::operation_canceled (-ECANCELED, or -EINTR for one
//                 interrupted while running), unless it completed first.
//               - Registered sockets are switched to blocking mode: io_uring
//                 handles readiness itself, and some 5.x kernels hand -EAGAIN
//                 back to the caller for O_NONBLOCK sockets.
// Usage:        Build with SPANIO_ENABLE_IO_URING (CMake option of the same
//               name); make_platform_proactor() then prefers this backend.
//               Explicit use: io_uring_proactor::create(entries, ec), or the
//               factory make_io_uring_proactor(ec), which reports
//               errc::function_not_supported on builds without the backend.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-18
// ============================================================================

#ifndef SPANIO_DETAIL_IO_URING_PROACTOR_H
#define SPANIO_DETAIL_IO_URING_PROACTOR_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/socket_ops.h"

#if defined(SPANIO_PLATFORM_LINUX) && defined(SPANIO_ENABLE_IO_URING)
#include <linux/io_uring.h>
// The backend needs the 5.11 ABI (EXT_ARG); older kernel headers compile it
// out and the factory reports "not supported"
#if defined(IORING_FEAT_EXT_ARG)
// Defined when the io_uring backend is compiled into this build
#define SPANIO_HAS_IO_URING 1
#endif
#endif

#if defined(SPANIO_HAS_IO_URING)

#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <csignal>
#include <ctime>

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// System call numbers are the same on every architecture since 5.1 (except
// alpha); older C library headers may not name them yet
#if !defined(__NR_io_uring_setup)
#define __NR_io_uring_setup 425
#endif
#if !defined(__NR_io_uring_enter)
#define __NR_io_uring_enter 426
#endif

#endif // SPANIO_HAS_IO_URING

#include <memory>
#include <system_error>

namespace spanio { namespace detail {

#if defined(SPANIO_HAS_IO_URING)

// Default number of submission ring entries. Every SQE is submitted at once,
// so this bounds only SQEs the kernel has refused for a moment.
constexpr unsigned kUringDefaultEntries = 256;

// Completion ring size as a multiple of the submission ring size. A larger
// completion ring keeps the kernel's overflow list (slow path) rarely used
// when many operations are in flight.
constexpr unsigned kUringCqMultiplier = 16;

// user_data of the NOP that wakes a blocked poll(); operation pointers are
// never null, so 0 cannot collide with them
constexpr std::uint64_t kUringWakeupTag = 0;

// user_data of IORING_OP_ASYNC_CANCEL requests; their own completions carry
// no operation and are dropped (1 is never a valid aligned pointer)
constexpr std::uint64_t kUringCancelTag = 1;

// How often a full submission ring is retried before the operation fails
constexpr int kUringSubmitRetries = 1000;

// Pause between two retries of a full submission ring (total ~1 s)
constexpr std::chrono::microseconds kUringSubmitRetryPause(1000);

// Most overflow flushes poll() performs before returning what it has
constexpr int kUringMaxOverflowFlushes = 4;

// Largest transfer one SQE is asked for: a CQE reports bytes as a 32-bit
// signed result, so larger requests are clamped and complete partially
constexpr std::size_t kUringMaxTransfer = static_cast<std::size_t>(INT_MAX);

// Buffers given to recvmsg / sendmsg for one datagram
constexpr std::size_t kUringDatagramIovecs = 1;

// "No file" descriptor for SQEs that do not act on a socket (NOP, cancel)
constexpr int kUringNoFd = -1;

// Milliseconds per second, to split a poll() timeout into a timespec
constexpr long long kUringMsPerSecond = 1000;

// Nanoseconds per millisecond, to split a poll() timeout into a timespec
constexpr long long kUringNsPerMs = 1000000;

// Size of the kernel signal set, passed with IORING_ENTER_EXT_ARG
constexpr std::size_t kUringSigsetSize = _NSIG / 8;

// Send flags: MSG_NOSIGNAL turns SIGPIPE on a closed peer into -EPIPE
constexpr unsigned kUringSendFlags = MSG_NOSIGNAL;

// Receive flags: none
constexpr unsigned kUringRecvFlags = 0;

// Flags of accepted sockets: SOCK_CLOEXEC keeps exec() from leaking them,
// as create_socket() does for sockets SpanIO opens itself
constexpr unsigned kUringAcceptFlags = SOCK_CLOEXEC;

// Operations outstanding on one registered socket
struct uring_socket_ops
{
    // Outstanding accept or receive, or null
    io_operation* m_read_op;
    // Outstanding connect or send, or null
    io_operation* m_write_op;
};

// One memory mapping of a ring, unmapped by its owner
struct uring_mapping
{
    // Start of the mapping, or MAP_FAILED when absent
    void* m_address;
    // Length of the mapping in bytes
    std::size_t m_length;
};

// io_proactor backed directly by an io_uring instance
class io_uring_proactor final : public io_proactor
{
public:
    // Set up an io_uring instance and map its rings
    // Parameter entries: submission ring size (rounded up to a power of two
    //                    by the kernel); small values are used by tests
    // Parameter ec:      set to the failure reason (errc::function_not_supported
    //                    when a required kernel feature is missing), or
    //                    cleared on success
    // Parameter cq_entries: completion ring size; 0 means entries *
    //                    kUringCqMultiplier. Tests pass a tiny value to force
    //                    the kernel's overflow path.
    // Return value: the new proactor, or nullptr on failure
    static std::unique_ptr<io_uring_proactor> create(unsigned entries, std::error_code& ec,
                                                     unsigned cq_entries = 0)
    {
        std::unique_ptr<io_uring_proactor> proactor(new io_uring_proactor());
        ec = proactor->setup(entries, cq_entries);
        if (ec)
        {
            log_error("io_uring_proactor::create", ec);
            return nullptr;
        }
        return proactor;
    }

    // Unmap the rings and close the instance. Every operation must have
    // been returned by poll() before this point (see the io_proactor
    // contract); the kernel would otherwise still write into them.
    ~io_uring_proactor() override
    {
        unmap(m_sqe_mapping);
        unmap(m_cq_mapping);
        unmap(m_sq_mapping);
        if (m_ring_fd != kInvalidHandle)
        {
            ::close(m_ring_fd);
        }
    }

    io_uring_proactor(const io_uring_proactor&) = delete;
    io_uring_proactor& operator=(const io_uring_proactor&) = delete;

    // See io_proactor::register_handle. Also switches the socket to
    // blocking mode (see the file header).
    std::error_code register_handle(native_handle_t handle) override
    {
        const std::error_code ec = set_nonblocking(handle, false);
        if (ec)
        {
            return ec;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_sockets.emplace(handle, uring_socket_ops{nullptr, nullptr}).second)
        {
            return std::make_error_code(std::errc::file_exists);
        }
        return std::error_code();
    }

    // See io_proactor::unregister_handle. Outstanding operations are
    // cancelled in the kernel before this returns, so closing the socket
    // afterwards is safe; they come back through poll() as cancelled.
    std::error_code unregister_handle(native_handle_t handle) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_sockets.find(handle);
        if (it == m_sockets.end())
        {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        cancel_ops_locked(it->second);
        m_sockets.erase(it);
        return std::error_code();
    }

    // See io_proactor::start
    void start(io_operation* op) override
    {
        if (op->m_kind == op_kind::kPost)
        {
            post(op);
            return;
        }

        bool failed = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_sockets.find(op->m_handle);
            if (it == m_sockets.end())
            {
                op->m_ec = std::make_error_code(std::errc::bad_file_descriptor);
                log_error("io_uring_proactor::start(unregistered socket)", op->m_ec);
                m_ready.push_back(op);
                failed = true;
            }
            else if (!submit_operation_locked(*op))
            {
                // No SQE could be had; report it through poll()
                op->m_ec = std::make_error_code(std::errc::resource_unavailable_try_again);
                log_error("io_uring_proactor::start(submission ring full)", op->m_ec);
                m_ready.push_back(op);
                failed = true;
            }
            else if (is_read_side(op->m_kind))
            {
                it->second.m_read_op = op;
            }
            else
            {
                it->second.m_write_op = op;
            }
        }
        // poll() may be blocked in another thread with nothing to wake it
        if (failed)
        {
            interrupt();
        }
    }

    // See io_proactor::cancel
    void cancel(native_handle_t handle) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_sockets.find(handle);
        if (it != m_sockets.end())
        {
            cancel_ops_locked(it->second);
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
        interrupt();
    }

    // See io_proactor::poll
    std::size_t poll(completion_list& out, int timeout_ms, std::error_code& ec) override
    {
        out.clear();
        ec.clear();
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            take_ready_locked(out);
        }

        // Something is already finished: only collect what the kernel has
        // right now instead of blocking
        m_reaped.clear();
        reap();
        if (out.empty() && m_reaped.empty() && timeout_ms != 0)
        {
            ec = wait_for_completion(timeout_ms);
            reap();
        }
        // Completions the kernel parked on its overflow list while the
        // completion ring was full are moved back by a GETEVENTS enter
        for (int i = 0; i < kUringMaxOverflowFlushes && cq_overflowed(); ++i)
        {
            enter(0, 0, IORING_ENTER_GETEVENTS);
            reap();
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        for (const io_uring_cqe& cqe : m_reaped)
        {
            complete_locked(cqe, out);
        }
        // Operations finished by other threads while the kernel waited
        take_ready_locked(out);
        return out.size();
    }

    // See io_proactor::interrupt
    void interrupt() noexcept override
    {
        // Only one wake-up NOP may be in flight at a time
        if (m_interrupt_pending.exchange(true))
        {
            return;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        io_uring_sqe* const sqe = acquire_sqe_locked();
        if (sqe == nullptr)
        {
            m_interrupt_pending.store(false);
            log_error("io_uring_proactor::interrupt(submission ring full)",
                      std::make_error_code(std::errc::resource_unavailable_try_again));
            return;
        }
        sqe->opcode = IORING_OP_NOP;
        sqe->fd = kUringNoFd;
        sqe->user_data = kUringWakeupTag;
        commit_sqe_locked();
    }

    // See io_proactor::name
    const char* name() const noexcept override
    {
        return "io-uring-proactor";
    }

private:
    // Start empty; create() calls setup()
    io_uring_proactor() noexcept
        : m_ring_fd(kInvalidHandle)
        , m_sq_mapping{MAP_FAILED, 0}
        , m_cq_mapping{MAP_FAILED, 0}
        , m_sqe_mapping{MAP_FAILED, 0}
        , m_sq_head(nullptr)
        , m_sq_tail(nullptr)
        , m_sq_mask(0)
        , m_sq_entries(0)
        , m_sq_flags(nullptr)
        , m_sq_array(nullptr)
        , m_sqes(nullptr)
        , m_cq_head(nullptr)
        , m_cq_tail(nullptr)
        , m_cq_mask(0)
        , m_cqes(nullptr)
        , m_interrupt_pending(false)
    {
    }

    // Wrap the errno of a failed call as an error code
    // Return value: normalized system error
    static std::error_code errno_error() noexcept
    {
        return normalize_error(std::error_code(errno, std::system_category()));
    }

    // Unmap one ring mapping if it exists
    // Parameter mapping: mapping to release; reset to absent
    static void unmap(uring_mapping& mapping) noexcept
    {
        if (mapping.m_address != MAP_FAILED)
        {
            ::munmap(mapping.m_address, mapping.m_length);
            mapping.m_address = MAP_FAILED;
        }
    }

    // Map one region of the ring file descriptor
    // Parameter length: bytes to map
    // Parameter offset: IORING_OFF_SQ_RING, IORING_OFF_CQ_RING or IORING_OFF_SQES
    // Parameter out:    receives the mapping
    // Return value: empty on success, otherwise the system error
    std::error_code map_region(std::size_t length, off_t offset, uring_mapping& out) noexcept
    {
        void* const address = ::mmap(nullptr, length, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_POPULATE, m_ring_fd, offset);
        if (address == MAP_FAILED)
        {
            return errno_error();
        }
        out.m_address = address;
        out.m_length = length;
        return std::error_code();
    }

    // Pointer to a field at a byte offset inside a mapping
    // Parameter mapping: ring mapping
    // Parameter offset:  offset reported by io_uring_setup
    // Return value: typed pointer into the mapping
    template <typename T>
    static T* field(const uring_mapping& mapping, std::uint32_t offset) noexcept
    {
        return reinterpret_cast<T*>(static_cast<char*>(mapping.m_address) + offset);
    }

    // Create the instance, check the kernel features and map the rings
    // Parameter entries:    requested submission ring size
    // Parameter cq_entries: requested completion ring size, 0 for the default
    // Return value: empty on success, otherwise the reason
    std::error_code setup(unsigned entries, unsigned cq_entries) noexcept
    {
        io_uring_params params{};
        params.flags = IORING_SETUP_CQSIZE;
        params.cq_entries = (cq_entries != 0) ? cq_entries : entries * kUringCqMultiplier;
        const long fd = ::syscall(__NR_io_uring_setup, entries, &params);
        if (fd < 0)
        {
            // ENOSYS: kernel too old; EPERM: blocked (seccomp, sysctl)
            return errno_error();
        }
        m_ring_fd = static_cast<native_handle_t>(fd);

        const unsigned required = IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG;
        if ((params.features & required) != required)
        {
            return std::make_error_code(std::errc::function_not_supported);
        }

        const std::size_t sq_length = params.sq_off.array + params.sq_entries * sizeof(unsigned);
        const std::size_t cq_length = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
        std::error_code ec;
        if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0)
        {
            // One mapping holds both rings
            ec = map_region(sq_length > cq_length ? sq_length : cq_length, IORING_OFF_SQ_RING,
                            m_sq_mapping);
        }
        else
        {
            ec = map_region(sq_length, IORING_OFF_SQ_RING, m_sq_mapping);
            if (!ec)
            {
                ec = map_region(cq_length, IORING_OFF_CQ_RING, m_cq_mapping);
            }
        }
        if (!ec)
        {
            ec = map_region(params.sq_entries * sizeof(io_uring_sqe), IORING_OFF_SQES, m_sqe_mapping);
        }
        if (ec)
        {
            return ec;
        }

        const uring_mapping& cq = (m_cq_mapping.m_address != MAP_FAILED) ? m_cq_mapping : m_sq_mapping;
        m_sq_head = field<unsigned>(m_sq_mapping, params.sq_off.head);
        m_sq_tail = field<unsigned>(m_sq_mapping, params.sq_off.tail);
        m_sq_mask = *field<unsigned>(m_sq_mapping, params.sq_off.ring_mask);
        m_sq_entries = *field<unsigned>(m_sq_mapping, params.sq_off.ring_entries);
        m_sq_flags = field<unsigned>(m_sq_mapping, params.sq_off.flags);
        m_sq_array = field<unsigned>(m_sq_mapping, params.sq_off.array);
        m_sqes = static_cast<io_uring_sqe*>(m_sqe_mapping.m_address);
        m_cq_head = field<unsigned>(cq, params.cq_off.head);
        m_cq_tail = field<unsigned>(cq, params.cq_off.tail);
        m_cq_mask = *field<unsigned>(cq, params.cq_off.ring_mask);
        m_cqes = field<io_uring_cqe>(cq, params.cq_off.cqes);
        return std::error_code();
    }

    // Call io_uring_enter
    // Parameter to_submit:    SQEs the kernel may consume
    // Parameter min_complete: completions to wait for (with GETEVENTS)
    // Parameter flags:        IORING_ENTER_* flags
    // Parameter arg:          extended argument (EXT_ARG), or null
    // Return value: the system call's result; -1 with errno on failure
    long enter(unsigned to_submit, unsigned min_complete, unsigned flags,
               const io_uring_getevents_arg* arg = nullptr) noexcept
    {
        const std::size_t arg_size = (arg == nullptr) ? 0 : sizeof(*arg);
        return ::syscall(__NR_io_uring_enter, m_ring_fd, to_submit, min_complete, flags, arg, arg_size);
    }

    // SQEs written to the ring that the kernel has not consumed yet
    // Return value: count; 0 when everything was submitted
    unsigned pending_sqes() const noexcept
    {
        return __atomic_load_n(m_sq_tail, __ATOMIC_ACQUIRE) - __atomic_load_n(m_sq_head, __ATOMIC_ACQUIRE);
    }

    // Whether the kernel holds completions on its overflow list
    // Return value: true if IORING_SQ_CQ_OVERFLOW is set
    bool cq_overflowed() const noexcept
    {
        return (__atomic_load_n(m_sq_flags, __ATOMIC_ACQUIRE) & IORING_SQ_CQ_OVERFLOW) != 0;
    }

    // Get the next free SQE, zeroed. A full ring is submitted and retried
    // for about a second. Caller holds m_mutex.
    // Return value: the SQE, or null if the kernel kept refusing SQEs
    io_uring_sqe* acquire_sqe_locked() noexcept
    {
        for (int attempt = 0; attempt < kUringSubmitRetries; ++attempt)
        {
            const unsigned tail = *m_sq_tail;
            if (tail - __atomic_load_n(m_sq_head, __ATOMIC_ACQUIRE) < m_sq_entries)
            {
                io_uring_sqe* const sqe = &m_sqes[tail & m_sq_mask];
                std::memset(sqe, 0, sizeof(*sqe));
                return sqe;
            }
            // Full: push the ring to the kernel. EBUSY means its overflow
            // list must drain first; GETEVENTS flushes it once the poller
            // has made room in the completion ring.
            if (enter(m_sq_entries, 0, 0) < 0)
            {
                enter(0, 0, IORING_ENTER_GETEVENTS);
                std::this_thread::sleep_for(kUringSubmitRetryPause);
            }
        }
        return nullptr;
    }

    // Publish the SQE returned by acquire_sqe_locked() and submit every
    // pending SQE. A refused submission leaves them in the ring; a later
    // io_uring_enter() submits them. Caller holds m_mutex.
    void commit_sqe_locked() noexcept
    {
        const unsigned tail = *m_sq_tail;
        m_sq_array[tail & m_sq_mask] = tail & m_sq_mask;
        __atomic_store_n(m_sq_tail, tail + 1, __ATOMIC_RELEASE);
        if (enter(pending_sqes(), 0, 0) < 0)
        {
            const int error = errno;
            if (error != EBUSY && error != EAGAIN && error != EINTR)
            {
                log_error("io_uring_enter(submit)", errno_error());
            }
        }
    }

    // Clamp a transfer size to what one SQE accepts
    // Parameter size: requested size
    // Return value: size, at most kUringMaxTransfer
    static std::size_t clamp_transfer(std::size_t size) noexcept
    {
        return size < kUringMaxTransfer ? size : kUringMaxTransfer;
    }

    // Point an operation's message header at its buffer and address, for
    // IORING_OP_RECVMSG / IORING_OP_SENDMSG
    // Parameter op:           datagram operation
    // Parameter address_size: size of the address buffer (receive) or of
    //                         the destination address (send)
    static void prepare_message(io_operation& op, socklen_type address_size) noexcept
    {
        op.m_iovec.iov_base = op.m_buffer;
        op.m_iovec.iov_len = clamp_transfer(op.m_size);
        std::memset(&op.m_message, 0, sizeof(op.m_message));
        op.m_message.msg_name = &op.m_address;
        op.m_message.msg_namelen = address_size;
        op.m_message.msg_iov = &op.m_iovec;
        op.m_message.msg_iovlen = kUringDatagramIovecs;
    }

    // Write and submit the SQE of an I/O operation. Caller holds m_mutex.
    // Parameter op: operation to start
    // Return value: false if no SQE could be had
    bool submit_operation_locked(io_operation& op) noexcept
    {
        io_uring_sqe* const sqe = acquire_sqe_locked();
        if (sqe == nullptr)
        {
            return false;
        }
        sqe->fd = op.m_handle;
        sqe->user_data = reinterpret_cast<std::uintptr_t>(&op);
        switch (op.m_kind)
        {
        case op_kind::kAccept:
            sqe->opcode = IORING_OP_ACCEPT;
            sqe->accept_flags = kUringAcceptFlags;
            break;
        case op_kind::kConnect:
            sqe->opcode = IORING_OP_CONNECT;
            sqe->addr = reinterpret_cast<std::uintptr_t>(&op.m_address);
            // IORING_OP_CONNECT takes the address length in the offset field
            sqe->off = static_cast<std::uint64_t>(op.m_address_len);
            break;
        case op_kind::kRecv:
            sqe->opcode = IORING_OP_RECV;
            sqe->addr = reinterpret_cast<std::uintptr_t>(op.m_buffer);
            sqe->len = static_cast<std::uint32_t>(clamp_transfer(op.m_size));
            sqe->msg_flags = kUringRecvFlags;
            break;
        case op_kind::kSend:
            sqe->opcode = IORING_OP_SEND;
            sqe->addr = reinterpret_cast<std::uintptr_t>(op.m_buffer);
            sqe->len = static_cast<std::uint32_t>(clamp_transfer(op.m_size));
            sqe->msg_flags = kUringSendFlags;
            break;
        case op_kind::kRecvFrom:
            prepare_message(op, static_cast<socklen_type>(sizeof(op.m_address)));
            sqe->opcode = IORING_OP_RECVMSG;
            sqe->addr = reinterpret_cast<std::uintptr_t>(&op.m_message);
            sqe->len = kUringDatagramIovecs;
            sqe->msg_flags = kUringRecvFlags;
            break;
        case op_kind::kSendTo:
            prepare_message(op, op.m_address_len);
            sqe->opcode = IORING_OP_SENDMSG;
            sqe->addr = reinterpret_cast<std::uintptr_t>(&op.m_message);
            sqe->len = kUringDatagramIovecs;
            sqe->msg_flags = kUringSendFlags;
            break;
        case op_kind::kPost:
        default:
            // start() routes posts to post(); a stray one completes as a NOP
            sqe->opcode = IORING_OP_NOP;
            sqe->fd = kUringNoFd;
            break;
        }
        commit_sqe_locked();
        return true;
    }

    // Ask the kernel to cancel the outstanding operations of one socket.
    // The cancellation runs inside io_uring_enter(), so once this returns
    // the socket may be closed. Caller holds m_mutex.
    // Parameter ops: the socket's outstanding operations
    void cancel_ops_locked(const uring_socket_ops& ops) noexcept
    {
        io_operation* const outstanding[] = {ops.m_read_op, ops.m_write_op};
        for (io_operation* op : outstanding)
        {
            // An operation already being cancelled needs no second request
            if (op == nullptr || !m_cancelled.insert(op).second)
            {
                continue;
            }
            io_uring_sqe* const sqe = acquire_sqe_locked();
            if (sqe == nullptr)
            {
                m_cancelled.erase(op);
                log_error("io_uring_proactor::cancel(submission ring full)",
                          std::make_error_code(std::errc::resource_unavailable_try_again));
                continue;
            }
            sqe->opcode = IORING_OP_ASYNC_CANCEL;
            sqe->fd = kUringNoFd;
            sqe->addr = reinterpret_cast<std::uintptr_t>(op);
            sqe->user_data = kUringCancelTag;
            commit_sqe_locked();
        }
    }

    // Wait until at least one completion arrives or the timeout expires
    // Parameter timeout_ms: milliseconds; negative waits without limit
    // Return value: empty on a completion, timeout or signal; otherwise the
    //               system error
    std::error_code wait_for_completion(int timeout_ms) noexcept
    {
        const wait_deadline deadline(timeout_ms);
        for (;;)
        {
            const int left_ms = deadline.remaining_ms();
            __kernel_timespec timeout{};
            io_uring_getevents_arg arg{};
            arg.sigmask = 0;
            arg.sigmask_sz = static_cast<std::uint32_t>(kUringSigsetSize);
            if (left_ms >= 0)
            {
                timeout.tv_sec = left_ms / kUringMsPerSecond;
                timeout.tv_nsec = (left_ms % kUringMsPerSecond) * kUringNsPerMs;
                arg.ts = reinterpret_cast<std::uintptr_t>(&timeout);
            }
            // SQEs another thread could not submit are pushed along as well
            if (enter(pending_sqes(), 1, IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG, &arg) >= 0)
            {
                return std::error_code();
            }
            const int error = errno;
            // EINTR also comes from the TWA_SIGNAL task_work the kernel
            // queues when any io_uring this thread used is closed; keep
            // waiting for the rest of the timeout instead of returning early
            if (error == EINTR && deadline.remaining_ms() != 0)
            {
                continue;
            }
            if (error == ETIME || error == EINTR || error == EBUSY || error == EAGAIN)
            {
                return std::error_code();
            }
            const std::error_code ec = errno_error();
            log_error("io_uring_enter(wait)", ec);
            return ec;
        }
    }

    // Copy every available CQE into m_reaped and release its ring slot.
    // Only the polling thread touches the completion ring, so no lock is
    // needed; this also lets a thread stuck in acquire_sqe_locked() get
    // room while holding m_mutex.
    void reap() noexcept
    {
        unsigned head = *m_cq_head;
        const unsigned tail = __atomic_load_n(m_cq_tail, __ATOMIC_ACQUIRE);
        while (head != tail)
        {
            m_reaped.push_back(m_cqes[head & m_cq_mask]);
            ++head;
        }
        __atomic_store_n(m_cq_head, head, __ATOMIC_RELEASE);
    }

    // Turn one CQE into a finished operation. Caller holds m_mutex.
    // Parameter cqe: completion copied from the ring
    // Parameter out: receives the finished operation
    void complete_locked(const io_uring_cqe& cqe, completion_list& out)
    {
        if (cqe.user_data == kUringWakeupTag)
        {
            // Allow the next interrupt() to submit a new NOP
            m_interrupt_pending.store(false);
            return;
        }
        if (cqe.user_data == kUringCancelTag)
        {
            // -ENOENT / -EALREADY only mean the target finished or is
            // finishing on its own; it still reports its own CQE
            if (cqe.res < 0 && cqe.res != -ENOENT && cqe.res != -EALREADY)
            {
                log_error("io_uring cancel", std::error_code(-cqe.res, std::system_category()));
            }
            return;
        }

        io_operation* const op = reinterpret_cast<io_operation*>(static_cast<std::uintptr_t>(cqe.user_data));
        const bool cancelled = m_cancelled.erase(op) > 0;
        // The slot may already belong to a new socket with the same
        // descriptor, so only clear it when it still points at op
        const auto it = m_sockets.find(op->m_handle);
        if (it != m_sockets.end())
        {
            if (it->second.m_read_op == op)
            {
                it->second.m_read_op = nullptr;
            }
            if (it->second.m_write_op == op)
            {
                it->second.m_write_op = nullptr;
            }
        }

        finish_operation(*op, cqe.res, cancelled);
        out.push_back(op);
    }

    // Fill in an operation's result from its CQE
    // Parameter op:        the finished operation
    // Parameter res:       CQE result: bytes, accepted descriptor or -errno
    // Parameter cancelled: whether cancel() targeted it
    static void finish_operation(io_operation& op, std::int32_t res, bool cancelled) noexcept
    {
        op.m_bytes = 0;
        if (res < 0)
        {
            // A request interrupted while running reports -EINTR
            if (res == -ECANCELED || (cancelled && res == -EINTR))
            {
                op.m_ec = std::make_error_code(std::errc::operation_canceled);
            }
            else
            {
                op.m_ec = normalize_error(std::error_code(-res, std::system_category()));
                log_error("io_uring_proactor I/O", op.m_ec);
            }
            return;
        }

        op.m_ec.clear();
        switch (op.m_kind)
        {
        case op_kind::kAccept:
            op.m_accepted = static_cast<native_handle_t>(res);
            break;
        case op_kind::kRecvFrom:
            op.m_bytes = static_cast<std::size_t>(res);
            op.m_address_len = op.m_message.msg_namelen;
            // The kernel copies the message flags back: MSG_TRUNC reveals
            // a datagram larger than the buffer
            if ((op.m_message.msg_flags & MSG_TRUNC) != 0)
            {
                op.m_ec = std::make_error_code(std::errc::message_size);
            }
            break;
        case op_kind::kRecv:
        case op_kind::kSend:
        case op_kind::kSendTo:
            op.m_bytes = static_cast<std::size_t>(res);
            break;
        case op_kind::kConnect:
        case op_kind::kPost:
        default:
            break;
        }
    }

    // Move queued finished operations into a poll() result. Caller holds
    // m_mutex.
    // Parameter out: receives the operations
    void take_ready_locked(completion_list& out)
    {
        out.insert(out.end(), m_ready.begin(), m_ready.end());
        m_ready.clear();
    }

    // io_uring instance descriptor, or kInvalidHandle
    native_handle_t m_ring_fd;
    // Mapping of the submission ring (and of the completion ring with
    // IORING_FEAT_SINGLE_MMAP)
    uring_mapping m_sq_mapping;
    // Separate mapping of the completion ring, or absent
    uring_mapping m_cq_mapping;
    // Mapping of the SQE array
    uring_mapping m_sqe_mapping;
    // Submission ring head, advanced by the kernel as it consumes SQEs
    unsigned* m_sq_head;
    // Submission ring tail, advanced by this class under m_mutex
    unsigned* m_sq_tail;
    // Mask turning a submission ring position into an index
    unsigned m_sq_mask;
    // Number of submission ring entries
    unsigned m_sq_entries;
    // Submission ring flags (IORING_SQ_CQ_OVERFLOW)
    unsigned* m_sq_flags;
    // Submission ring index array, pointing into m_sqes
    unsigned* m_sq_array;
    // SQE array shared with the kernel
    io_uring_sqe* m_sqes;
    // Completion ring head, advanced by the polling thread
    unsigned* m_cq_head;
    // Completion ring tail, advanced by the kernel
    unsigned* m_cq_tail;
    // Mask turning a completion ring position into an index
    unsigned m_cq_mask;
    // CQE array shared with the kernel
    io_uring_cqe* m_cqes;
    // Guards the submission ring, m_sockets, m_cancelled and m_ready
    std::mutex m_mutex;
    // Registered sockets and their outstanding operations
    std::unordered_map<native_handle_t, uring_socket_ops> m_sockets;
    // Operations targeted by an ASYNC_CANCEL whose CQE has not arrived
    std::unordered_set<io_operation*> m_cancelled;
    // Finished operations (posts, immediate failures) waiting for poll()
    std::vector<io_operation*> m_ready;
    // CQEs copied by the current poll(); only touched by the polling thread
    std::vector<io_uring_cqe> m_reaped;
    // Whether a wake-up NOP is in flight
    std::atomic<bool> m_interrupt_pending;
};

#endif // SPANIO_HAS_IO_URING

// Create the io_uring proactor through the common pointer type
// Parameter ec: set to the failure reason, or cleared on success;
//               errc::function_not_supported when the backend is not part
//               of this build (not Linux, SPANIO_ENABLE_IO_URING off, or
//               kernel headers older than 5.11)
// Return value: the new proactor, or nullptr on failure
inline io_proactor_ptr make_io_uring_proactor(std::error_code& ec)
{
#if defined(SPANIO_HAS_IO_URING)
    return io_uring_proactor::create(kUringDefaultEntries, ec);
#else
    ec = std::make_error_code(std::errc::function_not_supported);
    return nullptr;
#endif
}

}}

#endif // SPANIO_DETAIL_IO_URING_PROACTOR_H
