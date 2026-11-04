// ============================================================================
// File purpose: Windows io_reactor backend. IOCP is a completion model and
//               has no readiness events, so readiness is obtained the same
//               way as wepoll / libuv / mio: each watched socket gets an
//               IOCTL_AFD_POLL request on an \Device\Afd handle bound to an
//               I/O completion port. The request completes when the socket
//               is ready, and its completion packet becomes a ready_event.
//
//               IOCTL_AFD_POLL and its structures are UNDOCUMENTED Windows
//               internals. They have been stable since Windows Vista and are
//               used by the projects named above, but Microsoft could change
//               them without notice.
//
//               Level-triggered emulation: an AFD poll request completes only
//               once. After a socket reports events it is queued, and the
//               next poll() call submits a fresh request, which completes
//               immediately if the socket is still ready.
// Usage:        Create through make_platform_reactor() or
//               iocp_reactor::create(ec). Compiled only on Windows; on other
//               platforms this header is empty.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_IOCP_REACTOR_H
#define SPANIO_DETAIL_IOCP_REACTOR_H

#include "spanio/detail/io_reactor.h"
#include "spanio/detail/socket_ops.h"

#if defined(SPANIO_PLATFORM_WINDOWS)

#include <winternl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

// Winsock ioctls that return the base provider socket underneath any
// layered service providers. Older SDKs lack the names, so define them here.
#ifndef SIO_BASE_HANDLE
#define SIO_BASE_HANDLE _WSAIOR(IOC_WS2, 34)
#endif
#ifndef SIO_BSP_HANDLE_POLL
#define SIO_BSP_HANDLE_POLL _WSAIOR(IOC_WS2, 29)
#endif
#ifndef SIO_BSP_HANDLE_SELECT
#define SIO_BSP_HANDLE_SELECT _WSAIOR(IOC_WS2, 28)
#endif

// NtCreateFile disposition "open an existing object", if winternl.h lacks it
#ifndef FILE_OPEN
#define FILE_OPEN 0x00000001
#endif

namespace spanio { namespace detail {

// ----------------------------------------------------------------------------
// Undocumented AFD definitions (same values as wepoll and ReactOS)
// ----------------------------------------------------------------------------

// AFD poll event: data can be received
constexpr ULONG kAfdPollReceive = 0x0001;
// AFD poll event: out-of-band data can be received
constexpr ULONG kAfdPollReceiveExpedited = 0x0002;
// AFD poll event: data can be sent
constexpr ULONG kAfdPollSend = 0x0004;
// AFD poll event: the peer shut down its sending side (FIN received)
constexpr ULONG kAfdPollDisconnect = 0x0008;
// AFD poll event: the connection was reset or aborted
constexpr ULONG kAfdPollAbort = 0x0010;
// AFD poll event: the socket was closed locally (closesocket)
constexpr ULONG kAfdPollLocalClose = 0x0020;
// AFD poll event: a listening socket has a connection to accept
constexpr ULONG kAfdPollAccept = 0x0080;
// AFD poll event: a non-blocking connect failed
constexpr ULONG kAfdPollConnectFail = 0x0100;

// Device I/O control code of the AFD poll request
constexpr ULONG kIoctlAfdPoll = 0x00012024;

// Number of sockets carried by each AFD poll request (one request per socket)
constexpr ULONG kAfdPollHandleCount = 1;

// Per-socket part of an AFD poll request; layout fixed by the kernel
struct afd_poll_handle_info
{
    // Base provider socket to poll, as a HANDLE
    HANDLE m_handle;
    // In: AFD events to wait for. Out: AFD events that occurred.
    ULONG m_events;
    // Out: per-socket status
    NTSTATUS m_status;
};

// Input and output buffer of IOCTL_AFD_POLL; layout fixed by the kernel
struct afd_poll_info
{
    // Absolute or relative timeout; LLONG_MAX means "no timeout"
    LARGE_INTEGER m_timeout;
    // In: number of entries in m_handles. Out: number of entries with events.
    ULONG m_handle_count;
    // Whether this request replaces other polls on the same sockets (FALSE)
    ULONG m_exclusive;
    // Sockets to poll
    afd_poll_handle_info m_handles[kAfdPollHandleCount];
};

// ----------------------------------------------------------------------------
// NTSTATUS values used below; ntstatus.h conflicts with windows.h, so the
// few values needed are defined here
// ----------------------------------------------------------------------------

// The operation succeeded
constexpr NTSTATUS kStatusSuccess = 0x00000000L;
// The operation was queued and will complete through the completion port
constexpr NTSTATUS kStatusPending = 0x00000103L;
// The operation was cancelled
constexpr NTSTATUS kStatusCancelled = static_cast<NTSTATUS>(0xC0000120L);
// Cancel found no matching request (it already completed)
constexpr NTSTATUS kStatusNotFound = static_cast<NTSTATUS>(0xC0000225L);

// Whether an NTSTATUS means success (the NT_SUCCESS macro)
// Parameter status: value to test
// Return value: true for success and informational codes
inline bool nt_success(NTSTATUS status) noexcept
{
    return status >= 0;
}

// ----------------------------------------------------------------------------
// ntdll entry points, loaded at run time so no import library is needed
// ----------------------------------------------------------------------------

// Signature of NtCreateFile
using nt_create_file_fn = NTSTATUS(NTAPI*)(PHANDLE, ACCESS_MASK, POBJECT_ATTRIBUTES,
                                           PIO_STATUS_BLOCK, PLARGE_INTEGER, ULONG,
                                           ULONG, ULONG, ULONG, PVOID, ULONG);
// Signature of NtDeviceIoControlFile
using nt_device_io_control_file_fn = NTSTATUS(NTAPI*)(HANDLE, HANDLE, PIO_APC_ROUTINE, PVOID,
                                                      PIO_STATUS_BLOCK, ULONG, PVOID, ULONG,
                                                      PVOID, ULONG);
// Signature of NtCancelIoFileEx
using nt_cancel_io_file_ex_fn = NTSTATUS(NTAPI*)(HANDLE, PIO_STATUS_BLOCK, PIO_STATUS_BLOCK);
// Signature of RtlNtStatusToDosError
using rtl_nt_status_to_dos_error_fn = ULONG(NTAPI*)(NTSTATUS);

// The ntdll functions the AFD backend needs
struct nt_api
{
    // Opens the \Device\Afd handle
    nt_create_file_fn m_create_file;
    // Submits IOCTL_AFD_POLL requests
    nt_device_io_control_file_fn m_device_io_control_file;
    // Cancels one specific poll request
    nt_cancel_io_file_ex_fn m_cancel_io_file_ex;
    // Converts NTSTATUS failures to Win32 error codes
    rtl_nt_status_to_dos_error_fn m_status_to_dos_error;
};

// Look up one ntdll export
// Parameter ntdll: module handle of ntdll.dll
// Parameter name:  export name
// Parameter out:   receives the function pointer, or null if missing
template <typename Fn>
inline void load_nt_function(HMODULE ntdll, const char* name, Fn& out) noexcept
{
    out = reinterpret_cast<Fn>(reinterpret_cast<void*>(::GetProcAddress(ntdll, name)));
}

// Resolve the ntdll functions the backend needs
// Return value: the table; a member is null if the export was not found
inline nt_api load_nt_api() noexcept
{
    nt_api api{};
    // ntdll.dll is mapped into every Windows process, so it is never null
    const HMODULE ntdll = ::GetModuleHandleW(L"ntdll.dll");
    if (ntdll != nullptr)
    {
        load_nt_function(ntdll, "NtCreateFile", api.m_create_file);
        load_nt_function(ntdll, "NtDeviceIoControlFile", api.m_device_io_control_file);
        load_nt_function(ntdll, "NtCancelIoFileEx", api.m_cancel_io_file_ex);
        load_nt_function(ntdll, "RtlNtStatusToDosError", api.m_status_to_dos_error);
    }
    return api;
}

// The process-wide ntdll function table, resolved on first use
// Return value: the table; a member is null if the export was not found
inline const nt_api& get_nt_api() noexcept
{
    // Function-local static: resolved exactly once, thread-safe (C++11)
    static const nt_api s_api = load_nt_api();
    return s_api;
}

// Whether every ntdll function the backend needs was found
// Return value: true if the AFD backend can work on this system
inline bool nt_api_available() noexcept
{
    const nt_api& api = get_nt_api();
    return api.m_create_file != nullptr && api.m_device_io_control_file != nullptr
        && api.m_cancel_io_file_ex != nullptr && api.m_status_to_dos_error != nullptr;
}

// Convert a failed NTSTATUS to a std::error_code holding the Win32 error
// Parameter status: failed NTSTATUS
// Return value: the equivalent Win32 error in std::system_category
inline std::error_code nt_status_to_error(NTSTATUS status) noexcept
{
    const ULONG win32_error = get_nt_api().m_status_to_dos_error(status);
    return std::error_code(static_cast<int>(win32_error), std::system_category());
}

// Find the base provider socket under any layered service providers (LSPs).
// AFD only understands base sockets.
// Parameter socket: socket created by the application
// Parameter base:   receives the base socket on success
// Return value: empty on success, otherwise the Winsock error of the last try
inline std::error_code get_base_socket(SOCKET socket, SOCKET& base) noexcept
{
    // SIO_BASE_HANDLE works unless an LSP intercepts it; the BSP ioctls are
    // the fallbacks wepoll uses for such LSPs
    const DWORD ioctls[] = {SIO_BASE_HANDLE, SIO_BSP_HANDLE_POLL, SIO_BSP_HANDLE_SELECT};
    std::error_code ec;
    for (const DWORD ioctl : ioctls)
    {
        SOCKET result = INVALID_SOCKET;
        DWORD bytes = 0;
        if (::WSAIoctl(socket, ioctl, nullptr, 0, &result, static_cast<DWORD>(sizeof(result)),
                       &bytes, nullptr, nullptr) != SOCKET_ERROR
            && result != INVALID_SOCKET)
        {
            base = result;
            return std::error_code();
        }
        ec = last_error();
    }
    return ec;
}

// Lifecycle of the AFD poll request of one socket
enum class afd_poll_status
{
    // No request in flight; the next update submits one
    kIdle,
    // A request is in flight and watches m_pending_afd_events
    kPending,
    // A cancel was issued; a new request is submitted when the cancelled
    // one's completion packet arrives
    kCancelled
};

// Book-keeping for one watched socket. The kernel writes into m_iosb and
// m_poll_info while a request is in flight, so the object must stay at a
// fixed address until that request's completion packet has been processed.
struct afd_socket_state
{
    // Status block of the in-flight request, written by the kernel
    IO_STATUS_BLOCK m_iosb;
    // Request input and output buffer, written by the kernel
    afd_poll_info m_poll_info;
    // Socket as registered by the caller; reported back in ready_event
    descriptor_t m_socket;
    // Base provider socket actually given to AFD
    SOCKET m_base_socket;
    // Events the caller asked for (kEventRead / kEventWrite)
    event_mask m_interest;
    // AFD events of the request in flight, valid while kPending
    ULONG m_pending_afd_events;
    // Where the request lifecycle stands
    afd_poll_status m_status;
    // Set by del(); the state is freed when the in-flight request finishes
    bool m_deleted;
    // True while the state sits in the reactor's re-arm queue
    bool m_queued;
};

// Completion key of packets produced by AFD poll requests
constexpr ULONG_PTR kAfdCompletionKey = 0;

// Completion key of the packet posted by interrupt()
constexpr ULONG_PTR kWakeupCompletionKey = 1;

// Concurrency value for CreateIoCompletionPort: 0 means "number of CPUs"
constexpr DWORD kIocpDefaultConcurrency = 0;

// Most completion packets taken from the port by one poll() call
constexpr ULONG kMaxCompletionsPerPoll = static_cast<ULONG>(kMaxEventsPerPoll);

// Longest wait for cancelled requests to drain when the reactor is destroyed
constexpr DWORD kShutdownDrainTimeoutMs = 5000;

// io_reactor backed by an I/O completion port and AFD poll requests
class iocp_reactor final : public io_reactor
{
public:
    // Create the completion port and open the AFD device
    // Parameter ec: set to the failure reason, or cleared on success
    // Return value: the new reactor, or nullptr on failure
    static std::unique_ptr<iocp_reactor> create(std::error_code& ec)
    {
        ec = ensure_network_initialized();
        if (ec)
        {
            return nullptr;
        }
        if (!nt_api_available())
        {
            ec = std::make_error_code(std::errc::function_not_supported);
            log_error("iocp_reactor: ntdll AFD functions not found", ec);
            return nullptr;
        }

        const HANDLE iocp = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0,
                                                     kIocpDefaultConcurrency);
        if (iocp == nullptr)
        {
            ec = win32_error("CreateIoCompletionPort");
            return nullptr;
        }

        const HANDLE afd = open_afd_device(ec);
        if (afd == nullptr)
        {
            ::CloseHandle(iocp);
            return nullptr;
        }

        // Route AFD completions to the port, and skip signalling the file
        // handle as an event, which nobody waits on
        if (::CreateIoCompletionPort(afd, iocp, kAfdCompletionKey, kIocpDefaultConcurrency) == nullptr)
        {
            ec = win32_error("CreateIoCompletionPort(afd)");
            ::CloseHandle(afd);
            ::CloseHandle(iocp);
            return nullptr;
        }
        if (!::SetFileCompletionNotificationModes(afd, FILE_SKIP_SET_EVENT_ON_HANDLE))
        {
            ec = win32_error("SetFileCompletionNotificationModes");
            ::CloseHandle(afd);
            ::CloseHandle(iocp);
            return nullptr;
        }

        ec.clear();
        return std::unique_ptr<iocp_reactor>(new iocp_reactor(iocp, afd));
    }

    // Cancel every in-flight request and wait for the kernel to release the
    // request buffers before freeing them. No other thread may use the
    // reactor during destruction.
    ~iocp_reactor() override
    {
        if (m_pending_requests > 0)
        {
            ::CancelIoEx(m_afd, nullptr);
        }

        while (m_pending_requests > 0)
        {
            OVERLAPPED_ENTRY entries[kMaxCompletionsPerPoll];
            ULONG count = 0;
            if (!::GetQueuedCompletionStatusEx(m_iocp, entries, kMaxCompletionsPerPoll, &count,
                                               kShutdownDrainTimeoutMs, FALSE))
            {
                // The kernel still owns some buffers. Leaking them is the
                // only safe choice; freeing them would corrupt memory.
                log_error("iocp_reactor: drain on shutdown",
                          std::error_code(static_cast<int>(::GetLastError()), std::system_category()));
                leak_states();
                break;
            }
            for (ULONG i = 0; i < count; ++i)
            {
                if (entries[i].lpCompletionKey == kAfdCompletionKey)
                {
                    --m_pending_requests;
                }
            }
        }

        ::CloseHandle(m_afd);
        ::CloseHandle(m_iocp);
    }

    iocp_reactor(const iocp_reactor&) = delete;
    iocp_reactor& operator=(const iocp_reactor&) = delete;

    // See io_reactor::add
    std::error_code add(descriptor_t fd, event_mask interest) override
    {
        if (!is_valid_interest(interest))
        {
            return std::make_error_code(std::errc::invalid_argument);
        }

        SOCKET base = INVALID_SOCKET;
        std::error_code ec = get_base_socket(fd, base);
        if (ec)
        {
            log_error("iocp_reactor::add(base socket)", ec);
            return ec;
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_sockets.find(fd) != m_sockets.end())
        {
            return std::make_error_code(std::errc::file_exists);
        }

        std::unique_ptr<afd_socket_state> state(new afd_socket_state{});
        state->m_socket = fd;
        state->m_base_socket = base;
        state->m_interest = interest;
        state->m_status = afd_poll_status::kIdle;

        // Submit before inserting: if it fails, nothing is in flight and the
        // state can simply be dropped
        ec = update_state(*state);
        if (ec)
        {
            return ec;
        }
        m_sockets.emplace(fd, std::move(state));
        return std::error_code();
    }

    // See io_reactor::mod
    std::error_code mod(descriptor_t fd, event_mask interest) override
    {
        if (!is_valid_interest(interest))
        {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_sockets.find(fd);
        if (it == m_sockets.end())
        {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        it->second->m_interest = interest;
        return update_state(*it->second);
    }

    // See io_reactor::del
    std::error_code del(descriptor_t fd) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_sockets.find(fd);
        if (it == m_sockets.end())
        {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }

        std::unique_ptr<afd_socket_state> state = std::move(it->second);
        m_sockets.erase(it);
        unqueue(*state);

        if (state->m_status == afd_poll_status::kIdle)
        {
            // Nothing in flight; the memory can go now
            return std::error_code();
        }

        // The kernel still owns the buffers: cancel, and keep the state
        // alive until the completion packet arrives
        cancel_request(*state);
        state->m_deleted = true;
        afd_socket_state* const key = state.get();
        m_zombies.emplace(key, std::move(state));
        return std::error_code();
    }

    // See io_reactor::poll
    std::size_t poll(event_table& out, int timeout_ms, std::error_code& ec) override
    {
        out.clear();
        const bool infinite = timeout_ms < 0;
        const std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(infinite ? 0 : timeout_ms);

        // Some completion packets are pure book-keeping (a cancel issued by
        // mod(), or a request whose events were filtered out). They produce
        // no event, so keep waiting for the rest of the timeout instead of
        // returning an empty result early.
        for (;;)
        {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                rearm_queued(out);
            }

            const DWORD wait_ms = compute_wait(out.empty(), infinite, deadline);
            OVERLAPPED_ENTRY entries[kMaxCompletionsPerPoll];
            ULONG count = 0;
            if (!::GetQueuedCompletionStatusEx(m_iocp, entries, kMaxCompletionsPerPoll, &count,
                                               wait_ms, FALSE))
            {
                const DWORD error = ::GetLastError();
                if (error == WAIT_TIMEOUT)
                {
                    ec.clear();
                    return out.size();
                }
                ec = std::error_code(static_cast<int>(error), std::system_category());
                log_error("GetQueuedCompletionStatusEx", ec);
                return out.size();
            }

            bool interrupted = false;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                for (ULONG i = 0; i < count; ++i)
                {
                    if (entries[i].lpCompletionKey == kWakeupCompletionKey)
                    {
                        // Allow the next interrupt() to post a new packet
                        m_interrupt_pending.store(false);
                        interrupted = true;
                        continue;
                    }
                    handle_completion(*reinterpret_cast<afd_socket_state*>(entries[i].lpOverlapped),
                                      out);
                }
            }

            if (!out.empty() || interrupted)
            {
                ec.clear();
                return out.size();
            }
        }
    }

    // See io_reactor::interrupt
    void interrupt() noexcept override
    {
        // Only one wake-up packet may be outstanding at a time
        if (!m_interrupt_pending.exchange(true))
        {
            if (!::PostQueuedCompletionStatus(m_iocp, 0, kWakeupCompletionKey, nullptr))
            {
                m_interrupt_pending.store(false);
                log_error("PostQueuedCompletionStatus",
                          std::error_code(static_cast<int>(::GetLastError()), std::system_category()));
            }
        }
    }

    // See io_reactor::name
    const char* name() const noexcept override
    {
        return "iocp-afd";
    }

private:
    // Take ownership of the port and the AFD handle; use create() instead
    // Parameter iocp: completion port
    // Parameter afd:  AFD device handle already bound to iocp
    iocp_reactor(HANDLE iocp, HANDLE afd) noexcept
        : m_iocp(iocp)
        , m_afd(afd)
        , m_pending_requests(0)
        , m_interrupt_pending(false)
    {
    }

    // Work out how long the next GetQueuedCompletionStatusEx call may block
    // Parameter nothing_to_report: true if poll() has no event to return yet
    // Parameter infinite:          true if the caller asked for no timeout
    // Parameter deadline:          when the caller's timeout expires
    // Return value: wait in milliseconds, or INFINITE
    static DWORD compute_wait(bool nothing_to_report, bool infinite,
                              std::chrono::steady_clock::time_point deadline) noexcept
    {
        // Events (e.g. a failed re-arm) are already waiting: only collect
        // packets that are ready right now
        if (!nothing_to_report)
        {
            return 0;
        }
        if (infinite)
        {
            return INFINITE;
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        return remaining.count() > 0 ? static_cast<DWORD>(remaining.count()) : 0;
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

    // Open a handle to the AFD driver for submitting poll requests
    // Parameter ec: set to the failure reason, or cleared on success
    // Return value: the device handle, or nullptr on failure
    static HANDLE open_afd_device(std::error_code& ec) noexcept
    {
        // Any name under \Device\Afd\ opens the driver; the suffix only
        // makes the handle recognisable in tools such as Process Explorer
        wchar_t device_name[] = L"\\Device\\Afd\\SpanIO";
        UNICODE_STRING name;
        name.Buffer = device_name;
        name.Length = static_cast<USHORT>(sizeof(device_name) - sizeof(wchar_t));
        name.MaximumLength = static_cast<USHORT>(sizeof(device_name));

        OBJECT_ATTRIBUTES attributes{};
        attributes.Length = static_cast<ULONG>(sizeof(attributes));
        attributes.ObjectName = &name;

        IO_STATUS_BLOCK iosb{};
        HANDLE afd = nullptr;
        const NTSTATUS status = get_nt_api().m_create_file(
            &afd, SYNCHRONIZE, &attributes, &iosb, nullptr, 0,
            FILE_SHARE_READ | FILE_SHARE_WRITE, FILE_OPEN, 0, nullptr, 0);
        if (!nt_success(status))
        {
            ec = nt_status_to_error(status);
            log_error("NtCreateFile(\\Device\\Afd)", ec);
            return nullptr;
        }
        ec.clear();
        return afd;
    }

    // Translate an interest mask to AFD poll events
    // Parameter interest: kEventRead / kEventWrite combination
    // Return value: AFD events to request; errors, aborts and local closes
    //               are always included
    static ULONG to_afd_events(event_mask interest) noexcept
    {
        ULONG events = kAfdPollAbort | kAfdPollConnectFail | kAfdPollLocalClose;
        if ((interest & kEventRead) != 0)
        {
            events |= kAfdPollReceive | kAfdPollAccept | kAfdPollDisconnect;
        }
        if ((interest & kEventWrite) != 0)
        {
            events |= kAfdPollSend;
        }
        return events;
    }

    // Translate AFD events of a completed request to kEvent* bits, using the
    // same mapping as wepoll so behaviour matches epoll as closely as possible
    // Parameter events: AFD events that occurred
    // Return value: matching kEvent* combination
    static event_mask from_afd_events(ULONG events) noexcept
    {
        event_mask result = kEventNone;
        if ((events & (kAfdPollReceive | kAfdPollAccept | kAfdPollDisconnect
                       | kAfdPollReceiveExpedited)) != 0)
        {
            result |= kEventRead;
        }
        if ((events & kAfdPollSend) != 0)
        {
            result |= kEventWrite;
        }
        if ((events & kAfdPollAbort) != 0)
        {
            // Reading then returns the reset error, as on Linux
            result |= kEventRead | kEventHangup;
        }
        if ((events & kAfdPollConnectFail) != 0)
        {
            // Linux reports all of these after a failed connect
            result |= kEventRead | kEventWrite | kEventError;
        }
        return result;
    }

    // Submit an AFD poll request for the state's current interest. Caller
    // holds m_mutex and the state must be kIdle.
    // Parameter state: socket to poll
    // Return value: empty on success, otherwise the error
    std::error_code submit_request(afd_socket_state& state)
    {
        const ULONG afd_events = to_afd_events(state.m_interest);

        state.m_poll_info.m_timeout.QuadPart = LLONG_MAX;
        state.m_poll_info.m_handle_count = kAfdPollHandleCount;
        state.m_poll_info.m_exclusive = FALSE;
        state.m_poll_info.m_handles[0].m_handle = reinterpret_cast<HANDLE>(state.m_base_socket);
        state.m_poll_info.m_handles[0].m_events = afd_events;
        state.m_poll_info.m_handles[0].m_status = kStatusSuccess;
        // Lets cancel_request() tell whether the kernel finished already
        state.m_iosb.Status = kStatusPending;

        // The state pointer is passed as the APC context; it comes back as
        // OVERLAPPED_ENTRY::lpOverlapped in the completion packet. Even an
        // immediate STATUS_SUCCESS still posts a packet, because the handle
        // does not use FILE_SKIP_COMPLETION_PORT_ON_SUCCESS.
        const NTSTATUS status = get_nt_api().m_device_io_control_file(
            m_afd, nullptr, nullptr, &state, &state.m_iosb, kIoctlAfdPoll,
            &state.m_poll_info, static_cast<ULONG>(sizeof(state.m_poll_info)),
            &state.m_poll_info, static_cast<ULONG>(sizeof(state.m_poll_info)));
        if (status != kStatusSuccess && status != kStatusPending)
        {
            const std::error_code ec = nt_status_to_error(status);
            log_error("IOCTL_AFD_POLL", ec);
            return ec;
        }

        state.m_status = afd_poll_status::kPending;
        state.m_pending_afd_events = afd_events;
        ++m_pending_requests;
        return std::error_code();
    }

    // Ask the kernel to cancel the state's in-flight request. Its completion
    // packet (STATUS_CANCELLED or a real result) still arrives later.
    // Caller holds m_mutex.
    // Parameter state: socket whose request is cancelled
    void cancel_request(afd_socket_state& state)
    {
        state.m_status = afd_poll_status::kCancelled;
        if (state.m_iosb.Status != kStatusPending)
        {
            // Already completed; the packet is waiting in the port
            return;
        }
        IO_STATUS_BLOCK cancel_iosb{};
        const NTSTATUS status = get_nt_api().m_cancel_io_file_ex(m_afd, &state.m_iosb, &cancel_iosb);
        if (!nt_success(status) && status != kStatusNotFound)
        {
            log_error("NtCancelIoFileEx", nt_status_to_error(status));
        }
    }

    // Bring the in-flight request in line with the state's interest.
    // Caller holds m_mutex.
    // Parameter state: socket to update
    // Return value: empty on success, otherwise the submit error
    std::error_code update_state(afd_socket_state& state)
    {
        switch (state.m_status)
        {
        case afd_poll_status::kPending:
            if (state.m_pending_afd_events != to_afd_events(state.m_interest))
            {
                cancel_request(state);
            }
            return std::error_code();
        case afd_poll_status::kCancelled:
            // Resubmitted with the new interest once the cancel completes
            return std::error_code();
        case afd_poll_status::kIdle:
        default:
            return submit_request(state);
        }
    }

    // Process one AFD completion packet. Caller holds m_mutex.
    // Parameter state: socket whose request completed
    // Parameter out:   receives the socket's events, if any
    void handle_completion(afd_socket_state& state, event_table& out)
    {
        --m_pending_requests;
        const afd_poll_status previous = state.m_status;
        state.m_status = afd_poll_status::kIdle;

        if (state.m_deleted)
        {
            m_zombies.erase(&state);
            return;
        }

        const NTSTATUS status = state.m_iosb.Status;
        if (previous == afd_poll_status::kCancelled || status == kStatusCancelled)
        {
            // Cancelled to change the interest: watch again right away
            rearm_now(state, out);
            return;
        }

        event_mask events = kEventNone;
        if (!nt_success(status))
        {
            events = kEventError;
        }
        else if (state.m_poll_info.m_handle_count >= kAfdPollHandleCount)
        {
            const ULONG afd_events = state.m_poll_info.m_handles[0].m_events;
            if ((afd_events & kAfdPollLocalClose) != 0)
            {
                // Closed without del(): the handle value may be reused by a
                // new socket, so forget this registration like epoll does
                forget(state);
                return;
            }
            events = from_afd_events(afd_events) & (state.m_interest | kAlwaysReported);
        }

        if (events == kEventNone)
        {
            rearm_now(state, out);
            return;
        }

        out.push_back(ready_event{state.m_socket, events});
        // Level-triggered: poll again on the next poll() call, which reports
        // the socket again if it is still ready
        if (!state.m_queued)
        {
            state.m_queued = true;
            m_rearm_queue.push_back(&state);
        }
    }

    // Submit a fresh request at once; report kEventError if that fails.
    // Caller holds m_mutex.
    // Parameter state: idle socket to watch again
    // Parameter out:   receives an error event if the submit fails
    void rearm_now(afd_socket_state& state, event_table& out)
    {
        if (submit_request(state))
        {
            out.push_back(ready_event{state.m_socket, kEventError});
        }
    }

    // Submit requests for every socket that reported events in the previous
    // poll() call. Caller holds m_mutex.
    // Parameter out: receives an error event for each failed submit
    void rearm_queued(event_table& out)
    {
        for (afd_socket_state* state : m_rearm_queue)
        {
            state->m_queued = false;
            if (update_state(*state))
            {
                out.push_back(ready_event{state->m_socket, kEventError});
            }
        }
        m_rearm_queue.clear();
    }

    // Remove a state from the re-arm queue. Caller holds m_mutex.
    // Parameter state: state that is being removed
    void unqueue(afd_socket_state& state)
    {
        if (state.m_queued)
        {
            m_rearm_queue.erase(std::remove(m_rearm_queue.begin(), m_rearm_queue.end(), &state),
                                m_rearm_queue.end());
            state.m_queued = false;
        }
    }

    // Drop the registration of a socket that was closed without del().
    // Caller holds m_mutex; the state must be idle.
    // Parameter state: state to free; invalid after the call
    void forget(afd_socket_state& state)
    {
        unqueue(state);
        const auto it = m_sockets.find(state.m_socket);
        if (it != m_sockets.end() && it->second.get() == &state)
        {
            m_sockets.erase(it);
        }
    }

    // Give up ownership of every state without freeing it. Used only when
    // the kernel may still write to them during shutdown.
    void leak_states() noexcept
    {
        for (auto& entry : m_sockets)
        {
            entry.second.release();
        }
        for (auto& entry : m_zombies)
        {
            entry.second.release();
        }
    }

    // Completion port that receives AFD poll results and wake-up packets
    HANDLE m_iocp;
    // \Device\Afd handle that all poll requests are submitted on
    HANDLE m_afd;
    // Guards every member below except m_interrupt_pending
    std::mutex m_mutex;
    // Registered sockets, keyed by the handle the caller passed to add()
    std::unordered_map<descriptor_t, std::unique_ptr<afd_socket_state>> m_sockets;
    // States removed by del() whose cancelled request has not completed yet
    std::unordered_map<afd_socket_state*, std::unique_ptr<afd_socket_state>> m_zombies;
    // Sockets that reported events and must be polled again on the next
    // poll() call (level-triggered emulation)
    std::vector<afd_socket_state*> m_rearm_queue;
    // Requests submitted whose completion packet has not been processed
    std::size_t m_pending_requests;
    // True while a wake-up packet posted by interrupt() is in the port
    std::atomic<bool> m_interrupt_pending;
};

}}

#endif // SPANIO_PLATFORM_WINDOWS

#endif // SPANIO_DETAIL_IOCP_REACTOR_H
