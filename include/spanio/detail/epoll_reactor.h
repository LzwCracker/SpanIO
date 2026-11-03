// ============================================================================
// File purpose: Linux io_reactor backend built on epoll in level-triggered
//               mode, with an eventfd used to wake a blocked poll().
// Usage:        Create through make_platform_reactor() or
//               epoll_reactor::create(ec). Compiled only on Linux; on other
//               platforms this header is empty.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_DETAIL_EPOLL_REACTOR_H
#define SPANIO_DETAIL_EPOLL_REACTOR_H

#include "spanio/detail/io_reactor.h"
#include "spanio/detail/socket_ops.h"

#if defined(SPANIO_PLATFORM_LINUX)

#include <sys/epoll.h>
#include <sys/eventfd.h>

#include <cstdint>
#include <memory>

namespace spanio { namespace detail {

// Initial counter value of the wake-up eventfd (nothing pending)
constexpr unsigned int kEventFdInitialValue = 0;

// Value added to the eventfd counter by interrupt()
constexpr std::uint64_t kEventFdWakeValue = 1;

// io_reactor backed by a level-triggered epoll instance
class epoll_reactor final : public io_reactor
{
public:
    // Create the epoll instance and its wake-up eventfd
    // Parameter ec: set to the failure reason, or cleared on success
    // Return value: the new reactor, or nullptr on failure
    static std::unique_ptr<epoll_reactor> create(std::error_code& ec)
    {
        const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd == kSocketCallFailed)
        {
            ec = last_error();
            log_error("epoll_create1", ec);
            return nullptr;
        }

        // Non-blocking so draining it in poll() can never stall the loop
        const int wakeup_fd = ::eventfd(kEventFdInitialValue, EFD_CLOEXEC | EFD_NONBLOCK);
        if (wakeup_fd == kSocketCallFailed)
        {
            ec = last_error();
            log_error("eventfd", ec);
            ::close(epoll_fd);
            return nullptr;
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = wakeup_fd;
        if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wakeup_fd, &ev) == kSocketCallFailed)
        {
            ec = last_error();
            log_error("epoll_ctl(wakeup)", ec);
            ::close(wakeup_fd);
            ::close(epoll_fd);
            return nullptr;
        }

        ec.clear();
        return std::unique_ptr<epoll_reactor>(new epoll_reactor(epoll_fd, wakeup_fd));
    }

    // Close the epoll instance and the eventfd. Sockets still registered are
    // simply forgotten; they are not closed.
    ~epoll_reactor() override
    {
        ::close(m_wakeup_fd);
        ::close(m_epoll_fd);
    }

    epoll_reactor(const epoll_reactor&) = delete;
    epoll_reactor& operator=(const epoll_reactor&) = delete;

    // See io_reactor::add
    std::error_code add(descriptor_t fd, event_mask interest) override
    {
        return control(EPOLL_CTL_ADD, fd, interest, "epoll_reactor::add");
    }

    // See io_reactor::mod
    std::error_code mod(descriptor_t fd, event_mask interest) override
    {
        return control(EPOLL_CTL_MOD, fd, interest, "epoll_reactor::mod");
    }

    // See io_reactor::del
    std::error_code del(descriptor_t fd) override
    {
        // Kernels before 2.6.9 require a non-null event even for DEL
        epoll_event unused{};
        if (::epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, &unused) == kSocketCallFailed)
        {
            const std::error_code ec = last_error();
            log_error("epoll_reactor::del", ec);
            return ec;
        }
        return std::error_code();
    }

    // See io_reactor::poll
    std::size_t poll(event_table& out, int timeout_ms, std::error_code& ec) override
    {
        out.clear();
        epoll_event events[kMaxEventsPerPoll];
        const int count = ::epoll_wait(m_epoll_fd, events,
                                       static_cast<int>(kMaxEventsPerPoll), timeout_ms);
        if (count == kSocketCallFailed)
        {
            const std::error_code wait_ec = last_error();
            // A signal handler ran; report "no events" and let the caller loop
            if (wait_ec.value() == EINTR)
            {
                ec.clear();
                return 0;
            }
            ec = wait_ec;
            log_error("epoll_wait", ec);
            return 0;
        }

        for (int i = 0; i < count; ++i)
        {
            if (events[i].data.fd == m_wakeup_fd)
            {
                drain_wakeup();
                continue;
            }
            out.push_back(ready_event{events[i].data.fd, from_epoll_events(events[i].events)});
        }

        ec.clear();
        return out.size();
    }

    // See io_reactor::interrupt
    void interrupt() noexcept override
    {
        // EAGAIN only happens when the counter is saturated, which means a
        // wake-up is already pending, so the result can be ignored
        const ssize_t written = ::write(m_wakeup_fd, &kEventFdWakeValue, sizeof(kEventFdWakeValue));
        (void)written;
    }

    // See io_reactor::name
    const char* name() const noexcept override
    {
        return "epoll";
    }

private:
    // Take ownership of already created descriptors; use create() instead
    // Parameter epoll_fd:  epoll instance
    // Parameter wakeup_fd: eventfd already registered for EPOLLIN
    epoll_reactor(int epoll_fd, int wakeup_fd) noexcept
        : m_epoll_fd(epoll_fd)
        , m_wakeup_fd(wakeup_fd)
    {
    }

    // Shared implementation of add() and mod()
    // Parameter op:       EPOLL_CTL_ADD or EPOLL_CTL_MOD
    // Parameter fd:       socket to register or update
    // Parameter interest: requested events
    // Parameter what:     operation name for the log
    // Return value: empty on success, otherwise the error
    std::error_code control(int op, descriptor_t fd, event_mask interest, const char* what)
    {
        if (!is_valid_interest(interest))
        {
            return std::make_error_code(std::errc::invalid_argument);
        }

        epoll_event ev{};
        ev.events = to_epoll_events(interest);
        ev.data.fd = fd;
        if (::epoll_ctl(m_epoll_fd, op, fd, &ev) == kSocketCallFailed)
        {
            // EEXIST and ENOENT already match the errc values the
            // io_reactor contract promises
            const std::error_code ec = last_error();
            log_error(what, ec);
            return ec;
        }
        return std::error_code();
    }

    // Reset the eventfd counter so the level-triggered wake-up stops firing
    void drain_wakeup() noexcept
    {
        std::uint64_t value = 0;
        const ssize_t read_bytes = ::read(m_wakeup_fd, &value, sizeof(value));
        (void)read_bytes;
    }

    // Translate an interest mask to epoll flags
    // Parameter interest: kEventRead / kEventWrite combination
    // Return value: matching EPOLL* flags (level-triggered)
    static std::uint32_t to_epoll_events(event_mask interest) noexcept
    {
        std::uint32_t events = 0;
        if ((interest & kEventRead) != 0)
        {
            // EPOLLRDHUP reports a peer half-close even before data is read
            events |= EPOLLIN | EPOLLRDHUP;
        }
        if ((interest & kEventWrite) != 0)
        {
            events |= EPOLLOUT;
        }
        return events;
    }

    // Translate epoll flags returned by epoll_wait to kEvent* bits
    // Parameter events: EPOLL* flags
    // Return value: matching kEvent* combination
    static event_mask from_epoll_events(std::uint32_t events) noexcept
    {
        event_mask result = kEventNone;
        if ((events & (EPOLLIN | EPOLLRDHUP | EPOLLPRI)) != 0)
        {
            result |= kEventRead;
        }
        if ((events & EPOLLOUT) != 0)
        {
            result |= kEventWrite;
        }
        if ((events & EPOLLERR) != 0)
        {
            result |= kEventError;
        }
        if ((events & EPOLLHUP) != 0)
        {
            result |= kEventHangup;
        }
        return result;
    }

    // epoll instance that all sockets are registered with
    int m_epoll_fd;
    // eventfd registered with the epoll instance; interrupt() writes to it
    int m_wakeup_fd;
};

}}

#endif // SPANIO_PLATFORM_LINUX

#endif // SPANIO_DETAIL_EPOLL_REACTOR_H
