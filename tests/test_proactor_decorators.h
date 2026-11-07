// ============================================================================
// File purpose: Proactor decorators shared by the tests. A decorator wraps a
//               real backend and changes how some operations start, to force
//               situations that real sockets rarely produce.
//               - holding_proactor: holds datagram sends until released, so
//                 a send queue can fill up
// Usage:        holding_proactor* holder = new holding_proactor(std::move(inner));
//               io_context ctx(spanio::detail::io_proactor_ptr(holder));
//               ...
//               holder->release();
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#ifndef SPANIO_TESTS_TEST_PROACTOR_DECORATORS_H
#define SPANIO_TESTS_TEST_PROACTOR_DECORATORS_H

#include "spanio/detail/io_proactor.h"

#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

namespace spanio { namespace test {

// Proactor decorator that can hold back datagram sends until the test lets
// them go. A real UDP send almost never waits, so without this the send
// queue would never fill up. Held sends are released before a socket is
// unregistered, so each operation still comes back exactly once.
class holding_proactor final : public spanio::detail::io_proactor
{
public:
    // Parameter inner: real backend that performs the I/O
    explicit holding_proactor(spanio::detail::io_proactor_ptr inner)
        : m_inner(std::move(inner))
        , m_holding(true)
    {
    }

    // Stop holding sends and start those held so far
    void release()
    {
        std::vector<spanio::detail::io_operation*> held;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_holding = false;
            held.swap(m_held);
        }
        for (spanio::detail::io_operation* op : held)
        {
            m_inner->start(op);
        }
    }

    // Forwarded to the real backend
    // Parameter handle: socket to register
    // Return value: the backend's result
    std::error_code register_handle(detail::native_handle_t handle) override
    {
        return m_inner->register_handle(handle);
    }

    // Release held sends, then forward to the real backend
    // Parameter handle: socket to unregister
    // Return value: the backend's result
    std::error_code unregister_handle(detail::native_handle_t handle) override
    {
        release();
        return m_inner->unregister_handle(handle);
    }

    // Hold datagram sends while holding; forward everything else
    // Parameter op: operation to start
    void start(spanio::detail::io_operation* op) override
    {
        if (op->m_kind == spanio::detail::op_kind::kSendTo)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_holding)
            {
                m_held.push_back(op);
                return;
            }
        }
        m_inner->start(op);
    }

    // Forwarded to the real backend
    // Parameter handle: socket whose operations to cancel
    void cancel(detail::native_handle_t handle) override
    {
        m_inner->cancel(handle);
    }

    // Forwarded to the real backend
    // Parameter op: operation to queue
    void post(spanio::detail::io_operation* op) override
    {
        m_inner->post(op);
    }

    // Forwarded to the real backend
    // Parameter out:        receives finished operations
    // Parameter timeout_ms: maximum wait
    // Parameter ec:         receives the failure reason
    // Return value: number of finished operations
    std::size_t poll(spanio::detail::completion_list& out, int timeout_ms, std::error_code& ec) override
    {
        return m_inner->poll(out, timeout_ms, ec);
    }

    // Forwarded to the real backend
    void interrupt() noexcept override
    {
        m_inner->interrupt();
    }

    // Name of the wrapped backend
    // Return value: static string
    const char* name() const noexcept override
    {
        return m_inner->name();
    }

private:
    // Real backend doing the I/O
    spanio::detail::io_proactor_ptr m_inner;
    // Guards the fields below
    std::mutex m_mutex;
    // Sends are being held
    bool m_holding;
    // Sends held so far
    std::vector<spanio::detail::io_operation*> m_held;
};

}}

#endif // SPANIO_TESTS_TEST_PROACTOR_DECORATORS_H
