// ============================================================================
// File purpose: Building blocks of the io_context task queue:
//               - scheduler_op:  intrusive, non-virtual task node that is
//                                either run or destroyed exactly once
//               - op_queue:      intrusive FIFO of scheduler_op nodes
//               - handler_op:    task that calls a user function object
//               - io_op_base / io_op: task that is also an io_operation, so
//                                one heap object travels through the
//                                proactor and then through the task queue
//               - io_result:     what an I/O completion handler receives
// Usage:        Internal to io_context and the I/O objects built on it.
//               Allocate a node with make_handler_op() / make_io_op(), hand
//               it to io_context, and never touch it again: io_context calls
//               complete(true) to run it or complete(false) to discard it,
//               and either call frees the node.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_DETAIL_SCHEDULER_OP_H
#define SPANIO_DETAIL_SCHEDULER_OP_H

#include "spanio/detail/io_proactor.h"
#include "spanio/detail/platform.h"
#include "spanio/detail/socket_ops.h"

#include <cstddef>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>

namespace spanio { namespace detail {

// One entry of the io_context task queue. Dispatch goes through a function
// pointer instead of a virtual function so derived nodes can also derive
// from io_operation without a vtable getting in the way.
class scheduler_op
{
public:
    // Run or discard the node; both free it, so the pointer is dangling
    // afterwards
    // Parameter invoke: true runs the user code, false only destroys it
    void complete(bool invoke)
    {
        m_func(this, invoke);
    }

    // Next node in an op_queue; nullptr when last or not queued
    scheduler_op* m_next;

protected:
    // Signature of the per-type completion function
    using func_type = void (*)(scheduler_op* op, bool invoke);

    // Parameter func: completion function of the concrete type
    explicit scheduler_op(func_type func) noexcept
        : m_next(nullptr)
        , m_func(func)
    {
    }

    // Not virtual: nodes are only deleted through their concrete type
    ~scheduler_op() = default;

private:
    // Completion function of the concrete type; it frees the node
    func_type m_func;
};

// Intrusive first-in first-out queue of scheduler_op nodes. It does not own
// the nodes: whoever drains it must complete() each one.
class op_queue
{
public:
    op_queue() noexcept
        : m_front(nullptr)
        , m_back(nullptr)
    {
    }

    op_queue(const op_queue&) = delete;
    op_queue& operator=(const op_queue&) = delete;

    // Whether the queue holds no node
    // Return value: true when empty
    bool empty() const noexcept
    {
        return m_front == nullptr;
    }

    // Append a node at the back
    // Parameter op: node that is not in any queue
    void push(scheduler_op* op) noexcept
    {
        op->m_next = nullptr;
        if (m_back == nullptr)
        {
            m_front = op;
        }
        else
        {
            m_back->m_next = op;
        }
        m_back = op;
    }

    // Move every node of another queue to the back of this one
    // Parameter other: source queue; empty afterwards
    void splice(op_queue& other) noexcept
    {
        if (other.m_front == nullptr)
        {
            return;
        }
        if (m_back == nullptr)
        {
            m_front = other.m_front;
        }
        else
        {
            m_back->m_next = other.m_front;
        }
        m_back = other.m_back;
        other.m_front = nullptr;
        other.m_back = nullptr;
    }

    // Remove the front node
    // Return value: the former front node, or nullptr when empty
    scheduler_op* pop() noexcept
    {
        scheduler_op* op = m_front;
        if (op != nullptr)
        {
            m_front = op->m_next;
            if (m_front == nullptr)
            {
                m_back = nullptr;
            }
            op->m_next = nullptr;
        }
        return op;
    }

private:
    // Oldest node, popped first
    scheduler_op* m_front;
    // Newest node, where push() appends
    scheduler_op* m_back;
};

// Task node that calls a function object with no arguments
template <typename Handler>
class handler_op : public scheduler_op
{
public:
    // Parameter handler: function object to call; moved into the node
    template <typename H>
    explicit handler_op(H&& handler)
        : scheduler_op(&handler_op::do_complete)
        , m_handler(std::forward<H>(handler))
    {
    }

private:
    // Free the node, then call the handler if asked to. The node is freed
    // first so a throwing handler leaks nothing.
    // Parameter base:   this node
    // Parameter invoke: whether to call the handler
    static void do_complete(scheduler_op* base, bool invoke)
    {
        std::unique_ptr<handler_op> self(static_cast<handler_op*>(base));
        if (!invoke)
        {
            return;
        }
        Handler handler(std::move(self->m_handler));
        self.reset();
        handler();
    }

    // Function object called when the node runs
    Handler m_handler;
};

// Allocate a task node for a function object
// Parameter handler: function object taking no arguments
// Return value: the new node, owned by the caller until handed over
template <typename Handler>
scheduler_op* make_handler_op(Handler&& handler)
{
    return new handler_op<typename std::decay<Handler>::type>(std::forward<Handler>(handler));
}

// Result of a finished I/O operation, as passed to its completion handler
struct io_result
{
    // Empty on success; errc::operation_canceled when cancelled
    std::error_code m_ec;
    // Bytes transferred by recv / send; 0 for other kinds
    std::size_t m_bytes;
    // Accepted socket for an accept, now owned by the handler; otherwise
    // kInvalidHandle
    native_handle_t m_accepted;
};

// Task node that is also an io_operation. The proactor sees the
// io_operation part; after poll() returns it, io_context static_casts it
// back and queues the scheduler_op part.
class io_op_base : public io_operation, public scheduler_op
{
protected:
    // Parameter func: completion function of the concrete type
    explicit io_op_base(func_type func) noexcept
        : io_operation()
        , scheduler_op(func)
    {
    }
};

// I/O task node whose completion calls Handler with an io_result
template <typename Handler>
class io_op : public io_op_base
{
public:
    // Parameter handler: function object taking (const io_result&)
    template <typename H>
    explicit io_op(H&& handler)
        : io_op_base(&io_op::do_complete)
        , m_handler(std::forward<H>(handler))
    {
    }

private:
    // Free the node, then call the handler with the result if asked to. A
    // discarded accept closes the socket nobody will ever own.
    // Parameter base:   this node
    // Parameter invoke: whether to call the handler
    static void do_complete(scheduler_op* base, bool invoke)
    {
        std::unique_ptr<io_op> self(static_cast<io_op*>(base));
        const io_result result{self->m_ec, self->m_bytes, self->m_accepted};
        if (!invoke)
        {
            if (result.m_accepted != kInvalidHandle)
            {
                close_socket(result.m_accepted);
            }
            return;
        }
        Handler handler(std::move(self->m_handler));
        self.reset();
        handler(result);
    }

    // Function object called with the result
    Handler m_handler;
};

// Allocate an I/O task node. Fill it with a prepare_*() helper, then give it
// to io_context::start_io().
// Parameter handler: function object taking (const io_result&)
// Return value: the new node, owned by the caller until handed over
template <typename Handler>
io_op_base* make_io_op(Handler&& handler)
{
    return new io_op<typename std::decay<Handler>::type>(std::forward<Handler>(handler));
}

}}

#endif // SPANIO_DETAIL_SCHEDULER_OP_H
