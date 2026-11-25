// ============================================================================
// File purpose: TLS over tcp::socket with OpenSSL 3.0 or later (design doc
//               6.12): ssl::context holds certificates and settings,
//               ssl::stream encrypts one connection. OpenSSL only works on
//               memory buffers; the encrypted bytes travel through the normal
//               tcp::socket operations, so every backend works unchanged.
// Usage:        Needs SPANIO_ENABLE_SSL (CMake option) and OpenSSL.
//                   spanio::ssl::context tls(spanio::ssl::role::client);
//                   std::error_code ec = tls.set_default_verify_paths();
//                   spanio::ssl::stream stream(std::move(socket), tls);
//                   ec = stream.set_verify_hostname("example.org");
//                   stream.async_handshake(on_handshake);
//
//               Rules:
//               - Every async_* function takes a completion token. Handlers
//                 never run inside the call that starts the operation.
//               - At most one read, one write, one handshake and one shutdown
//                 may be pending; a second one of a kind fails with
//                 errc::operation_in_progress. A read and a write may be
//                 pending together.
//               - A stream locks itself, so operations may be started from
//                 any thread without a strand. Moving or destroying the
//                 stream object itself must not race with other calls.
//               - A context must be fully set up before the first stream is
//                 created from it.
//               - Clients verify the server certificate by default; also call
//                 set_verify_hostname(), or any trusted certificate passes.
//               - A connection that ends without the TLS close message
//                 reports error::ssl_errors::stream_truncated, not eof.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-25
// ============================================================================

#ifndef SPANIO_SSL_H
#define SPANIO_SSL_H

#include "spanio/tcp.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#if OPENSSL_VERSION_NUMBER < 0x30000000L
#error "spanio/ssl.h needs OpenSSL 3.0 or later"
#endif

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace spanio { namespace error {

// TLS conditions that are not OpenSSL error codes
enum class ssl_errors
{
    // The TCP connection ended without the TLS close message, so data may
    // be missing
    stream_truncated = 1,
    // OpenSSL failed without saying why, e.g. the stream already failed
    unspecified_failure
};

// Category of ssl_errors
class ssl_stream_category_impl : public std::error_category
{
public:
    // Name of the category
    // Return value: static string "spanio.ssl.stream"
    const char* name() const noexcept override
    {
        return "spanio.ssl.stream";
    }

    // Human-readable description of an error value
    // Parameter value: an ssl_errors value
    // Return value: description text
    std::string message(int value) const override
    {
        switch (value)
        {
        case static_cast<int>(ssl_errors::stream_truncated):
            return "TLS stream truncated";
        case static_cast<int>(ssl_errors::unspecified_failure):
            return "Unspecified TLS failure";
        default:
            return "spanio.ssl.stream error";
        }
    }
};

// The single instance of the ssl_errors category
// Return value: reference to the category, valid for the whole program
inline const std::error_category& ssl_stream_category() noexcept
{
    static const ssl_stream_category_impl s_category;
    return s_category;
}

// Make an error_code from an ssl_errors value
// Parameter value: the error
// Return value: the error code in the ssl_errors category
inline std::error_code make_error_code(ssl_errors value) noexcept
{
    return std::error_code(static_cast<int>(value), ssl_stream_category());
}

// Category of packed OpenSSL error codes (ERR_get_error)
class ssl_category_impl : public std::error_category
{
public:
    // Name of the category
    // Return value: static string "spanio.ssl"
    const char* name() const noexcept override
    {
        return "spanio.ssl";
    }

    // Human-readable description of an error value
    // Parameter value: packed OpenSSL error code
    // Return value: OpenSSL's text for its reason
    std::string message(int value) const override
    {
        const char* text = ::ERR_reason_error_string(static_cast<unsigned long>(value));
        return text != nullptr ? text : "OpenSSL error";
    }
};

// The single instance of the OpenSSL category
// Return value: reference to the category, valid for the whole program
inline const std::error_category& ssl_category() noexcept
{
    static const ssl_category_impl s_category;
    return s_category;
}

// Category of certificate verification results (X509_V_ERR_* values)
class ssl_verify_category_impl : public std::error_category
{
public:
    // Name of the category
    // Return value: static string "spanio.ssl.verify"
    const char* name() const noexcept override
    {
        return "spanio.ssl.verify";
    }

    // Human-readable description of an error value
    // Parameter value: an X509_V_ERR_* value
    // Return value: OpenSSL's text for it
    std::string message(int value) const override
    {
        return ::X509_verify_cert_error_string(value);
    }
};

// The single instance of the verification category
// Return value: reference to the category, valid for the whole program
inline const std::error_category& ssl_verify_category() noexcept
{
    static const ssl_verify_category_impl s_category;
    return s_category;
}

}}

namespace std {

// Lets ssl_errors convert implicitly to std::error_code
template <>
struct is_error_code_enum<spanio::error::ssl_errors> : true_type
{
};

}

namespace spanio { namespace detail {

// Most encrypted bytes read from the socket at once: one full TLS record
// (16 KiB of data plus its overhead) fits
constexpr std::size_t kSslInputSize = 17 * 1024;

// Most plain bytes encrypted before the result is sent: one TLS record
constexpr std::size_t kSslWriteChunk = 16 * 1024;

// Reading stops while more than this is waiting to be sent, so a peer that
// makes us answer (e.g. with key updates) but never reads cannot use up memory
constexpr std::size_t kSslMaxBacklog = 64 * 1024;

// What most OpenSSL functions return on success
constexpr int kSslOk = 1;

// Frees OpenSSL objects held by std::unique_ptr
struct ssl_deleter
{
    // Parameter value: object to free
    void operator()(SSL* value) const noexcept
    {
        ::SSL_free(value);
    }

    // Parameter value: object to free
    void operator()(SSL_CTX* value) const noexcept
    {
        ::SSL_CTX_free(value);
    }

    // Parameter value: object to free
    void operator()(BIO* value) const noexcept
    {
        ::BIO_free(value);
    }

    // Parameter value: object to free
    void operator()(X509* value) const noexcept
    {
        ::X509_free(value);
    }

    // Parameter value: object to free
    void operator()(EVP_PKEY* value) const noexcept
    {
        ::EVP_PKEY_free(value);
    }
};

// Owning pointer to an OpenSSL object
template <typename T>
using ssl_ptr = std::unique_ptr<T, ssl_deleter>;

// Turn this thread's oldest OpenSSL error into an error code and clear the
// error queue. System errors (a missing file) become std::errc values.
// Return value: never empty
inline std::error_code take_ssl_error() noexcept
{
    const unsigned long code = ::ERR_get_error();
    ::ERR_clear_error();
    if (code == 0)
    {
        return error::ssl_errors::unspecified_failure;
    }
    if (ERR_SYSTEM_ERROR(code))
    {
        return std::error_code(ERR_GET_REASON(code), std::generic_category());
    }
    return std::error_code(static_cast<int>(code), error::ssl_category());
}

// Result of an OpenSSL call that returns 1 on success
// Parameter result: what the call returned
// Return value: empty on success, otherwise the error
inline std::error_code check_ssl(int result) noexcept
{
    if (result == kSslOk)
    {
        ::ERR_clear_error();
        return std::error_code();
    }
    return take_ssl_error();
}

// Password callback that never supplies one: without it OpenSSL would ask on
// the terminal when a private key is encrypted
// Return value: -1, meaning "no password"
inline int no_ssl_password(char* /*buffer*/, int /*size*/, int /*writing*/, void* /*user*/) noexcept
{
    return -1;
}

// Kinds of stream operation; at most one of each is pending. The values
// index the slots and give the order in which operations are tried.
enum class ssl_kind : std::size_t
{
    // async_handshake
    handshake = 0,
    // async_write_some / async_write
    write = 1,
    // async_read_some / async_read
    read = 2,
    // async_shutdown
    shutdown = 3
};

// Number of ssl_kind values
constexpr std::size_t kSslKinds = 4;

// A started stream operation. Derived classes own the user's handler.
class ssl_operation
{
public:
    // Parameter kind: which operation this is
    explicit ssl_operation(ssl_kind kind) noexcept
        : m_kind(kind)
    {
    }

    ssl_operation(const ssl_operation&) = delete;
    ssl_operation& operator=(const ssl_operation&) = delete;
    virtual ~ssl_operation() = default;

    // Queue the handler with the recorded result; called once
    // Parameter context: context that runs it
    virtual void post(io_context& context) = 0;

    // Which operation this is
    ssl_kind m_kind;
    // Destination of a read
    char* m_input = nullptr;
    // Source of a write
    const char* m_output = nullptr;
    // Size of the buffer
    std::size_t m_size = 0;
    // Bytes of the buffer done so far
    std::size_t m_done = 0;
    // Whether the whole buffer must be transferred
    bool m_all = false;
    // OpenSSL needs more bytes from the peer
    bool m_needs_input = false;
    // Waits until m_mark encrypted bytes have been sent
    bool m_flushing = false;
    // Finished: after the flush only the result is left to report
    bool m_finished = false;
    // Amount of encrypted output that must be sent first
    std::uint64_t m_mark = 0;
    // Result
    std::error_code m_ec;
};

// Operation whose handler takes only an error code
template <typename Handler>
class ssl_status_operation : public ssl_operation
{
public:
    // Parameter kind:    handshake or shutdown
    // Parameter handler: function object taking (std::error_code)
    template <typename H>
    ssl_status_operation(ssl_kind kind, H&& handler)
        : ssl_operation(kind)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Queue the handler
    // Parameter context: context that runs it
    void post(io_context& context) override
    {
        context.post(error_binder<Handler>(std::move(m_handler), m_ec));
    }

private:
    // User handler
    Handler m_handler;
};

// Operation whose handler takes an error code and a byte count
template <typename Handler>
class ssl_transfer_operation : public ssl_operation
{
public:
    // Parameter kind:    read or write
    // Parameter handler: function object taking (std::error_code, std::size_t)
    template <typename H>
    ssl_transfer_operation(ssl_kind kind, H&& handler)
        : ssl_operation(kind)
        , m_handler(std::forward<H>(handler))
    {
    }

    // Queue the handler
    // Parameter context: context that runs it
    void post(io_context& context) override
    {
        context.post(transfer_binder<Handler>(std::move(m_handler), m_ec, m_done));
    }

private:
    // User handler
    Handler m_handler;
};

// Owning pointer to a started operation
using ssl_operation_ptr = std::unique_ptr<ssl_operation>;

// State of one TLS connection, shared by the stream and the socket
// operations in flight, all used under m_mutex. Invariant: while a user
// operation is pending, a socket read or write is in flight, so every
// operation completes once, also after close() and context destruction.
class ssl_core : public std::enable_shared_from_this<ssl_core>
{
public:
    // Throws std::system_error if OpenSSL cannot create the connection
    // Parameter socket:   the connection
    // Parameter settings: context to create the connection from
    // Parameter server:   whether this end is the server
    ssl_core(tcp::socket&& socket, SSL_CTX* settings, bool server)
        : m_socket(std::move(socket))
        , m_ssl(::SSL_new(settings))
        , m_input(kSslInputSize)
    {
        ssl_ptr<BIO> from_peer(::BIO_new(::BIO_s_mem()));
        ssl_ptr<BIO> to_peer(::BIO_new(::BIO_s_mem()));
        if (m_ssl == nullptr || from_peer == nullptr || to_peer == nullptr)
        {
            throw std::system_error(take_ssl_error(), "spanio::ssl::stream");
        }
        // An empty buffer means "wait for more", not end of file
        BIO_set_mem_eof_return(from_peer.get(), -1);
        m_from_peer = from_peer.release();
        m_to_peer = to_peer.release();
        ::SSL_set_bio(m_ssl.get(), m_from_peer, m_to_peer);
        if (server)
        {
            ::SSL_set_accept_state(m_ssl.get());
        }
        else
        {
            ::SSL_set_connect_state(m_ssl.get());
        }
    }

    // Start an operation, or queue its failure
    // Parameter op: the new operation
    void start(ssl_operation_ptr op)
    {
        std::vector<ssl_operation_ptr> finished;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ssl_operation_ptr& slot = m_slots[static_cast<std::size_t>(op->m_kind)];
            const bool transfer = op->m_kind == ssl_kind::read || op->m_kind == ssl_kind::write;
            if (!m_socket.is_open())
            {
                op->m_ec = not_open_error();
                finished.push_back(std::move(op));
            }
            else if (slot != nullptr)
            {
                op->m_ec = std::make_error_code(std::errc::operation_in_progress);
                finished.push_back(std::move(op));
            }
            else if (transfer && op->m_size == 0)
            {
                finished.push_back(std::move(op));
            }
            else
            {
                slot = std::move(op);
                pump(finished);
            }
        }
        post_all(finished);
    }

    // Close the socket; pending operations fail with operation_canceled
    void close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_socket.close();
    }

    // Whether the socket is open
    // Return value: true until close()
    bool is_open() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_socket.is_open();
    }

    // Address of the peer
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint remote_endpoint(std::error_code& ec) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_socket.remote_endpoint(ec);
    }

    // Address of this end
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_socket.local_endpoint(ec);
    }

    // Set the name the peer's certificate must carry, also sent as SNI
    // Parameter host: DNS name or numeric address
    // Return value: empty on success
    std::error_code set_verify_hostname(const std::string& host)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        ::ERR_clear_error();
        if (host.empty() || host.find('\0') != std::string::npos)
        {
            // An empty name would switch the check off
            return std::make_error_code(std::errc::invalid_argument);
        }
        X509_VERIFY_PARAM* param = ::SSL_get0_param(m_ssl.get());
        if (::X509_VERIFY_PARAM_set1_ip_asc(param, host.c_str()) == kSslOk)
        {
            // SNI carries names only, never addresses
            return std::error_code();
        }
        ::ERR_clear_error();
        const std::error_code ec = check_ssl(::X509_VERIFY_PARAM_set1_host(param, host.c_str(), host.size()));
        if (ec)
        {
            return ec;
        }
        return check_ssl(static_cast<int>(SSL_set_tlsext_host_name(m_ssl.get(), host.c_str())));
    }

    // Context that runs the handlers
    // Return value: the context
    io_context& context() const noexcept
    {
        return m_socket.context();
    }

    // The OpenSSL connection
    // Return value: never null
    SSL* native_handle() const noexcept
    {
        return m_ssl.get();
    }

    // A socket read finished
    // Parameter ec:    its result
    // Parameter bytes: bytes in m_input
    void on_read(const std::error_code& ec, std::size_t bytes)
    {
        std::vector<ssl_operation_ptr> finished;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_reading = false;
            if (ec)
            {
                // An end of file without the TLS close message is suspicious
                m_read_error = ec == error::misc_errors::eof ? std::error_code(error::ssl_errors::stream_truncated)
                                                             : ec;
            }
            else if (::BIO_write(m_from_peer, m_input.data(), static_cast<int>(bytes)) != static_cast<int>(bytes))
            {
                m_read_error = std::make_error_code(std::errc::not_enough_memory);
            }
            pump(finished);
        }
        post_all(finished);
    }

    // A socket write finished
    // Parameter ec:    its result
    // Parameter bytes: bytes sent
    void on_write(const std::error_code& ec, std::size_t bytes)
    {
        std::vector<ssl_operation_ptr> finished;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_writing = false;
            m_sent += bytes;
            m_sending_offset += bytes;
            if (ec)
            {
                m_write_error = ec;
            }
            pump(finished);
        }
        post_all(finished);
    }

    // A socket operation was destroyed without running: its io_context is
    // being destroyed. Drop the pending operations, because their handlers
    // may own the stream and so keep this object alive for ever.
    void abandon() noexcept
    {
        std::array<ssl_operation_ptr, kSslKinds> dropped;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            dropped.swap(m_slots);
        }
        // Handlers are destroyed here, outside the lock: they may close()
    }

private:
    // Queue the handlers of finished operations; never under m_mutex, since
    // destroying a handler may close the stream
    // Parameter finished: the operations
    void post_all(std::vector<ssl_operation_ptr>& finished)
    {
        for (ssl_operation_ptr& op : finished)
        {
            op->post(context());
        }
    }

    // Move every operation as far as possible, then start the socket
    // operations it takes to continue
    // Parameter finished: receives operations whose handler is due
    void pump(std::vector<ssl_operation_ptr>& finished)
    {
        // One pass is enough: during the handshake the first operation uses
        // all input, so the ones after it see its result
        for (ssl_operation_ptr& slot : m_slots)
        {
            if (slot != nullptr && step(*slot))
            {
                finished.push_back(std::move(slot));
            }
        }
        start_write();
        start_read();
    }

    // Move one operation as far as possible
    // Parameter op: pending operation
    // Return value: true when its handler is due
    bool step(ssl_operation& op)
    {
        for (;;)
        {
            if (op.m_flushing)
            {
                if (m_sent < op.m_mark)
                {
                    return flush_failed(op);
                }
                op.m_flushing = false;
            }
            if (op.m_finished)
            {
                return true;
            }
            if (!m_socket.is_open())
            {
                op.m_ec = std::make_error_code(std::errc::operation_canceled);
                return true;
            }
            if (m_write_error && op.m_kind != ssl_kind::read)
            {
                op.m_ec = m_write_error;
                return true;
            }
            advance(op);
            if (op.m_needs_input)
            {
                if (!m_read_error)
                {
                    return false;
                }
                op.m_ec = m_read_error;
                return true;
            }
        }
    }

    // Whether an operation waiting for a flush has to give up
    // Parameter op: the operation
    // Return value: true when its handler is due
    bool flush_failed(ssl_operation& op)
    {
        std::error_code ec = m_write_error;
        if (!m_socket.is_open())
        {
            ec = std::make_error_code(std::errc::operation_canceled);
        }
        // A TLS error explains more than the lost alert about it
        if (ec && !op.m_ec)
        {
            op.m_ec = ec;
        }
        return static_cast<bool>(ec);
    }

    // Run OpenSSL once for an operation and collect what it wants to send
    // Parameter op: operation that is not waiting for a flush
    void advance(ssl_operation& op)
    {
        ::ERR_clear_error();
        op.m_needs_input = false;
        std::size_t bytes = 0;
        int result = 0;
        bool success = false;
        switch (op.m_kind)
        {
        case ssl_kind::handshake:
            result = ::SSL_do_handshake(m_ssl.get());
            success = result == kSslOk;
            break;
        case ssl_kind::write:
            result = ::SSL_write_ex(m_ssl.get(), op.m_output + op.m_done, write_size(op), &bytes);
            success = result == kSslOk;
            break;
        case ssl_kind::read:
            result = ::SSL_read_ex(m_ssl.get(), op.m_input + op.m_done, op.m_size - op.m_done, &bytes);
            success = result == kSslOk;
            break;
        case ssl_kind::shutdown:
            // 0: our close message is written, the peer's has not arrived
            result = ::SSL_shutdown(m_ssl.get());
            success = result >= 0;
            break;
        }
        if (success)
        {
            op.m_done += bytes;
            const bool transfer = op.m_kind == ssl_kind::read || op.m_kind == ssl_kind::write;
            op.m_finished = !transfer || !op.m_all || op.m_done == op.m_size;
        }
        else
        {
            fail(op, ::SSL_get_error(m_ssl.get(), result));
        }
        collect_output();
        // A read is done once it has data; all others also wait until their
        // output, including the alert that explains an error, has been sent
        if (!op.m_needs_input && op.m_kind != ssl_kind::read)
        {
            op.m_flushing = true;
            op.m_mark = m_produced;
        }
    }

    // Bytes a write hands to OpenSSL at once
    // Parameter op: write operation
    // Return value: at most one TLS record
    static std::size_t write_size(const ssl_operation& op) noexcept
    {
        const std::size_t left = op.m_size - op.m_done;
        return left < kSslWriteChunk ? left : kSslWriteChunk;
    }

    // Record why OpenSSL did not finish an operation
    // Parameter op:     the operation
    // Parameter reason: value of SSL_get_error
    void fail(ssl_operation& op, int reason)
    {
        if (reason == SSL_ERROR_WANT_READ)
        {
            op.m_needs_input = true;
            return;
        }
        op.m_finished = true;
        if (reason == SSL_ERROR_ZERO_RETURN)
        {
            op.m_ec = error::misc_errors::eof;
            return;
        }
        const unsigned long code = ::ERR_peek_error();
        const long verify = ::SSL_get_verify_result(m_ssl.get());
        op.m_ec = take_ssl_error();
        if (ERR_GET_LIB(code) == ERR_LIB_SSL && ERR_GET_REASON(code) == SSL_R_CERTIFICATE_VERIFY_FAILED
            && verify != X509_V_OK)
        {
            // Say what is wrong with the certificate, not just that it is
            op.m_ec = std::error_code(static_cast<int>(verify), error::ssl_verify_category());
        }
        log_error("ssl::stream", op.m_ec);
    }

    // Move what OpenSSL wrote into the output queue
    void collect_output()
    {
        const std::size_t pending = ::BIO_ctrl_pending(m_to_peer);
        if (pending == 0)
        {
            return;
        }
        const std::size_t old_size = m_queued.size();
        m_queued.resize(old_size + pending);
        const int moved = ::BIO_read(m_to_peer, m_queued.data() + old_size, static_cast<int>(pending));
        const std::size_t count = moved > 0 ? static_cast<std::size_t>(moved) : 0;
        m_queued.resize(old_size + count);
        m_produced += count;
    }

    // Send queued output unless a socket write is in flight. One write at a
    // time keeps the records in order.
    void start_write();

    // Read from the socket if an operation needs input
    void start_read();

    // Whether an operation waits for bytes from the peer
    // Return value: true if one does
    bool input_needed() const noexcept
    {
        for (const ssl_operation_ptr& slot : m_slots)
        {
            if (slot != nullptr && slot->m_needs_input)
            {
                return true;
            }
        }
        return false;
    }

    // Guards every member
    mutable std::mutex m_mutex;
    // The connection
    tcp::socket m_socket;
    // OpenSSL's connection; owns the two buffers below
    ssl_ptr<SSL> m_ssl;
    // Encrypted bytes from the peer, read by OpenSSL
    BIO* m_from_peer = nullptr;
    // Encrypted bytes for the peer, written by OpenSSL
    BIO* m_to_peer = nullptr;
    // Destination of the socket read in flight
    std::vector<char> m_input;
    // Output the socket write in flight sends; untouched until it returns
    std::vector<char> m_sending;
    // Bytes of m_sending already sent
    std::size_t m_sending_offset = 0;
    // Output that waits for the write in flight
    std::vector<char> m_queued;
    // Encrypted bytes produced so far
    std::uint64_t m_produced = 0;
    // Encrypted bytes sent so far
    std::uint64_t m_sent = 0;
    // A socket read is in flight
    bool m_reading = false;
    // A socket write is in flight
    bool m_writing = false;
    // Why reading from the socket ended
    std::error_code m_read_error;
    // Why writing to the socket ended
    std::error_code m_write_error;
    // Pending operations, indexed by ssl_kind
    std::array<ssl_operation_ptr, kSslKinds> m_slots;
};

// Handler of the core's socket operations. Destroyed without running, it
// tells the core to drop the pending operations.
class ssl_io_handler
{
public:
    // Parameter core:    the connection
    // Parameter is_read: whether the operation is a read
    ssl_io_handler(std::shared_ptr<ssl_core> core, bool is_read) noexcept
        : m_core(std::move(core))
        , m_is_read(is_read)
    {
    }

    ssl_io_handler(ssl_io_handler&& other) noexcept = default;
    ssl_io_handler& operator=(ssl_io_handler&&) = delete;
    ssl_io_handler(const ssl_io_handler&) = delete;
    ssl_io_handler& operator=(const ssl_io_handler&) = delete;

    // Report an operation that never ran
    ~ssl_io_handler()
    {
        if (m_core != nullptr)
        {
            m_core->abandon();
        }
    }

    // Parameter ec:    result of the operation
    // Parameter bytes: bytes transferred
    void operator()(const std::error_code& ec, std::size_t bytes)
    {
        const std::shared_ptr<ssl_core> core = std::move(m_core);
        if (m_is_read)
        {
            core->on_read(ec, bytes);
        }
        else
        {
            core->on_write(ec, bytes);
        }
    }

private:
    // The connection; null once run or moved from
    std::shared_ptr<ssl_core> m_core;
    // Whether the operation is a read
    bool m_is_read;
};

inline void ssl_core::start_write()
{
    if (m_writing || m_write_error || !m_socket.is_open())
    {
        return;
    }
    if (m_sending_offset == m_sending.size())
    {
        m_sending.clear();
        m_sending_offset = 0;
        m_sending.swap(m_queued);
    }
    if (m_sending.empty())
    {
        return;
    }
    m_writing = true;
    m_socket.async_write_some(const_buffer(m_sending.data() + m_sending_offset, m_sending.size() - m_sending_offset),
                              ssl_io_handler(shared_from_this(), false));
}

inline void ssl_core::start_read()
{
    const std::size_t backlog = m_queued.size() + m_sending.size() - m_sending_offset;
    const bool blocked = m_writing && backlog > kSslMaxBacklog;
    if (m_reading || m_read_error || blocked || !m_socket.is_open() || !input_needed())
    {
        return;
    }
    m_reading = true;
    m_socket.async_read_some(mutable_buffer(m_input.data(), m_input.size()),
                             ssl_io_handler(shared_from_this(), true));
}

}}

namespace spanio { namespace ssl {

// Which end of the connection a context is for
enum class role
{
    // Starts the handshake and verifies the server by default
    client,
    // Answers the handshake; needs a certificate and its private key
    server
};

// What to check about the peer's certificate
enum class verify_mode
{
    // Accept any peer
    none,
    // Verify the peer's certificate; a server accepts a client without one
    peer,
    // Verify the peer's certificate and refuse a client without one
    require_peer
};

// Protocol versions
enum class tls_version
{
    // TLS 1.2
    tls1_2 = TLS1_2_VERSION,
    // TLS 1.3
    tls1_3 = TLS1_3_VERSION
};

// Certificates and settings shared by connections. Movable, not copyable.
// Functions return an error code and never throw; a stream keeps what it
// needs, so the context may be destroyed first.
class context
{
public:
    // TLS 1.2 or later; a client verifies its peer, a server does not.
    // Throws std::system_error if OpenSSL fails.
    // Parameter side: which end the streams will be
    explicit context(role side)
        : m_role(side)
        , m_settings(::SSL_CTX_new(side == role::server ? ::TLS_server_method() : ::TLS_client_method()))
    {
        if (m_settings == nullptr)
        {
            throw std::system_error(detail::take_ssl_error(), "spanio::ssl::context");
        }
        SSL_CTX* settings = m_settings.get();
        SSL_CTX_set_min_proto_version(settings, TLS1_2_VERSION);
        // Renegotiation is not supported, and resumption is not implemented
        ::SSL_CTX_set_options(settings, SSL_OP_NO_RENEGOTIATION | SSL_OP_NO_COMPRESSION | SSL_OP_NO_TICKET);
        SSL_CTX_set_session_cache_mode(settings, SSL_SESS_CACHE_OFF);
        ::SSL_CTX_set_num_tickets(settings, 0);
        SSL_CTX_set_mode(settings, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER
                                       | SSL_MODE_RELEASE_BUFFERS);
        ::SSL_CTX_set_default_passwd_cb(settings, &detail::no_ssl_password);
        set_verify_mode(side == role::client ? verify_mode::peer : verify_mode::none);
        ::ERR_clear_error();
    }

    // Load this end's certificate, followed by any intermediate ones
    // Parameter path: PEM file
    // Return value: empty on success, e.g. errc::no_such_file_or_directory
    std::error_code use_certificate_chain_file(const std::string& path)
    {
        ::ERR_clear_error();
        return detail::check_ssl(::SSL_CTX_use_certificate_chain_file(m_settings.get(), path.c_str()));
    }

    // Load this end's private key; load the certificate first, so a key that
    // does not belong to it is refused
    // Parameter path: unencrypted PEM file
    // Return value: empty on success
    std::error_code use_private_key_file(const std::string& path)
    {
        ::ERR_clear_error();
        const std::error_code ec =
            detail::check_ssl(::SSL_CTX_use_PrivateKey_file(m_settings.get(), path.c_str(), SSL_FILETYPE_PEM));
        return ec ? ec : check_key();
    }

    // Load this end's certificate from memory
    // Parameter pem: PEM text
    // Return value: empty on success
    std::error_code use_certificate(std::string_view pem)
    {
        ::ERR_clear_error();
        const detail::ssl_ptr<X509> certificate = read_certificate(pem);
        if (certificate == nullptr)
        {
            return detail::take_ssl_error();
        }
        return detail::check_ssl(::SSL_CTX_use_certificate(m_settings.get(), certificate.get()));
    }

    // Load this end's private key from memory; load the certificate first
    // Parameter pem: PEM text, unencrypted
    // Return value: empty on success
    std::error_code use_private_key(std::string_view pem)
    {
        ::ERR_clear_error();
        const detail::ssl_ptr<BIO> source(::BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())));
        if (source == nullptr)
        {
            return detail::take_ssl_error();
        }
        const detail::ssl_ptr<EVP_PKEY> key(
            ::PEM_read_bio_PrivateKey(source.get(), nullptr, &detail::no_ssl_password, nullptr));
        if (key == nullptr)
        {
            return detail::take_ssl_error();
        }
        const std::error_code ec = detail::check_ssl(::SSL_CTX_use_PrivateKey(m_settings.get(), key.get()));
        return ec ? ec : check_key();
    }

    // Trust the certificate authorities in a file
    // Parameter path: PEM file
    // Return value: empty on success
    std::error_code load_verify_file(const std::string& path)
    {
        ::ERR_clear_error();
        return detail::check_ssl(::SSL_CTX_load_verify_file(m_settings.get(), path.c_str()));
    }

    // Trust a certificate authority given in memory
    // Parameter pem: PEM text of one certificate
    // Return value: empty on success
    std::error_code add_certificate_authority(std::string_view pem)
    {
        ::ERR_clear_error();
        const detail::ssl_ptr<X509> certificate = read_certificate(pem);
        if (certificate == nullptr)
        {
            return detail::take_ssl_error();
        }
        return detail::check_ssl(
            ::X509_STORE_add_cert(::SSL_CTX_get_cert_store(m_settings.get()), certificate.get()));
    }

    // Trust the authorities in OpenSSL's default location
    // Return value: empty on success, also when nothing is installed there
    std::error_code set_default_verify_paths()
    {
        ::ERR_clear_error();
        return detail::check_ssl(::SSL_CTX_set_default_verify_paths(m_settings.get()));
    }

    // Choose what to check about the peer's certificate
    // Parameter mode: the checks
    void set_verify_mode(verify_mode mode) noexcept
    {
        int flags = SSL_VERIFY_NONE;
        if (mode != verify_mode::none)
        {
            flags = SSL_VERIFY_PEER;
        }
        if (mode == verify_mode::require_peer)
        {
            flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        }
        ::SSL_CTX_set_verify(m_settings.get(), flags, nullptr);
    }

    // Refuse older protocol versions (default TLS 1.2)
    // Parameter version: oldest accepted version
    // Return value: empty on success
    std::error_code set_min_version(tls_version version)
    {
        ::ERR_clear_error();
        return detail::check_ssl(
            static_cast<int>(SSL_CTX_set_min_proto_version(m_settings.get(), static_cast<int>(version))));
    }

    // Refuse newer protocol versions (default: none refused)
    // Parameter version: newest accepted version
    // Return value: empty on success
    std::error_code set_max_version(tls_version version)
    {
        ::ERR_clear_error();
        return detail::check_ssl(
            static_cast<int>(SSL_CTX_set_max_proto_version(m_settings.get(), static_cast<int>(version))));
    }

    // Which end this context is for
    // Return value: the role given to the constructor
    role side() const noexcept
    {
        return m_role;
    }

    // The OpenSSL object, for settings without a function here
    // Return value: never null; owned by this context
    SSL_CTX* native_handle() noexcept
    {
        return m_settings.get();
    }

private:
    // Parse one certificate
    // Parameter pem: PEM text
    // Return value: the certificate, or null with the reason in OpenSSL's queue
    static detail::ssl_ptr<X509> read_certificate(std::string_view pem)
    {
        const detail::ssl_ptr<BIO> source(::BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())));
        if (source == nullptr)
        {
            return nullptr;
        }
        return detail::ssl_ptr<X509>(
            ::PEM_read_bio_X509(source.get(), nullptr, &detail::no_ssl_password, nullptr));
    }

    // Check that the key belongs to the certificate. Also refuses a key
    // loaded first: loading a certificate would silently drop it if it
    // does not match
    // Return value: empty if it does
    std::error_code check_key()
    {
        return detail::check_ssl(::SSL_CTX_check_private_key(m_settings.get()));
    }

    // Which end this context is for
    role m_role;
    // OpenSSL's settings
    detail::ssl_ptr<SSL_CTX> m_settings;
};

// One TLS connection over a connected tcp::socket. Movable, not copyable.
// Destroying it closes the socket without the TLS close message; its pending
// handlers still run once, with operation_canceled. A stream that was moved
// from may only be destroyed or assigned to.
class stream
{
public:
    // Completion signature of async_handshake and async_shutdown
    using status_signature = void(std::error_code);

    // Completion signature of the reads and writes
    using transfer_signature = void(std::error_code, std::size_t);

    // Throws std::system_error if OpenSSL fails
    // Parameter socket:   connected socket; owned by the stream now
    // Parameter settings: certificates and settings; also decides the role
    stream(tcp::socket&& socket, context& settings)
        : m_core(std::make_shared<detail::ssl_core>(std::move(socket), settings.native_handle(),
                                                    settings.side() == role::server))
    {
    }

    // Take over another stream
    // Parameter other: stream to move from
    stream(stream&& other) noexcept = default;

    // Close this stream, then take over another one
    // Parameter other: stream to move from
    // Return value: *this
    stream& operator=(stream&& other) noexcept
    {
        if (this != &other)
        {
            release();
            m_core = std::move(other.m_core);
        }
        return *this;
    }

    stream(const stream&) = delete;
    stream& operator=(const stream&) = delete;

    // Close the socket
    ~stream()
    {
        release();
    }

    // Require the peer's certificate to be for this name, and send the name
    // to the server (SNI). Call it before the handshake.
    // Parameter host: DNS name or numeric address
    // Return value: empty on success; errc::invalid_argument for an empty name
    std::error_code set_verify_hostname(const std::string& host)
    {
        return m_core->set_verify_hostname(host);
    }

    // Set up TLS with the peer
    // Parameter token: completion token; a callback is called as
    //                  handler(std::error_code). A refused certificate
    //                  reports an X509_V_ERR_* value in
    //                  error::ssl_verify_category()
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_handshake(Token&& token) -> async_return_t<Token, status_signature>
    {
        return start_status(detail::ssl_kind::handshake, std::forward<Token>(token));
    }

    // Read what is available, at most buffer.size() bytes
    // Parameter buffer: destination; valid until the operation completes
    // Parameter token:  completion token; a callback is called as
    //                   handler(std::error_code, bytes read), with
    //                   error::misc_errors::eof after the peer's TLS close
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_read_some(const mutable_buffer& buffer, Token&& token) -> async_return_t<Token, transfer_signature>
    {
        return start_read(buffer, false, std::forward<Token>(token));
    }

    // Fill the whole buffer
    // Parameter buffer: destination; valid until the operation completes
    // Parameter token:  completion token, as for async_read_some; on an error
    //                   the byte count says what arrived before
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_read(const mutable_buffer& buffer, Token&& token) -> async_return_t<Token, transfer_signature>
    {
        return start_read(buffer, true, std::forward<Token>(token));
    }

    // Send part or all of a buffer, at most one TLS record
    // Parameter buffer: source; valid until the operation completes
    // Parameter token:  completion token; a callback is called as
    //                   handler(std::error_code, bytes sent) once the
    //                   encrypted data has been handed to the system
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_write_some(const const_buffer& buffer, Token&& token) -> async_return_t<Token, transfer_signature>
    {
        return start_write(buffer, false, std::forward<Token>(token));
    }

    // Send the whole buffer
    // Parameter buffer: source; valid until the operation completes
    // Parameter token:  completion token, as for async_write_some
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_write(const const_buffer& buffer, Token&& token) -> async_return_t<Token, transfer_signature>
    {
        return start_write(buffer, true, std::forward<Token>(token));
    }

    // Send the TLS close message, so the peer reads eof. Does not wait for
    // the peer's: to receive what it still sends, read until eof, and then
    // close(). Call it when no write is pending.
    // Parameter token: completion token; a callback is called as
    //                  handler(std::error_code)
    // Return value: void for a callback, otherwise what the token produces
    template <typename Token>
    auto async_shutdown(Token&& token) -> async_return_t<Token, status_signature>
    {
        return start_status(detail::ssl_kind::shutdown, std::forward<Token>(token));
    }

    // Close the socket at once, without the TLS close message; pending
    // operations fail with operation_canceled
    void close()
    {
        m_core->close();
    }

    // Whether the socket is open
    // Return value: true until close()
    bool is_open() const
    {
        return m_core->is_open();
    }

    // Address of the peer
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint remote_endpoint(std::error_code& ec) const
    {
        return m_core->remote_endpoint(ec);
    }

    // Address of this end
    // Parameter ec: set to the failure reason, or cleared
    // Return value: the endpoint
    tcp::endpoint local_endpoint(std::error_code& ec) const
    {
        return m_core->local_endpoint(ec);
    }

    // Context that runs the handlers
    // Return value: the context
    io_context& get_io_context() const noexcept
    {
        return m_core->context();
    }

    // The OpenSSL connection. Not locked: use it only while no operation is
    // pending, or from the only thread that runs the context.
    // Return value: never null; owned by the stream
    SSL* native_handle() noexcept
    {
        return m_core->native_handle();
    }

private:
    // Close the connection, if there is one
    void release() noexcept
    {
        if (m_core != nullptr)
        {
            m_core->close();
        }
    }

    // Start a handshake or shutdown
    // Parameter kind:  which one
    // Parameter token: completion token
    // Return value: what the token produces
    template <typename Token>
    auto start_status(detail::ssl_kind kind, Token&& token) -> async_return_t<Token, status_signature>
    {
        std::shared_ptr<detail::ssl_core> core = m_core;
        auto initiation = [core, kind](auto&& handler)
        {
            using handler_type = typename std::decay<decltype(handler)>::type;
            core->start(std::make_unique<detail::ssl_status_operation<handler_type>>(
                kind, std::forward<decltype(handler)>(handler)));
        };
        return async_initiate<status_signature>(std::move(initiation), std::forward<Token>(token));
    }

    // Start a read
    // Parameter buffer: destination
    // Parameter all:    whether to fill it
    // Parameter token:  completion token
    // Return value: what the token produces
    template <typename Token>
    auto start_read(const mutable_buffer& buffer, bool all, Token&& token)
        -> async_return_t<Token, transfer_signature>
    {
        std::shared_ptr<detail::ssl_core> core = m_core;
        auto initiation = [core, buffer, all](auto&& handler)
        {
            using handler_type = typename std::decay<decltype(handler)>::type;
            auto op = std::make_unique<detail::ssl_transfer_operation<handler_type>>(
                detail::ssl_kind::read, std::forward<decltype(handler)>(handler));
            op->m_input = static_cast<char*>(buffer.data());
            op->m_size = buffer.size();
            op->m_all = all;
            core->start(std::move(op));
        };
        return async_initiate<transfer_signature>(std::move(initiation), std::forward<Token>(token));
    }

    // Start a write
    // Parameter buffer: source
    // Parameter all:    whether to send all of it
    // Parameter token:  completion token
    // Return value: what the token produces
    template <typename Token>
    auto start_write(const const_buffer& buffer, bool all, Token&& token)
        -> async_return_t<Token, transfer_signature>
    {
        std::shared_ptr<detail::ssl_core> core = m_core;
        auto initiation = [core, buffer, all](auto&& handler)
        {
            using handler_type = typename std::decay<decltype(handler)>::type;
            auto op = std::make_unique<detail::ssl_transfer_operation<handler_type>>(
                detail::ssl_kind::write, std::forward<decltype(handler)>(handler));
            op->m_output = static_cast<const char*>(buffer.data());
            op->m_size = buffer.size();
            op->m_all = all;
            core->start(std::move(op));
        };
        return async_initiate<transfer_signature>(std::move(initiation), std::forward<Token>(token));
    }

    // The connection; null after a move
    std::shared_ptr<detail::ssl_core> m_core;
};

}}

#endif // SPANIO_SSL_H
