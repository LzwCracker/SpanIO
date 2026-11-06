// ============================================================================
// File purpose: IP addresses for the networking API:
//               - ip::address:      an IPv4 or IPv6 address (value type)
//               - ip::make_address: parse dotted / colon text into an address
//               - ip::endpoint:     an address and a port (tcp::endpoint and
//                                   udp::endpoint are aliases of it)
//               - ipv4::any() / ipv4::loopback(), ipv6::any() /
//                 ipv6::loopback(): the well-known addresses
// Usage:        std::error_code ec;
//               spanio::ip::address a = spanio::ip::make_address("127.0.0.1", ec);
//               if (!ec)
//               {
//                   std::string text = a.to_string();   // "127.0.0.1"
//               }
//
//               Only numeric addresses are parsed; host names need the
//               resolver (planned). The IPv6 scope id is kept but not shown
//               by to_string().
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_IP_H
#define SPANIO_IP_H

#include "spanio/detail/platform.h"
#include "spanio/detail/socket_ops.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>

namespace spanio { namespace ip {

// Bytes in an IPv4 address
constexpr std::size_t kV4AddressBytes = 4;

// Bytes in an IPv6 address
constexpr std::size_t kV6AddressBytes = 16;

// Text buffer large enough for any address inet_ntop produces, including
// the terminating zero (INET6_ADDRSTRLEN covers IPv4 too)
constexpr std::size_t kAddressTextSize = INET6_ADDRSTRLEN;

// inet_pton result that means the text was a valid address
constexpr int kPtonSuccess = 1;

// An IPv4 or IPv6 address. Default-constructed it is the IPv4 wildcard
// address 0.0.0.0. Addresses are stored in network byte order.
class address
{
public:
    // The IPv4 wildcard address 0.0.0.0
    address() noexcept
        : m_family(AF_INET)
        , m_bytes{}
        , m_scope_id(0)
    {
    }

    // Build an IPv4 address
    // Parameter value: address in network byte order
    // Return value: the address
    static address from_v4(const in_addr& value) noexcept
    {
        address result;
        result.m_family = AF_INET;
        std::memcpy(result.m_bytes.data(), &value, kV4AddressBytes);
        return result;
    }

    // Build an IPv6 address
    // Parameter value:    address in network byte order
    // Parameter scope_id: interface index for link-local addresses, else 0
    // Return value: the address
    static address from_v6(const in6_addr& value, std::uint32_t scope_id = 0) noexcept
    {
        address result;
        result.m_family = AF_INET6;
        std::memcpy(result.m_bytes.data(), &value, kV6AddressBytes);
        result.m_scope_id = scope_id;
        return result;
    }

    // Whether this is an IPv4 address
    // Return value: true for IPv4
    bool is_v4() const noexcept
    {
        return m_family == AF_INET;
    }

    // Whether this is an IPv6 address
    // Return value: true for IPv6
    bool is_v6() const noexcept
    {
        return m_family == AF_INET6;
    }

    // Address family
    // Return value: AF_INET or AF_INET6
    int family() const noexcept
    {
        return m_family;
    }

    // The address as an in_addr; only meaningful when is_v4()
    // Return value: address in network byte order
    in_addr to_v4() const noexcept
    {
        in_addr value{};
        std::memcpy(&value, m_bytes.data(), kV4AddressBytes);
        return value;
    }

    // The address as an in6_addr; only meaningful when is_v6()
    // Return value: address in network byte order
    in6_addr to_v6() const noexcept
    {
        in6_addr value{};
        std::memcpy(&value, m_bytes.data(), kV6AddressBytes);
        return value;
    }

    // IPv6 scope id (interface index); 0 for IPv4 and global addresses
    // Return value: the scope id
    std::uint32_t scope_id() const noexcept
    {
        return m_scope_id;
    }

    // Text form: dotted decimal for IPv4, RFC 5952 form for IPv6
    // Return value: the text, or an empty string if formatting failed
    std::string to_string() const
    {
        char text[kAddressTextSize] = {};
        if (::inet_ntop(m_family, m_bytes.data(), text, sizeof(text)) == nullptr)
        {
            detail::log_error("inet_ntop", detail::last_error());
            return std::string();
        }
        return std::string(text);
    }

    // Two addresses are equal when family, bytes and scope id match
    // Parameter other: address to compare with
    // Return value: true when equal
    bool operator==(const address& other) const noexcept
    {
        return m_family == other.m_family && m_bytes == other.m_bytes
            && m_scope_id == other.m_scope_id;
    }

    // Negation of operator==
    // Parameter other: address to compare with
    // Return value: true when different
    bool operator!=(const address& other) const noexcept
    {
        return !(*this == other);
    }

private:
    // AF_INET or AF_INET6
    int m_family;
    // Address bytes in network order; IPv4 uses the first 4, the rest stay 0
    std::array<unsigned char, kV6AddressBytes> m_bytes;
    // IPv6 interface index; always 0 for IPv4
    std::uint32_t m_scope_id;
};

// Parse a numeric IPv4 or IPv6 address
// Parameter text: e.g. "192.168.1.10" or "fe80::1"
// Parameter ec:   set to errc::invalid_argument if text is not an address,
//                 cleared on success
// Return value: the address, or a default address on failure
inline address make_address(const char* text, std::error_code& ec)
{
    ec = detail::ensure_network_initialized();
    if (ec)
    {
        return address();
    }
    in_addr v4{};
    if (::inet_pton(AF_INET, text, &v4) == kPtonSuccess)
    {
        return address::from_v4(v4);
    }
    in6_addr v6{};
    if (::inet_pton(AF_INET6, text, &v6) == kPtonSuccess)
    {
        return address::from_v6(v6);
    }
    ec = std::make_error_code(std::errc::invalid_argument);
    return address();
}

// Parse a numeric IPv4 or IPv6 address
// Parameter text: e.g. "192.168.1.10" or "fe80::1"
// Parameter ec:   set to errc::invalid_argument if text is not an address
// Return value: the address, or a default address on failure
inline address make_address(const std::string& text, std::error_code& ec)
{
    return make_address(text.c_str(), ec);
}

// Parse a numeric IPv4 or IPv6 address; throws std::system_error on failure
// Parameter text: e.g. "192.168.1.10" or "fe80::1"
// Return value: the address
inline address make_address(const std::string& text)
{
    std::error_code ec;
    const address result = make_address(text.c_str(), ec);
    if (ec)
    {
        throw std::system_error(ec, "spanio::ip::make_address");
    }
    return result;
}

// An IP address and a port. Default-constructed it is 0.0.0.0:0.
class endpoint
{
public:
    // The IPv4 wildcard address with port 0
    endpoint() noexcept
        : endpoint(ip::address(), 0)
    {
    }

    // Parameter address: IPv4 or IPv6 address
    // Parameter port:    port in host byte order; 0 lets bind() pick one
    endpoint(const ip::address& address, std::uint16_t port) noexcept
        : m_data{}
        , m_size(0)
    {
        if (address.is_v4())
        {
            sockaddr_in v4{};
            v4.sin_family = AF_INET;
            v4.sin_port = htons(port);
            v4.sin_addr = address.to_v4();
            std::memcpy(&m_data, &v4, sizeof(v4));
            m_size = static_cast<detail::socklen_type>(sizeof(v4));
        }
        else
        {
            sockaddr_in6 v6{};
            v6.sin6_family = AF_INET6;
            v6.sin6_port = htons(port);
            v6.sin6_addr = address.to_v6();
            v6.sin6_scope_id = address.scope_id();
            std::memcpy(&m_data, &v6, sizeof(v6));
            m_size = static_cast<detail::socklen_type>(sizeof(v6));
        }
    }

    // Build an endpoint from a socket address returned by the system
    // Parameter data: sockaddr_in or sockaddr_in6
    // Parameter size: bytes valid at data
    // Return value: the endpoint
    static endpoint from_native(const sockaddr* data, detail::socklen_type size) noexcept
    {
        endpoint result;
        std::size_t length = static_cast<std::size_t>(size);
        if (length > sizeof(result.m_data))
        {
            length = sizeof(result.m_data);
        }
        std::memset(&result.m_data, 0, sizeof(result.m_data));
        std::memcpy(&result.m_data, data, length);
        result.m_size = static_cast<detail::socklen_type>(length);
        return result;
    }

    // Address family
    // Return value: AF_INET or AF_INET6
    int family() const noexcept
    {
        return m_data.ss_family;
    }

    // The IP address
    // Return value: the address part
    ip::address address() const noexcept
    {
        if (family() == AF_INET6)
        {
            sockaddr_in6 v6{};
            std::memcpy(&v6, &m_data, sizeof(v6));
            return ip::address::from_v6(v6.sin6_addr, v6.sin6_scope_id);
        }
        sockaddr_in v4{};
        std::memcpy(&v4, &m_data, sizeof(v4));
        return ip::address::from_v4(v4.sin_addr);
    }

    // The port
    // Return value: port in host byte order
    std::uint16_t port() const noexcept
    {
        if (family() == AF_INET6)
        {
            sockaddr_in6 v6{};
            std::memcpy(&v6, &m_data, sizeof(v6));
            return ntohs(v6.sin6_port);
        }
        sockaddr_in v4{};
        std::memcpy(&v4, &m_data, sizeof(v4));
        return ntohs(v4.sin_port);
    }

    // The socket address for system calls
    // Return value: pointer to the stored address
    const sockaddr* data() const noexcept
    {
        return reinterpret_cast<const sockaddr*>(&m_data);
    }

    // Number of meaningful bytes at data()
    // Return value: sizeof(sockaddr_in) or sizeof(sockaddr_in6)
    detail::socklen_type size() const noexcept
    {
        return m_size;
    }

    // Two endpoints are equal when address and port match
    // Parameter other: endpoint to compare with
    // Return value: true when equal
    bool operator==(const endpoint& other) const noexcept
    {
        return address() == other.address() && port() == other.port();
    }

    // Negation of operator==
    // Parameter other: endpoint to compare with
    // Return value: true when different
    bool operator!=(const endpoint& other) const noexcept
    {
        return !(*this == other);
    }

private:
    // The socket address (sockaddr_in or sockaddr_in6)
    sockaddr_storage m_data;
    // Number of meaningful bytes in m_data
    detail::socklen_type m_size;
};


}}

namespace spanio { namespace ipv4 {

// The IPv4 wildcard address 0.0.0.0, used to listen on every interface
// Return value: the address
inline ip::address any() noexcept
{
    in_addr value{};
    value.s_addr = htonl(INADDR_ANY);
    return ip::address::from_v4(value);
}

// The IPv4 loopback address 127.0.0.1
// Return value: the address
inline ip::address loopback() noexcept
{
    in_addr value{};
    value.s_addr = htonl(INADDR_LOOPBACK);
    return ip::address::from_v4(value);
}

}}

namespace spanio { namespace ipv6 {

// The IPv6 wildcard address ::, used to listen on every interface
// Return value: the address
inline ip::address any() noexcept
{
    const in6_addr value = IN6ADDR_ANY_INIT;
    return ip::address::from_v6(value);
}

// The IPv6 loopback address ::1
// Return value: the address
inline ip::address loopback() noexcept
{
    const in6_addr value = IN6ADDR_LOOPBACK_INIT;
    return ip::address::from_v6(value);
}

}}

#endif // SPANIO_IP_H
