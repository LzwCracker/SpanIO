// ============================================================================
// File purpose: Message framing for tcp_server. TCP is a byte stream: one
//               send by the peer may arrive split or merged with others. A
//               framer finds where one message ends in the bytes received so
//               far.
//               - line_framer():          messages end with '\n'
//               - length_prefix_framer(): 4-byte big-endian length + payload
//               Any function object with the framer signature works too.
// Usage:        spanio::tcp_server server(8080);
//               server.set_framer(spanio::line_framer());
//
//               A framer receives all unconsumed input and returns:
//               - m_consumed == 0: the message is not complete yet
//               - m_consumed > 0:  m_payload is one message, and m_consumed
//                                  input bytes can be dropped
//               - m_error == true: the input is malformed; the connection
//                                  is closed
//               m_payload must point into the input it was given.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-05
// ============================================================================

#ifndef SPANIO_FRAMING_H
#define SPANIO_FRAMING_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace spanio {

// Result of looking for one message in the input received so far
struct frame_result
{
    // The message, without its framing bytes; points into the input
    std::string_view m_payload;
    // Input bytes used by this message; 0 means more data is needed
    std::size_t m_consumed = 0;
    // The input is malformed and the connection should be closed
    bool m_error = false;
};

// Signature of a framer: takes the unconsumed input, returns a frame_result
using framer = std::function<frame_result(std::string_view)>;

// Size of the length header written by encode_length_prefixed()
constexpr std::size_t kLengthHeaderBytes = 4;

}

namespace spanio { namespace detail {

// Bits in one byte, for assembling the big-endian length header
constexpr unsigned kBitsPerByte = 8;

// Mask selecting the lowest byte of a value
constexpr std::uint32_t kLowByteMask = 0xFF;

// Character ending a line
constexpr char kLineFeed = '\n';

// Character some clients send before the line feed
constexpr char kCarriageReturn = '\r';

// Find one '\n'-terminated line
// Parameter input: unconsumed input
// Return value: the line without "\n" or "\r\n", or "need more data"
inline frame_result parse_line(std::string_view input)
{
    frame_result result;
    const std::size_t end = input.find(kLineFeed);
    if (end == std::string_view::npos)
    {
        return result;
    }
    result.m_consumed = end + 1;
    result.m_payload = input.substr(0, end);
    if (!result.m_payload.empty() && result.m_payload.back() == kCarriageReturn)
    {
        result.m_payload.remove_suffix(1);
    }
    return result;
}

// Find one length-prefixed message
// Parameter input: unconsumed input
// Return value: the payload without its header, or "need more data"
inline frame_result parse_length_prefixed(std::string_view input)
{
    frame_result result;
    if (input.size() < kLengthHeaderBytes)
    {
        return result;
    }
    std::uint32_t length = 0;
    for (std::size_t i = 0; i < kLengthHeaderBytes; ++i)
    {
        length = (length << kBitsPerByte) | static_cast<unsigned char>(input[i]);
    }
    const std::string_view rest = input.substr(kLengthHeaderBytes);
    if (rest.size() < length)
    {
        return result;
    }
    result.m_consumed = kLengthHeaderBytes + length;
    result.m_payload = rest.substr(0, length);
    return result;
}

}}

namespace spanio {

// Framer for text protocols: a message ends with '\n', and a '\r' before it
// is removed. Empty lines are messages too.
// Return value: the framer
inline framer line_framer()
{
    return framer(&detail::parse_line);
}

// Framer for binary protocols: each message is a 4-byte big-endian length
// followed by that many bytes
// Return value: the framer
inline framer length_prefix_framer()
{
    return framer(&detail::parse_length_prefixed);
}

// Build the bytes to send for one length-prefixed message. Throws
// std::length_error if the payload does not fit the 4-byte length.
// Parameter payload: message content
// Return value: header + payload
inline std::string encode_length_prefixed(std::string_view payload)
{
    if (payload.size() > (std::numeric_limits<std::uint32_t>::max)())
    {
        throw std::length_error("spanio::encode_length_prefixed: payload too large");
    }
    std::string message(kLengthHeaderBytes, '\0');
    const std::uint32_t length = static_cast<std::uint32_t>(payload.size());
    for (std::size_t i = 0; i < kLengthHeaderBytes; ++i)
    {
        const unsigned shift = static_cast<unsigned>(kLengthHeaderBytes - 1 - i) * detail::kBitsPerByte;
        message[i] = static_cast<char>((length >> shift) & detail::kLowByteMask);
    }
    message.append(payload.data(), payload.size());
    return message;
}

}

#endif // SPANIO_FRAMING_H
