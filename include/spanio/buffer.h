// ============================================================================
// File purpose: Non-owning views of memory used by the asynchronous read and
//               write operations:
//               - mutable_buffer: memory an operation may write (reads)
//               - const_buffer:   memory an operation only reads (writes)
//               The spanio::buffer() overloads build them from raw memory,
//               arrays, std::string, std::vector<char> and std::string_view.
// Usage:        char data[1024];
//               sock.async_read_some(spanio::buffer(data), on_read);
//               std::string reply = "hello";
//               sock.async_write(spanio::buffer(reply), on_written);
//
//               A buffer never owns its memory: the memory must stay valid
//               until the operation's handler has run. buffer() of a string
//               literal includes its terminating zero, like any char array.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_BUFFER_H
#define SPANIO_BUFFER_H

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace spanio {

// Writable memory region: a pointer and a size in bytes
class mutable_buffer
{
public:
    // Empty buffer
    mutable_buffer() noexcept
        : m_data(nullptr)
        , m_size(0)
    {
    }

    // Parameter data: start of the region
    // Parameter size: length of the region in bytes
    mutable_buffer(void* data, std::size_t size) noexcept
        : m_data(data)
        , m_size(size)
    {
    }

    // Start of the region
    // Return value: the pointer given at construction
    void* data() const noexcept
    {
        return m_data;
    }

    // Length of the region
    // Return value: size in bytes
    std::size_t size() const noexcept
    {
        return m_size;
    }

private:
    // Start of the region; not owned
    void* m_data;
    // Length of the region in bytes
    std::size_t m_size;
};

// Read-only memory region: a pointer and a size in bytes
class const_buffer
{
public:
    // Empty buffer
    const_buffer() noexcept
        : m_data(nullptr)
        , m_size(0)
    {
    }

    // Parameter data: start of the region
    // Parameter size: length of the region in bytes
    const_buffer(const void* data, std::size_t size) noexcept
        : m_data(data)
        , m_size(size)
    {
    }

    // Any writable region can also be read
    // Parameter other: writable region to view read-only
    const_buffer(const mutable_buffer& other) noexcept
        : m_data(other.data())
        , m_size(other.size())
    {
    }

    // Start of the region
    // Return value: the pointer given at construction
    const void* data() const noexcept
    {
        return m_data;
    }

    // Length of the region
    // Return value: size in bytes
    std::size_t size() const noexcept
    {
        return m_size;
    }

private:
    // Start of the region; not owned
    const void* m_data;
    // Length of the region in bytes
    std::size_t m_size;
};

// Writable buffer over raw memory
// Parameter data: start of the region
// Parameter size: length in bytes
// Return value: the buffer
inline mutable_buffer buffer(void* data, std::size_t size) noexcept
{
    return mutable_buffer(data, size);
}

// Read-only buffer over raw memory
// Parameter data: start of the region
// Parameter size: length in bytes
// Return value: the buffer
inline const_buffer buffer(const void* data, std::size_t size) noexcept
{
    return const_buffer(data, size);
}

// Writable buffer over a whole char array
// Parameter data: the array
// Return value: buffer of N bytes
template <std::size_t N>
mutable_buffer buffer(char (&data)[N]) noexcept
{
    return mutable_buffer(data, N);
}

// Read-only buffer over a whole char array (a string literal includes its
// terminating zero)
// Parameter data: the array
// Return value: buffer of N bytes
template <std::size_t N>
const_buffer buffer(const char (&data)[N]) noexcept
{
    return const_buffer(data, N);
}

// Writable buffer over the characters of a string
// Parameter data: the string; must not be resized while the buffer is in use
// Return value: buffer of data.size() bytes
inline mutable_buffer buffer(std::string& data) noexcept
{
    return mutable_buffer(&data[0], data.size());
}

// Read-only buffer over the characters of a string
// Parameter data: the string
// Return value: buffer of data.size() bytes
inline const_buffer buffer(const std::string& data) noexcept
{
    return const_buffer(data.data(), data.size());
}

// Writable buffer over the elements of a vector
// Parameter data: the vector; must not be resized while the buffer is in use
// Return value: buffer of data.size() bytes
inline mutable_buffer buffer(std::vector<char>& data) noexcept
{
    return mutable_buffer(data.data(), data.size());
}

// Read-only buffer over the elements of a vector
// Parameter data: the vector
// Return value: buffer of data.size() bytes
inline const_buffer buffer(const std::vector<char>& data) noexcept
{
    return const_buffer(data.data(), data.size());
}

// Read-only buffer over a string view
// Parameter data: the viewed characters; they must outlive the operation
// Return value: buffer of data.size() bytes
inline const_buffer buffer(std::string_view data) noexcept
{
    return const_buffer(data.data(), data.size());
}

}

#endif // SPANIO_BUFFER_H
