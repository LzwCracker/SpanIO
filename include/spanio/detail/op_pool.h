// ============================================================================
// File purpose: Memory pool for operation nodes (handler_op, io_op) to reduce
//               malloc/free overhead. Each thread holds its own pool (no lock
//               contention). Nodes are organized into size buckets; oversized
//               nodes fall back to operator new.
// Usage:        Internal to scheduler_op.h. Nodes are allocated through
//               make_handler_op() / make_io_op() and freed through their
//               do_complete() static functions.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-15
// ============================================================================

#ifndef SPANIO_DETAIL_OP_POOL_H
#define SPANIO_DETAIL_OP_POOL_H

#include <cstddef>
#include <cstdlib>
#include <new>

namespace spanio { namespace detail {

// Size of the smallest bucket, bytes; must be at least sizeof(void*)
constexpr std::size_t kMinBucketSize = 16;

// Size of the largest bucket, bytes; larger allocations use operator new
constexpr std::size_t kMaxBucketSize = 256;

// Number of size buckets (16, 32, 64, 128, 256)
constexpr std::size_t kBucketCount = 5;

// Per-thread memory pool for operation nodes. Each bucket is a singly-linked
// free list of blocks of one size. The pool never shrinks: freed blocks stay
// in the pool until thread exit.
class op_pool
{
public:
    op_pool() noexcept
    {
        for (std::size_t i = 0; i < kBucketCount; ++i)
        {
            m_buckets[i] = nullptr;
        }
    }

    ~op_pool()
    {
        // Free every block in every bucket
        for (std::size_t i = 0; i < kBucketCount; ++i)
        {
            void* block = m_buckets[i];
            while (block != nullptr)
            {
                void* next = *static_cast<void**>(block);
                std::free(block);
                block = next;
            }
        }
    }

    op_pool(const op_pool&) = delete;
    op_pool& operator=(const op_pool&) = delete;

    // Allocate memory for a node of the given size. Returns nullptr on
    // allocation failure (the caller must handle it or propagate the
    // std::bad_alloc that operator new would have thrown).
    // Parameter size: bytes needed, must be > 0
    // Return value: pointer to uninitialized memory, or nullptr
    void* allocate(std::size_t size) noexcept
    {
        if (size == 0 || size > kMaxBucketSize)
        {
            // Oversized or invalid: fall back to operator new (may throw)
            return ::operator new(size, std::nothrow);
        }
        const std::size_t bucket = size_to_bucket(size);
        const std::size_t bucket_size = bucket_to_size(bucket);
        if (m_buckets[bucket] != nullptr)
        {
            // Pop from the free list
            void* block = m_buckets[bucket];
            m_buckets[bucket] = *static_cast<void**>(block);
            return block;
        }
        // Bucket is empty; allocate a new block
        return std::malloc(bucket_size);
    }

    // Return a block to the pool. The block must have been allocated by this
    // pool or by operator new if it was oversized.
    // Parameter ptr:  block to free; may be nullptr (no-op)
    // Parameter size: the size that was passed to allocate()
    void deallocate(void* ptr, std::size_t size) noexcept
    {
        if (ptr == nullptr)
        {
            return;
        }
        if (size == 0 || size > kMaxBucketSize)
        {
            // Oversized: was allocated by operator new
            ::operator delete(ptr);
            return;
        }
        const std::size_t bucket = size_to_bucket(size);
        // Push onto the free list (store the old head in the block itself)
        *static_cast<void**>(ptr) = m_buckets[bucket];
        m_buckets[bucket] = ptr;
    }

private:
    // Map a requested size to a bucket index
    // Parameter size: bytes requested (1..kMaxBucketSize)
    // Return value: bucket index (0..kBucketCount-1)
    static std::size_t size_to_bucket(std::size_t size) noexcept
    {
        // Bucket 0: 16, 1: 32, 2: 64, 3: 128, 4: 256
        // Round up to the next power of 2 that is >= kMinBucketSize
        std::size_t bucket_size = kMinBucketSize;
        std::size_t bucket = 0;
        while (bucket_size < size && bucket < kBucketCount - 1)
        {
            bucket_size *= 2;
            ++bucket;
        }
        return bucket;
    }

    // Map a bucket index to the block size it holds
    // Parameter bucket: index (0..kBucketCount-1)
    // Return value: block size in bytes
    static std::size_t bucket_to_size(std::size_t bucket) noexcept
    {
        return kMinBucketSize << bucket;
    }

    // Free lists, one per bucket; each entry points to the next free block
    void* m_buckets[kBucketCount];
};

// Thread-local pool for the calling thread
// Return value: reference to the thread-local pool
inline op_pool& thread_pool() noexcept
{
    thread_local op_pool pool;
    return pool;
}

}}

#endif // SPANIO_DETAIL_OP_POOL_H
