// ============================================================================
// File purpose: Runs an io_context on a number of threads, for the servers
//               that own their context (tcp_server, udp_server).
// Usage:        spanio::detail::run_on_threads(context, 4);
//               Blocks until run() has returned on every thread. The calling
//               thread is one of them.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-06
// ============================================================================

#ifndef SPANIO_DETAIL_RUN_THREADS_H
#define SPANIO_DETAIL_RUN_THREADS_H

#include "spanio/io_context.h"

#include <exception>
#include <mutex>
#include <thread>
#include <vector>

namespace spanio { namespace detail {

// Run a context on several threads. If run() throws on one of them, the
// context is stopped so the others end too, and the first exception is
// rethrown once they all have.
// Parameter context: context to run
// Parameter threads: number of threads, including the calling one
inline void run_on_threads(io_context& context, unsigned threads)
{
    std::mutex failure_mutex;
    std::exception_ptr failure;
    auto run_context = [&context, &failure_mutex, &failure]()
    {
        try
        {
            context.run();
        }
        catch (...)
        {
            std::lock_guard<std::mutex> lock(failure_mutex);
            if (failure == nullptr)
            {
                failure = std::current_exception();
            }
            context.stop();
        }
    };
    std::vector<std::thread> workers;
    for (unsigned i = 1; i < threads; ++i)
    {
        workers.emplace_back(run_context);
    }
    run_context();
    for (std::thread& worker : workers)
    {
        worker.join();
    }
    if (failure != nullptr)
    {
        std::rethrow_exception(failure);
    }
}

}}

#endif // SPANIO_DETAIL_RUN_THREADS_H
