// ============================================================================
// File purpose: List of every io_proactor backend available on the platform,
//               shared by the tests that must cover all of them:
//               - Windows: iocp_proactor (native) and reactor_proactor over
//                 the IOCP + AFD poll reactor
//               - Linux:   reactor_proactor over epoll
// Usage:        for (const backend_factory& backend : backends())
//               {
//                   announce(backend);
//                   std::error_code ec;
//                   io_proactor_ptr proactor = backend.m_create(ec);
//                   ...
//               }
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-04
// ============================================================================

#ifndef SPANIO_TESTS_TEST_PROACTOR_BACKENDS_H
#define SPANIO_TESTS_TEST_PROACTOR_BACKENDS_H

#include "spanio/detail/platform_proactor.h"

#if defined(SPANIO_PLATFORM_WINDOWS)
#include "spanio/detail/iocp_proactor.h"
#endif

#include <cstdio>
#include <system_error>
#include <vector>

namespace spanio { namespace test {

// One backend under test
struct backend_factory
{
    // Label printed before the backend's checks
    const char* m_label;
    // Function creating a fresh instance
    detail::io_proactor_ptr (*m_create)(std::error_code&);
};

#if defined(SPANIO_PLATFORM_WINDOWS)
// Create the native IOCP proactor through the common pointer type
// Parameter ec: set to the failure reason, or cleared
// Return value: the proactor, or nullptr
inline detail::io_proactor_ptr create_native_proactor(std::error_code& ec)
{
    return detail::iocp_proactor::create(ec);
}
#endif

// Every backend available on this platform
// Return value: list of factories, never empty
inline std::vector<backend_factory> backends()
{
    std::vector<backend_factory> list;
#if defined(SPANIO_PLATFORM_WINDOWS)
    list.push_back(backend_factory{"iocp-proactor (native)", &create_native_proactor});
    list.push_back(backend_factory{"reactor-proactor over iocp-afd", &detail::make_reactor_proactor});
#else
    list.push_back(backend_factory{"reactor-proactor over epoll", &detail::make_reactor_proactor});
#endif
    return list;
}

// Print which backend the following checks belong to
// Parameter backend: backend about to be tested
inline void announce(const backend_factory& backend)
{
    std::printf("    [%s]\n", backend.m_label);
}

}}

#endif // SPANIO_TESTS_TEST_PROACTOR_BACKENDS_H
