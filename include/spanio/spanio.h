// ============================================================================
// File purpose: Umbrella header: includes the whole public API of SpanIO.
// Usage:        #include "spanio/spanio.h"   // with include/ on the include path
//               spanio::io_context ctx;
//               spanio::tcp_server server(8080);
//               ...
//               Including single headers (e.g. "spanio/tcp.h") also works and
//               compiles faster.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

#ifndef SPANIO_SPANIO_H
#define SPANIO_SPANIO_H

#include "spanio/version.h"

#include "spanio/buffer.h"
#include "spanio/error.h"
#include "spanio/framing.h"
#include "spanio/io_context.h"
#include "spanio/ip.h"
#include "spanio/resolver.h"
#include "spanio/strand.h"
#include "spanio/tcp.h"
#include "spanio/tcp_server.h"
#include "spanio/timer.h"
#include "spanio/udp.h"
#include "spanio/udp_server.h"

// C++20 coroutine support (awaitable, use_awaitable, co_spawn) is opt-in,
// so the rest of the library keeps building as C++17
#if defined(SPANIO_ENABLE_COROUTINE)
#include "spanio/coroutine.h"
#endif

#endif // SPANIO_SPANIO_H
