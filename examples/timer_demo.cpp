// ============================================================================
// File purpose: steady_timer example: a timer that re-arms itself from its
//               handler, printing a tick each time.
// Usage:        timer_demo
//               It prints three ticks, half a second apart, and exits: once
//               no wait is pending, run() is out of work and returns.
//
//               Single-file build, from the repository root:
//                 Linux:   g++ -std=c++17 -Iinclude examples/timer_demo.cpp -o timer_demo -lpthread
//                 Windows: cl /std:c++17 /EHsc /Iinclude examples\timer_demo.cpp ws2_32.lib
//               CMake builds it as the timer_demo target when
//               SPANIO_BUILD_EXAMPLES is on (the default at top level).
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-07
// ============================================================================

#include "spanio/timer.h"

#include <chrono>
#include <cstdio>
#include <functional>
#include <system_error>

// Time between two ticks
constexpr std::chrono::milliseconds kTickInterval(500);

// Number of ticks to print
constexpr int kTickCount = 3;

// Run the timers until the last tick
// Return value: 0
int main()
{
    spanio::io_context context;
    spanio::steady_timer ticker(context, kTickInterval);
    int ticks = 0;

    std::function<void(std::error_code)> on_tick;
    on_tick = [&](std::error_code ec)
    {
        if (ec)
        {
            return;
        }
        ++ticks;
        std::printf("tick %d\n", ticks);
        if (ticks < kTickCount)
        {
            // Relative to the last expiry, so delays do not add up
            ticker.expires_at(ticker.expiry() + kTickInterval);
            ticker.async_wait(on_tick);
        }
    };
    ticker.async_wait(on_tick);

    context.run();
    return 0;
}
