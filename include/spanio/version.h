// ============================================================================
// File purpose: Version of SpanIO. The public API (everything in namespace
//               spanio outside spanio::detail) is frozen as of 0.3.0: later
//               versions only add to it, and a change that breaks existing
//               code raises the major version. spanio::detail may change at
//               any time.
// Usage:        #include "spanio/version.h"
//               #if SPANIO_VERSION >= SPANIO_MAKE_VERSION(0, 3, 0)
//               ...
//               #endif
//               std::puts(SPANIO_VERSION_STRING);
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-10
// ============================================================================

#ifndef SPANIO_VERSION_H
#define SPANIO_VERSION_H

// Major version: raised by changes that break existing code
#define SPANIO_VERSION_MAJOR 0

// Minor version: raised by additions; 3 is the API freeze at milestone M3
#define SPANIO_VERSION_MINOR 3

// Patch version: raised by fixes that change no interface
#define SPANIO_VERSION_PATCH 0

// Weight of the major version in SPANIO_MAKE_VERSION
#define SPANIO_VERSION_MAJOR_FACTOR 10000

// Weight of the minor version in SPANIO_MAKE_VERSION
#define SPANIO_VERSION_MINOR_FACTOR 100

// One comparable number for a version, e.g. 0.3.0 -> 300
#define SPANIO_MAKE_VERSION(major, minor, patch) \
    ((major) * SPANIO_VERSION_MAJOR_FACTOR + (minor) * SPANIO_VERSION_MINOR_FACTOR + (patch))

// This version as one comparable number
#define SPANIO_VERSION SPANIO_MAKE_VERSION(SPANIO_VERSION_MAJOR, SPANIO_VERSION_MINOR, SPANIO_VERSION_PATCH)

// This version as text
#define SPANIO_VERSION_STRING "0.3.0"

#endif // SPANIO_VERSION_H
