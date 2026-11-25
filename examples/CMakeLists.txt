# ============================================================================
# File purpose: Builds the example programs. Each example is a single source
#               file that also compiles on its own with the one-line commands
#               in its header; here they get the same strict warnings as the
#               tests.
# Usage:        Add an example by creating examples/<name>.cpp and appending
#               <name> to SPANIO_EXAMPLE_NAMES. Turn examples off with
#               -DSPANIO_BUILD_EXAMPLES=OFF.
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-05
# ============================================================================

set(SPANIO_EXAMPLE_NAMES
    echo_server
    chat_server
    udp_echo_server
    timer_demo
    tcp_client
    udp_client
    coro_echo_server
)

# The TLS examples need OpenSSL
if(SPANIO_ENABLE_SSL)
    list(APPEND SPANIO_EXAMPLE_NAMES tls_echo_server tls_client)
endif()

foreach(example_name IN LISTS SPANIO_EXAMPLE_NAMES)
    add_executable(${example_name} ${example_name}.cpp)
    target_link_libraries(${example_name} PRIVATE spanio::spanio)
    target_compile_options(${example_name} PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX /permissive- /utf-8>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic -Werror>)
    # Show examples under an "examples" solution folder in Visual Studio
    set_target_properties(${example_name} PROPERTIES FOLDER "examples")
endforeach()

# The coroutine examples need C++20; the others stay C++17
target_compile_features(coro_echo_server PRIVATE cxx_std_20)
if(SPANIO_ENABLE_SSL)
    target_compile_features(tls_echo_server PRIVATE cxx_std_20)
endif()
