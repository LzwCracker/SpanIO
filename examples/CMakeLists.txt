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
)

foreach(example_name IN LISTS SPANIO_EXAMPLE_NAMES)
    add_executable(${example_name} ${example_name}.cpp)
    target_link_libraries(${example_name} PRIVATE spanio::spanio)
    target_compile_options(${example_name} PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX /permissive- /utf-8>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic -Werror>)
    # Show examples under an "examples" solution folder in Visual Studio
    set_target_properties(${example_name} PROPERTIES FOLDER "examples")
endforeach()
