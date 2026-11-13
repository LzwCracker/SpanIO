# ============================================================================
# File purpose: Builds one executable per test source and registers each with
#               CTest. Tests compile with strict warnings treated as errors.
# Usage:        Add a new test by creating tests/<name>.cpp and appending
#               <name> to SPANIO_TEST_NAMES. Run one test with
#               ctest --test-dir build -C Debug -R <name>, or run the
#               executable directly with a case-name filter argument.
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-03
# ============================================================================

set(SPANIO_TEST_NAMES
    test_socket_ops
    test_io_reactor
    test_io_proactor
    test_io_context
    test_tcp
    test_echo_server
    test_tcp_server
    test_chat_server
    test_udp
    test_udp_server
    test_timer
    test_server_timeouts
    test_resolver
    test_strand
    test_errors
    test_api
    test_client_examples
    test_guide
    test_benchmark
)

foreach(test_name IN LISTS SPANIO_TEST_NAMES)
    add_executable(${test_name} ${test_name}.cpp test_framework.h test_net_helpers.h
        test_proactor_backends.h test_proactor_decorators.h)
    target_link_libraries(${test_name} PRIVATE spanio::spanio)
    target_compile_options(${test_name} PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX /permissive- /utf-8>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic -Werror>)
    # Show test executables under a "tests" solution folder in Visual Studio
    set_target_properties(${test_name} PROPERTIES FOLDER "tests")
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# The code snippets of docs/guide.md must be the code test_guide compiles
add_test(NAME guide_snippets_match
    COMMAND ${CMAKE_COMMAND}
        -DGUIDE=${CMAKE_CURRENT_SOURCE_DIR}/../docs/guide.md
        -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/test_guide.cpp
        -P ${CMAKE_CURRENT_SOURCE_DIR}/check_guide_snippets.cmake)

# test_benchmark runs benchmark/compare.cmake: give it CMake, the script and
# a directory for its scratch files
target_compile_definitions(test_benchmark PRIVATE
    SPANIO_CMAKE_COMMAND="${CMAKE_COMMAND}"
    SPANIO_COMPARE_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/../benchmark/compare.cmake"
    SPANIO_TEST_WORK_DIR="${CMAKE_CURRENT_BINARY_DIR}")
