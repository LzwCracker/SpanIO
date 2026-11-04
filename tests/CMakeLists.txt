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
)

foreach(test_name IN LISTS SPANIO_TEST_NAMES)
    add_executable(${test_name} ${test_name}.cpp test_framework.h test_net_helpers.h
        test_proactor_backends.h)
    target_link_libraries(${test_name} PRIVATE spanio::spanio)
    target_compile_options(${test_name} PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX /permissive- /utf-8>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic -Werror>)
    # Show test executables under a "tests" solution folder in Visual Studio
    set_target_properties(${test_name} PROPERTIES FOLDER "tests")
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
