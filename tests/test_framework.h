// ============================================================================
// File purpose: Minimal self-contained unit test framework for SpanIO, so the
//               test suite keeps the project's zero-dependency promise.
// Usage:        Define cases with SPANIO_TEST(name) { ... } and check them
//               with SPANIO_CHECK(cond) (records a failure, keeps going) or
//               SPANIO_REQUIRE(cond) (records a failure, stops the case).
//               Each test executable defines main() and returns
//               spanio::test::run_all(argc, argv). Passing an argument runs
//               only the cases whose name contains that text, e.g.
//               `test_socket_ops close_socket`.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-03
// ============================================================================

#ifndef SPANIO_TESTS_TEST_FRAMEWORK_H
#define SPANIO_TESTS_TEST_FRAMEWORK_H

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace spanio { namespace test {

// Process exit code returned by run_all() when every selected case passed
constexpr int kExitSuccess = 0;

// Process exit code returned by run_all() when a case failed or none matched
constexpr int kExitFailure = 1;

// Index of the optional name filter in argv (argv[0] is the program name)
constexpr int kFilterArgIndex = 1;

// One registered test case
struct test_case
{
    // Name shown in the report and matched by the command-line filter
    const char* m_name;
    // Function holding the body of the case
    void (*m_func)();
};

// Thrown by SPANIO_REQUIRE to leave the current case early. It carries no
// data because the failure has already been reported and counted.
struct require_failed
{
};

// All cases defined in this executable, in registration order
// Return value: the process-wide list, created on first use so registration
//               from static initializers is safe regardless of order
inline std::vector<test_case>& registry()
{
    static std::vector<test_case> s_cases;
    return s_cases;
}

// Number of failed checks in the case that is currently running
// Return value: reference to the counter, reset by run_all() before each case
inline int& current_failures()
{
    static int s_failures = 0;
    return s_failures;
}

// Record and print a failed check
// Parameter file: source file containing the check
// Parameter line: line number of the check
// Parameter expr: text of the expression that evaluated to false
inline void report_failure(const char* file, int line, const char* expr)
{
    ++current_failures();
    std::printf("    FAILED %s:%d: %s\n", file, line, expr);
}

// Adds a case to the registry during static initialization; SPANIO_TEST
// creates one instance per case
class registrar
{
public:
    // Register a case
    // Parameter name: case name, must be a string literal
    // Parameter func: function holding the body of the case
    registrar(const char* name, void (*func)())
    {
        registry().push_back(test_case{name, func});
    }
};

// Run every registered case, or only those matching the filter, and print a
// summary
// Parameter argc: argument count from main()
// Parameter argv: arguments from main(); argv[1], if present, is a substring
//                 that case names must contain to be run
// Return value: kExitSuccess if all selected cases passed, else kExitFailure
inline int run_all(int argc, char** argv)
{
    const char* filter = (argc > kFilterArgIndex) ? argv[kFilterArgIndex] : nullptr;
    int run_count = 0;
    int failed_count = 0;

    for (const test_case& tc : registry())
    {
        if (filter != nullptr && std::strstr(tc.m_name, filter) == nullptr)
        {
            continue;
        }

        ++run_count;
        current_failures() = 0;
        std::printf("[ RUN  ] %s\n", tc.m_name);
        try
        {
            tc.m_func();
        }
        catch (const require_failed&)
        {
            // Already reported; only the rest of this case is skipped
        }
        catch (...)
        {
            report_failure(__FILE__, __LINE__, "unexpected exception");
        }

        if (current_failures() == 0)
        {
            std::printf("[  OK  ] %s\n", tc.m_name);
        }
        else
        {
            ++failed_count;
            std::printf("[ FAIL ] %s\n", tc.m_name);
        }
    }

    std::printf("%d case(s) run, %d failed\n", run_count, failed_count);
    // A filter that matches nothing is treated as an error so a typo is not
    // mistaken for a pass
    if (run_count == 0 || failed_count != 0)
    {
        return kExitFailure;
    }
    return kExitSuccess;
}

}}

// Define and register a test case named `name` (must be a valid identifier)
#define SPANIO_TEST(name)                                                    \
    static void spanio_test_##name();                                        \
    static ::spanio::test::registrar spanio_test_registrar_##name(          \
        #name, &spanio_test_##name);                                         \
    static void spanio_test_##name()

// Check a condition; on failure report it and continue the case
#define SPANIO_CHECK(cond)                                                   \
    do                                                                       \
    {                                                                        \
        if (!(cond))                                                         \
        {                                                                    \
            ::spanio::test::report_failure(__FILE__, __LINE__, #cond);       \
        }                                                                    \
    } while (false)

// Check a condition; on failure report it and stop the current case. Use it
// when later checks would be meaningless or unsafe after this one fails.
#define SPANIO_REQUIRE(cond)                                                 \
    do                                                                       \
    {                                                                        \
        if (!(cond))                                                         \
        {                                                                    \
            ::spanio::test::report_failure(__FILE__, __LINE__, #cond);       \
            throw ::spanio::test::require_failed{};                          \
        }                                                                    \
    } while (false)

#endif // SPANIO_TESTS_TEST_FRAMEWORK_H
