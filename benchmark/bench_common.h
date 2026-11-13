// ============================================================================
// File purpose: Helpers shared by the SpanIO benchmarks: statistics over
//               repeated runs (median, nearest-rank percentiles, stability),
//               the CSV format results are stored in, and a description of
//               the machine the numbers come from.
// Usage:        Included by benchmark/bench_spanio.cpp and, through it, by
//               tests/test_benchmark.cpp.
//                 std::vector<double> runs = {...};
//                 bench::spread s = bench::summarize(runs);
//                 if (!s.m_stable) { ... }
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-12
// ============================================================================

#ifndef SPANIO_BENCHMARK_BENCH_COMMON_H
#define SPANIO_BENCHMARK_BENCH_COMMON_H

#include "spanio/version.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
#include <intrin.h>
#define SPANIO_BENCH_CPUID_MSVC 1
#elif (defined(__GNUC__) || defined(__clang__)) && (defined(__x86_64__) || defined(__i386__))
#include <cpuid.h>
#define SPANIO_BENCH_CPUID_GCC 1
#endif

namespace bench {

// A run set counts as unstable when max - min exceeds this share of the
// median: the numbers are then too noisy to compare
constexpr double kStabilityLimit = 0.10;

// Factor for turning a fraction into a percentage
constexpr double kPercent = 100.0;

// Scale of the 3 decimals that reports show; stability is judged on values
// rounded to it, so a row never reads "0.000 (min 0.000, max 0.000)"
// and is flagged unstable
constexpr double kReportedScale = 1000.0;

// First CPUID leaf of the processor brand string
constexpr unsigned kBrandLeafFirst = 0x80000002u;

// Number of CPUID leaves holding the brand string
constexpr unsigned kBrandLeaves = 3;

// Registers per CPUID leaf (eax, ebx, ecx, edx)
constexpr unsigned kCpuidRegisters = 4;

// Header line of the result CSV; one row per metric of one configuration
constexpr char kCsvHeader[] =
    "version,platform,cpu,backend,scenario,params,metric,unit,better,median,min,max,stable";

// Median, range and stability of the values of repeated runs
struct spread
{
    // Middle value (mean of the two middle values for an even count)
    double m_median = 0.0;
    // Smallest value
    double m_min = 0.0;
    // Largest value
    double m_max = 0.0;
    // Whether max - min stays within kStabilityLimit of the median, judged on
    // the values rounded to the 3 reported decimals
    bool m_stable = true;
};

// Median of a set of values
// Parameter values: the values; may be unsorted
// Return value: the median; 0 for an empty set
inline double median(std::vector<double> values)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t middle = values.size() / 2;
    if (values.size() % 2 == 0)
    {
        return (values[middle - 1] + values[middle]) / 2.0;
    }
    return values[middle];
}

// Nearest-rank percentile: the smallest value that at least p percent of
// the values do not exceed
// Parameter sorted: the values in ascending order
// Parameter p:      percentile, 0-100
// Return value: the percentile; 0 for an empty set
inline double percentile(const std::vector<double>& sorted, double p)
{
    if (sorted.empty())
    {
        return 0.0;
    }
    const double rank = std::ceil(p / kPercent * static_cast<double>(sorted.size()));
    std::size_t index = rank < 1.0 ? 0 : static_cast<std::size_t>(rank) - 1;
    if (index >= sorted.size())
    {
        index = sorted.size() - 1;
    }
    return sorted[index];
}

// Median, range and stability of the values of repeated runs
// Parameter values: one value per run
// Return value: the summary
inline spread summarize(const std::vector<double>& values)
{
    spread result;
    if (values.empty())
    {
        return result;
    }
    result.m_median = median(values);
    result.m_min = *std::min_element(values.begin(), values.end());
    result.m_max = *std::max_element(values.begin(), values.end());
    // Judge on the numbers as reported (3 decimals)
    const double shown_median = std::round(result.m_median * kReportedScale);
    const double width = std::round(result.m_max * kReportedScale) - std::round(result.m_min * kReportedScale);
    if (shown_median > 0.0)
    {
        result.m_stable = width <= kStabilityLimit * shown_median;
    }
    else
    {
        result.m_stable = width == 0.0;
    }
    return result;
}

// One CSV field, quoted when it holds a comma, a quote or a line break
// (quotes inside are doubled)
// Parameter field: the text
// Return value: the field as written to the file
inline std::string csv_field(const std::string& field)
{
    if (field.find_first_of(",\"\r\n") == std::string::npos)
    {
        return field;
    }
    std::string quoted = "\"";
    for (const char c : field)
    {
        if (c == '"')
        {
            quoted += "\"\"";
        }
        else
        {
            quoted += c;
        }
    }
    quoted += '"';
    return quoted;
}

// One CSV line from its fields (without the line end)
// Parameter fields: the fields in column order
// Return value: the line
inline std::string csv_line(const std::vector<std::string>& fields)
{
    std::string line;
    for (std::size_t i = 0; i < fields.size(); ++i)
    {
        if (i != 0)
        {
            line += ',';
        }
        line += csv_field(fields[i]);
    }
    return line;
}

// A number for the CSV and the console: fixed point, 3 decimals
// Parameter value: the number
// Return value: the text
inline std::string number_text(double value)
{
    char text[64] = {};
    std::snprintf(text, sizeof(text), "%.3f", value);
    return text;
}

// The processor's brand string, from CPUID on x86
// Return value: e.g. "Intel(R) Core(TM) i7-...", or "unknown cpu"
inline std::string cpu_name()
{
    char brand[kBrandLeaves * kCpuidRegisters * sizeof(unsigned) + 1] = {};
    unsigned registers[kCpuidRegisters] = {};
#if defined(SPANIO_BENCH_CPUID_MSVC)
    int info[kCpuidRegisters] = {};
    __cpuid(info, static_cast<int>(kBrandLeafFirst - 2u));
    if (static_cast<unsigned>(info[0]) < kBrandLeafFirst + kBrandLeaves - 1)
    {
        return "unknown cpu";
    }
    for (unsigned leaf = 0; leaf < kBrandLeaves; ++leaf)
    {
        __cpuid(info, static_cast<int>(kBrandLeafFirst + leaf));
        std::memcpy(registers, info, sizeof(registers));
        std::memcpy(brand + leaf * sizeof(registers), registers, sizeof(registers));
    }
#elif defined(SPANIO_BENCH_CPUID_GCC)
    if (__get_cpuid_max(kBrandLeafFirst - 2u, nullptr) < kBrandLeafFirst + kBrandLeaves - 1)
    {
        return "unknown cpu";
    }
    for (unsigned leaf = 0; leaf < kBrandLeaves; ++leaf)
    {
        __get_cpuid(kBrandLeafFirst + leaf, &registers[0], &registers[1], &registers[2], &registers[3]);
        std::memcpy(brand + leaf * sizeof(registers), registers, sizeof(registers));
    }
#else
    static_cast<void>(registers);
    return "unknown cpu";
#endif
    std::string name(brand);
    const std::size_t first = name.find_first_not_of(' ');
    const std::size_t last = name.find_last_not_of(' ');
    return first == std::string::npos ? "unknown cpu" : name.substr(first, last - first + 1);
}

// Operating system family
// Return value: "windows" or "linux" (or "other")
inline std::string platform_name()
{
#if defined(_WIN32)
    return "windows";
#elif defined(__linux__)
    return "linux";
#else
    return "other";
#endif
}

// Compiler and version
// Return value: e.g. "msvc 1944" or "gcc 13.3.0"
inline std::string compiler_name()
{
#if defined(_MSC_VER)
    return "msvc " + std::to_string(_MSC_VER);
#elif defined(__clang__)
    return "clang " __clang_version__;
#elif defined(__GNUC__)
    return "gcc " __VERSION__;
#else
    return "unknown compiler";
#endif
}

// Whether this build has optimizations on (a Debug benchmark is meaningless)
// Return value: true for a build without NDEBUG
inline bool debug_build()
{
#if defined(NDEBUG)
    return false;
#else
    return true;
#endif
}

// Print the machine description that belongs with a set of results
// Parameter out: destination
inline void print_environment(std::ostream& out)
{
    out << "spanio " << SPANIO_VERSION_STRING << ", " << platform_name() << ", " << compiler_name() << ", "
        << cpu_name() << ", " << std::thread::hardware_concurrency() << " hardware threads"
        << (debug_build() ? ", DEBUG BUILD (numbers are not meaningful)" : "") << "\n";
}

}

#endif // SPANIO_BENCHMARK_BENCH_COMMON_H
