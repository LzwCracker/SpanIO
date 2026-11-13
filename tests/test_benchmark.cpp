// ============================================================================
// File purpose: Tests of the benchmark program itself (benchmark/): the
//               statistics, the stability flag, the command line, the CSV
//               format, every scenario on a short window, detection of
//               corrupted echoes, the latency samples, and the comparison
//               script benchmark/compare.cmake. The benchmark source is
//               included with SPANIO_BENCH_NO_MAIN defined, so the tests run
//               the shipped code.
// Usage:        Built as the test_benchmark executable by CMake. Run it with
//               no arguments for every case, or with a substring of a case
//               name, e.g. `test_benchmark csv`.
//
//               The comparison case runs CMake; tests/CMakeLists.txt passes
//               its path, the script's path and a scratch directory as
//               SPANIO_CMAKE_COMMAND, SPANIO_COMPARE_SCRIPT and
//               SPANIO_TEST_WORK_DIR.
// Developer:    lzwcracker@gmail.com
// Date:         2026-11-12
// ============================================================================

#define SPANIO_BENCH_NO_MAIN
#include "../benchmark/bench_spanio.cpp"

#include "test_framework.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

// Measuring window of the scenario cases, seconds
constexpr double kShortWindow = 0.2;

// Warm-up of the scenario cases, seconds
constexpr double kShortWarmup = 0.05;

// Columns of the result CSV
constexpr std::size_t kCsvColumns = 13;

// Connections / senders of the smoke configurations
constexpr std::size_t kSmokeConns = 2;

// Message size of the TCP smoke configurations
constexpr std::size_t kSmokeSize = 1024;

// Datagram and latency message size of the smoke configurations
constexpr std::size_t kSmallSize = 64;

// Threads of the engine smoke configurations
constexpr unsigned kSmokeThreads = 2;

// Round trips per connection by which the samples may differ from the
// separate count at the window's edges
constexpr std::uint64_t kEdgeRoundTrips = 2;

// Options for a short run of one scenario
// Return value: the options
bench::options short_options()
{
    bench::options opts;
    opts.m_seconds = kShortWindow;
    opts.m_warmup = kShortWarmup;
    opts.m_runs = 1;
    return opts;
}

// A small configuration for a scenario
// Parameter name: scenario name
// Return value: the configuration
bench::config smoke_config(const std::string& name)
{
    bench::config cfg;
    cfg.m_conns = kSmokeConns;
    cfg.m_size = kSmokeSize;
    cfg.m_threads = 1;
    if (name == "tcp_latency" || name == "udp_throughput")
    {
        cfg.m_size = kSmallSize;
    }
    if (name == "post_rate" || name == "strand_rate")
    {
        cfg.m_threads = kSmokeThreads;
    }
    return cfg;
}

// Number of argv entries of an array
// Parameter args: the array
// Return value: N
template <std::size_t N>
int argc_of(const char* const (&args)[N])
{
    static_cast<void>(args);
    return static_cast<int>(N);
}

// A result row for the CSV and comparison cases
// Parameter metric_name: metric
// Parameter unit:        unit
// Parameter higher:      whether larger is better
// Parameter value:       median
// Return value: the row
bench::report_row make_row(const std::string& metric_name, const std::string& unit, bool higher, double value)
{
    bench::report_row row;
    row.m_backend = "test-backend";
    row.m_scenario = "tcp_throughput";
    row.m_params = "conns=1;size=2;threads=3";
    row.m_metric = metric_name;
    row.m_unit = unit;
    row.m_higher_better = higher;
    row.m_spread.m_median = value;
    row.m_spread.m_min = value;
    row.m_spread.m_max = value;
    return row;
}

// Write a result file
// Parameter path: file
// Parameter rows: its rows
void write_results(const std::string& path, const std::vector<bench::report_row>& rows)
{
    std::ofstream file(path, std::ios::binary);
    file << bench::kCsvHeader << "\n";
    for (const bench::report_row& row : rows)
    {
        file << bench::row_csv(row) << "\n";
    }
}

// Run the comparison script on two files
// Parameter base:   baseline file
// Parameter now:    new file
// Parameter output: receives what the script printed
// Return value: the script's exit status (0 when nothing regressed)
int run_compare(const std::string& base, const std::string& now, std::string& output)
{
    const std::string log = std::string(SPANIO_TEST_WORK_DIR) + "/compare_output.txt";
    std::string command = "\"" + std::string(SPANIO_CMAKE_COMMAND) + "\" -DBASE=\"" + base + "\" -DNEW=\"" + now
                        + "\" -P \"" + std::string(SPANIO_COMPARE_SCRIPT) + "\" > \"" + log + "\" 2>&1";
#if defined(_WIN32)
    // cmd.exe strips the outer quotes of a command that starts with one
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    std::ifstream file(log);
    std::stringstream text;
    text << file.rdbuf();
    output = text.str();
    return status;
}

// Number of times a text occurs in another
// Parameter haystack: text searched
// Parameter needle:   text counted
// Return value: the count
std::size_t occurrences(const std::string& haystack, const std::string& needle)
{
    std::size_t count = 0;
    for (std::size_t at = haystack.find(needle); at != std::string::npos; at = haystack.find(needle, at + 1))
    {
        ++count;
    }
    return count;
}

}

// Case 1: median and nearest-rank percentiles
SPANIO_TEST(percentiles)
{
    SPANIO_CHECK(bench::median({3.0, 1.0, 2.0}) == 2.0);
    SPANIO_CHECK(bench::median({4.0, 1.0, 3.0, 2.0}) == 2.5);
    SPANIO_CHECK(bench::median({}) == 0.0);
    SPANIO_CHECK(bench::median({7.0}) == 7.0);

    std::vector<double> hundred;
    for (int i = 1; i <= 100; ++i)
    {
        hundred.push_back(static_cast<double>(i));
    }
    SPANIO_CHECK(bench::percentile(hundred, 50.0) == 50.0);
    SPANIO_CHECK(bench::percentile(hundred, 99.0) == 99.0);
    SPANIO_CHECK(bench::percentile(hundred, 99.9) == 100.0);
    SPANIO_CHECK(bench::percentile(hundred, 0.0) == 1.0);
    SPANIO_CHECK(bench::percentile(hundred, 100.0) == 100.0);
    SPANIO_CHECK(bench::percentile({7.0}, 99.9) == 7.0);
    SPANIO_CHECK(bench::percentile({}, 50.0) == 0.0);
}

// Case 2: a run set is unstable when max - min exceeds 10% of the median
SPANIO_TEST(stability_flag)
{
    const bench::spread steady = bench::summarize({100.0, 105.0, 108.0});
    SPANIO_CHECK(steady.m_stable);
    SPANIO_CHECK(steady.m_median == 105.0);
    SPANIO_CHECK(steady.m_min == 100.0);
    SPANIO_CHECK(steady.m_max == 108.0);
    SPANIO_CHECK(bench::summarize({100.0, 110.0, 100.0}).m_stable);
    SPANIO_CHECK(!bench::summarize({100.0, 95.0, 111.0}).m_stable);
    SPANIO_CHECK(bench::summarize({0.0, 0.0}).m_stable);
    SPANIO_CHECK(!bench::summarize({0.0, 1.0}).m_stable);
    // Judged as reported (3 decimals): differences that print as 0.000 do
    // not make a row unstable, visible ones do
    SPANIO_CHECK(bench::summarize({0.0, 0.0001, 0.0004}).m_stable);
    SPANIO_CHECK(!bench::summarize({0.0, 0.0, 0.047}).m_stable);
}

// Case 3: the command line: defaults, every option, and rejected input
SPANIO_TEST(arguments)
{
    bench::options opts;
    std::string error;

    const char* const none[] = {"bench_spanio"};
    SPANIO_REQUIRE(bench::parse_arguments(argc_of(none), none, opts, error));
    SPANIO_CHECK(opts.m_scenario.empty());
    SPANIO_CHECK(opts.m_conns == 0);
    SPANIO_CHECK(opts.m_seconds == bench::kDefaultSeconds);
    SPANIO_CHECK(opts.m_warmup == bench::kDefaultWarmup);
    SPANIO_CHECK(opts.m_runs == bench::kDefaultRuns);

    const char* const full[] = {"bench_spanio", "tcp_latency", "--conns", "4", "--size", "128", "--threads", "2",
                                "--seconds", "0.5", "--warmup", "0", "--runs", "3", "--backend", "x",
                                "--csv", "out.csv"};
    SPANIO_REQUIRE(bench::parse_arguments(argc_of(full), full, opts, error));
    SPANIO_CHECK(opts.m_scenario == "tcp_latency");
    SPANIO_CHECK(opts.m_conns == 4);
    SPANIO_CHECK(opts.m_size == 128);
    SPANIO_CHECK(opts.m_threads == 2);
    SPANIO_CHECK(opts.m_seconds == 0.5);
    SPANIO_CHECK(opts.m_warmup == 0.0);
    SPANIO_CHECK(opts.m_runs == 3);
    SPANIO_CHECK(opts.m_backend == "x");
    SPANIO_CHECK(opts.m_csv == "out.csv");

    // Explicit values replace a dimension of the default set
    const std::vector<bench::config> configs = bench::default_configs("tcp_latency", opts);
    SPANIO_REQUIRE(configs.size() == 1);
    SPANIO_CHECK(configs.front().m_conns == 4);
    SPANIO_CHECK(configs.front().m_size == 128);
    SPANIO_CHECK(bench::default_configs("tcp_throughput", bench::options()).size() == 9);

    const std::vector<std::vector<const char*>> bad = {
        {"bench_spanio", "no_such_scenario"},
        {"bench_spanio", "tcp_latency", "udp_throughput"},
        {"bench_spanio", "--verbose", "1"},
        {"bench_spanio", "--conns"},
        {"bench_spanio", "--conns", "0"},
        {"bench_spanio", "--conns", "abc"},
        {"bench_spanio", "--size", "-5"},
        {"bench_spanio", "--seconds", "0"},
        {"bench_spanio", "--warmup", "-1"},
        {"bench_spanio", "--runs", "0"},
        {"bench_spanio", "--threads", "1x"},
    };
    for (const std::vector<const char*>& args : bad)
    {
        error.clear();
        SPANIO_CHECK(!bench::parse_arguments(static_cast<int>(args.size()), args.data(), opts, error));
        SPANIO_CHECK(!error.empty());
    }
}

// Case 4: the CSV header, quoting, and the line of a result row
SPANIO_TEST(csv_format)
{
    const std::string header = bench::kCsvHeader;
    SPANIO_CHECK(occurrences(header, ",") + 1 == kCsvColumns);
    SPANIO_CHECK(bench::csv_field("plain") == "plain");
    SPANIO_CHECK(bench::csv_field("a,b") == "\"a,b\"");
    SPANIO_CHECK(bench::csv_field("say \"hi\"") == "\"say \"\"hi\"\"\"");
    SPANIO_CHECK(bench::csv_field("two\nlines") == "\"two\nlines\"");
    SPANIO_CHECK(bench::csv_line({"a", "b,c", ""}) == "a,\"b,c\",");
    SPANIO_CHECK(bench::number_text(1.5) == "1.500");

    bench::report_row row = make_row("throughput", "MB/s", true, 812.3456);
    row.m_spread.m_min = 800.0;
    row.m_spread.m_stable = false;
    const std::string line = bench::row_csv(row);
    SPANIO_CHECK(line.find(",test-backend,tcp_throughput,conns=1;size=2;threads=3,throughput,MB/s,higher,812.346,"
                           "800.000,812.346,no")
                 != std::string::npos);
    SPANIO_CHECK(line.compare(0, std::strlen(SPANIO_VERSION_STRING), SPANIO_VERSION_STRING) == 0);
}

// Case 5: every scenario runs on a short window on every backend it covers,
// produces a positive main figure and sees no data errors
SPANIO_TEST(scenarios_smoke)
{
    const bench::options opts = short_options();
    const std::vector<bench::backend> all = bench::backends();
    for (const std::string& name : bench::scenario_names())
    {
        for (std::size_t b = 0; b < all.size(); ++b)
        {
            if (b != 0 && !bench::network_scenario(name))
            {
                continue;
            }
            std::printf("    [%s / %s]\n", name.c_str(), all[b].m_name);
            const bench::run_result result = bench::run_scenario(name, all[b], smoke_config(name), opts);
            SPANIO_REQUIRE(!result.m_metrics.empty());
            SPANIO_CHECK(result.m_metrics.front().m_value > 0.0);
            SPANIO_CHECK(result.m_data_errors == 0);
        }
    }
}

// Case 6: an echo server that corrupts data is caught by both throughput
// scenarios
SPANIO_TEST(throughput_detects_corruption)
{
    bench::options opts = short_options();
    auto flip_first_byte = [](char* data, std::size_t size)
    {
        if (size > 0)
        {
            data[0] = static_cast<char>(data[0] ^ 1);
        }
    };
    opts.m_tamper = flip_first_byte;
    const bench::backend which = bench::backends().front();
    const bench::config cfg = smoke_config("tcp_throughput");
    SPANIO_CHECK(bench::run_scenario("tcp_throughput", which, cfg, opts).m_data_errors > 0);
    SPANIO_CHECK(bench::run_scenario("tcp_server_throughput", which, cfg, opts).m_data_errors > 0);
    SPANIO_CHECK(bench::run_scenario("tcp_latency", which, smoke_config("tcp_latency"), opts).m_data_errors > 0);
}

// Case 7: the latency scenario keeps one sample per round trip of the window
// (checked against a separate count; the window's edges may add or drop one
// round trip per connection), and its percentiles are ordered
SPANIO_TEST(latency_samples)
{
    const bench::options opts = short_options();
    bench::config cfg = smoke_config("tcp_latency");
    cfg.m_conns = 1;
    const bench::run_result result = bench::run_scenario("tcp_latency", bench::backends().front(), cfg, opts);
    SPANIO_REQUIRE(result.m_metrics.size() >= 4);
    SPANIO_CHECK(result.m_samples > 0);
    const std::uint64_t samples = result.m_samples;
    const std::uint64_t edge = kEdgeRoundTrips * cfg.m_conns;
    SPANIO_CHECK(samples + edge >= result.m_round_trips);
    SPANIO_CHECK(samples <= result.m_round_trips + edge);
    const double p50 = result.m_metrics[0].m_value;
    const double p99 = result.m_metrics[1].m_value;
    const double p999 = result.m_metrics[2].m_value;
    SPANIO_CHECK(result.m_metrics[0].m_name == "p50");
    SPANIO_CHECK(p50 > 0.0);
    SPANIO_CHECK(p50 <= p99);
    SPANIO_CHECK(p99 <= p999);
    SPANIO_CHECK(!result.m_metrics[0].m_higher_better);
    SPANIO_CHECK(result.m_metrics[3].m_value > 0.0);
}

// Case 8: the comparison script flags metrics that got more than 10% worse
// (smaller for "higher", larger for "lower"), and passes otherwise
SPANIO_TEST(compare_script)
{
    const std::string dir = SPANIO_TEST_WORK_DIR;
    const std::string base = dir + "/bench_base.csv";
    const std::string worse = dir + "/bench_worse.csv";
    const std::string better = dir + "/bench_better.csv";
    write_results(base, {make_row("throughput", "MB/s", true, 100.0), make_row("p99", "us", false, 50.0),
                         make_row("handlers", "handler/s", true, 200.0)});
    write_results(worse, {make_row("throughput", "MB/s", true, 85.0), make_row("p99", "us", false, 60.0),
                          make_row("handlers", "handler/s", true, 205.0)});
    write_results(better, {make_row("throughput", "MB/s", true, 120.0), make_row("p99", "us", false, 40.0),
                           make_row("handlers", "handler/s", true, 195.0)});

    std::string output;
    SPANIO_CHECK(run_compare(base, worse, output) != 0);
    SPANIO_CHECK(occurrences(output, "REGRESSION") == 2);
    SPANIO_CHECK(output.find("throughput: 100.000 -> 85.000") != std::string::npos);
    SPANIO_CHECK(output.find("p99: 50.000 -> 60.000") != std::string::npos);
    SPANIO_CHECK(output.find("ok") != std::string::npos);
    if (occurrences(output, "REGRESSION") != 2)
    {
        std::printf("%s\n", output.c_str());
    }

    SPANIO_CHECK(run_compare(base, better, output) == 0);
    SPANIO_CHECK(occurrences(output, "REGRESSION") == 0);
    SPANIO_CHECK(occurrences(output, "IMPROVED") == 2);
    if (occurrences(output, "IMPROVED") != 2)
    {
        std::printf("%s\n", output.c_str());
    }
}

int main(int argc, char** argv)
{
    return spanio::test::run_all(argc, argv);
}
