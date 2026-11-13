# ============================================================================
# File purpose: Builds the benchmark program bench_spanio when
#               SPANIO_BUILD_BENCHMARKS is on (off by default, so normal builds
#               stay fast). Benchmark numbers only mean something in a Release
#               build; the program says so when it runs in Debug.
# Usage:        cmake -S . -B build -DSPANIO_BUILD_BENCHMARKS=ON
#               cmake --build build --config Release --target bench_spanio
#               build/benchmark/Release/bench_spanio --csv results.csv
#               Compare two result files with
#               cmake -DBASE=old.csv -DNEW=new.csv -P benchmark/compare.cmake
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-12
# ============================================================================

add_executable(bench_spanio bench_spanio.cpp bench_common.h)
target_link_libraries(bench_spanio PRIVATE spanio::spanio)
target_compile_options(bench_spanio PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX /permissive- /utf-8>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic -Werror>)
# Show the benchmark under a "benchmark" solution folder in Visual Studio
set_target_properties(bench_spanio PROPERTIES FOLDER "benchmark")
