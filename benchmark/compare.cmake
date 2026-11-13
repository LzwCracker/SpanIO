# ============================================================================
# File purpose: Compares two result files of bench_spanio and reports every
#               metric that got worse by more than a threshold (default 10%),
#               taking into account whether larger or smaller is better.
#               Rows are matched by backend, scenario, parameters and metric;
#               rows present in only one file are listed but do not fail.
# Usage:        cmake -DBASE=baseline.csv -DNEW=current.csv [-DTHRESHOLD=10]
#                     -P benchmark/compare.cmake
#               Prints one line per metric:
#                 REGRESSION / IMPROVED / ok  <backend> <scenario> <params>
#                 <metric>: <base> -> <new> <unit> (<change>%)
#               Exits with an error when at least one metric regressed.
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-12
# ============================================================================

cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED BASE OR NOT DEFINED NEW)
    message(FATAL_ERROR "usage: cmake -DBASE=<old.csv> -DNEW=<new.csv> [-DTHRESHOLD=10] -P compare.cmake")
endif()
if(NOT DEFINED THRESHOLD)
    set(THRESHOLD 10)
endif()

# Columns of the result CSV (see kCsvHeader in bench_common.h)
set(column_backend 3)
set(column_scenario 4)
set(column_params 5)
set(column_metric 6)
set(column_unit 7)
set(column_better 8)
set(column_median 9)

# Split one CSV line into fields (quoted fields may hold commas and doubled
# quotes). Semicolons are kept as @SC@, so the result is a clean CMake list.
function(split_csv line out_var)
    string(REPLACE ";" "@SC@" rest "${line}")
    set(fields "")
    while(TRUE)
        if(rest MATCHES "^\"((\"\"|[^\"])*)\"(,|$)")
            string(REPLACE "\"\"" "\"" field "${CMAKE_MATCH_1}")
            string(LENGTH "${CMAKE_MATCH_0}" consumed)
        elseif(rest MATCHES "^([^,]*)(,|$)")
            set(field "${CMAKE_MATCH_1}")
            string(LENGTH "${CMAKE_MATCH_0}" consumed)
        else()
            message(FATAL_ERROR "cannot parse CSV line: ${line}")
        endif()
        list(APPEND fields "${field}")
        if(NOT CMAKE_MATCH_0 MATCHES ",$")
            break()
        endif()
        string(SUBSTRING "${rest}" ${consumed} -1 rest)
    endwhile()
    set(${out_var} "${fields}" PARENT_SCOPE)
endfunction()

# A decimal number such as "812.345" as an integer in thousandths, because
# CMake's math() only does integers
function(to_thousandths text out_var)
    if(NOT text MATCHES "^([0-9]+)(\\.([0-9]*))?$")
        message(FATAL_ERROR "not a number: ${text}")
    endif()
    set(whole "${CMAKE_MATCH_1}")
    set(fraction "${CMAKE_MATCH_3}000")
    string(SUBSTRING "${fraction}" 0 3 fraction)
    math(EXPR value "${whole} * 1000 + 1${fraction} - 1000")
    set(${out_var} ${value} PARENT_SCOPE)
endfunction()

# Read a result file into variables <prefix>_<key>_median / _better / _unit
# and a list <prefix>_keys of the keys in file order
function(read_results path prefix)
    file(STRINGS "${path}" lines)
    set(keys "")
    set(first TRUE)
    foreach(line IN LISTS lines)
        if(first)
            set(first FALSE)
            continue()
        endif()
        split_csv("${line}" fields)
        list(GET fields ${column_backend} backend)
        list(GET fields ${column_scenario} scenario)
        list(GET fields ${column_params} params)
        list(GET fields ${column_metric} metric)
        list(GET fields ${column_unit} unit)
        list(GET fields ${column_better} better)
        list(GET fields ${column_median} value)
        set(label "${backend} ${scenario} ${params} ${metric}")
        string(MAKE_C_IDENTIFIER "${label}" key)
        list(APPEND keys "${key}")
        to_thousandths("${value}" scaled)
        set(${prefix}_${key}_median ${scaled} PARENT_SCOPE)
        set(${prefix}_${key}_text "${value}" PARENT_SCOPE)
        set(${prefix}_${key}_better "${better}" PARENT_SCOPE)
        set(${prefix}_${key}_unit "${unit}" PARENT_SCOPE)
        string(REPLACE "@SC@" ";" printable "${label}")
        set(${prefix}_${key}_label "${printable}" PARENT_SCOPE)
    endforeach()
    set(${prefix}_keys "${keys}" PARENT_SCOPE)
endfunction()

read_results("${BASE}" base)
read_results("${NEW}" new)

set(regressions 0)
foreach(key IN LISTS new_keys)
    if(NOT DEFINED base_${key}_median)
        message(STATUS "NEW         ${new_${key}_label}: ${new_${key}_text} ${new_${key}_unit}")
        continue()
    endif()
    set(old ${base_${key}_median})
    set(now ${new_${key}_median})
    # Change in percent of the baseline (integer, rounded toward zero)
    if(old EQUAL 0)
        set(change 0)
    else()
        math(EXPR change "(${now} - ${old}) * 100 / ${old}")
    endif()
    # Gain: the change in the "better" direction (negative when worse)
    if(new_${key}_better STREQUAL "lower")
        math(EXPR gain "0 - ${change}")
    else()
        set(gain ${change})
    endif()
    math(EXPR limit "0 - ${THRESHOLD}")
    if(gain LESS limit)
        set(verdict "REGRESSION")
        math(EXPR regressions "${regressions} + 1")
    elseif(gain GREATER THRESHOLD)
        set(verdict "IMPROVED  ")
    else()
        set(verdict "ok        ")
    endif()
    message(STATUS "${verdict}  ${new_${key}_label}: ${base_${key}_text} -> ${new_${key}_text} ${new_${key}_unit} (${change}%)")
endforeach()
foreach(key IN LISTS base_keys)
    if(NOT DEFINED new_${key}_median)
        message(STATUS "MISSING     ${base_${key}_label}")
    endif()
endforeach()

if(regressions GREATER 0)
    message(FATAL_ERROR "${regressions} metric(s) regressed by more than ${THRESHOLD}%")
endif()
message(STATUS "no metric regressed by more than ${THRESHOLD}%")
