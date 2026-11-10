# ============================================================================
# File purpose: Checks that every code snippet of docs/guide.md is exactly
#               the code compiled and run by tests/test_guide.cpp.
#               - In the guide, a snippet is a ```cpp block right after a
#                 line "<!-- snippet: <name> -->".
#               - In the test, it is the lines between "// [snippet <name>]"
#                 and "// [/snippet]".
#               Both are compared after removing their common indentation,
#               trailing spaces and blank lines at either end. The check
#               fails when a guide snippet is missing from the test or
#               differs, when a test snippet is not shown in the guide, or
#               when a name is used twice. Plain ```cpp blocks without a
#               marker are not checked; keep them to a few lines.
# Usage:        cmake -DGUIDE=docs/guide.md -DSOURCE=tests/test_guide.cpp
#                     -P tests/check_guide_snippets.cmake
#               Registered with CTest as guide_snippets_match.
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-10
# ============================================================================

cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED GUIDE OR NOT DEFINED SOURCE)
    message(FATAL_ERROR "usage: cmake -DGUIDE=<guide.md> -DSOURCE=<test_guide.cpp> -P check_guide_snippets.cmake")
endif()

# Read a file as a CMake list of lines. Characters that CMake lists treat
# specially (; [ ] \) are replaced by placeholders first, and '@' too so the
# placeholders stay unambiguous; restore_text() undoes it for messages.
function(read_lines path out_var)
    file(READ "${path}" text)
    string(REPLACE "\r" "" text "${text}")
    string(REPLACE "@" "@AT@" text "${text}")
    string(REPLACE ";" "@SC@" text "${text}")
    string(REPLACE "[" "@LB@" text "${text}")
    string(REPLACE "]" "@RB@" text "${text}")
    string(REPLACE "\\" "@BS@" text "${text}")
    string(REPLACE "\n" ";" text "${text}")
    set(${out_var} "${text}" PARENT_SCOPE)
endfunction()

# Undo the placeholders of read_lines(), for printing
function(restore_text text out_var)
    string(REPLACE "@BS@" "\\" text "${text}")
    string(REPLACE "@RB@" "]" text "${text}")
    string(REPLACE "@LB@" "[" text "${text}")
    string(REPLACE "@SC@" ";" text "${text}")
    string(REPLACE "@AT@" "@" text "${text}")
    set(${out_var} "${text}" PARENT_SCOPE)
endfunction()

# Normalize a block given as text with "\n" line ends: drop trailing spaces,
# remove the indentation common to all non-blank lines, and drop blank lines
# at the start and the end
function(normalize_block text out_var)
    string(REPLACE "\n" ";" lines "${text}")
    set(indent 1000000)
    foreach(line IN LISTS lines)
        string(REGEX REPLACE " +$" "" trimmed "${line}")
        if(NOT trimmed STREQUAL "")
            # "^ +", not "^ *": CMake before 3.29 rejects a match of an empty string
            string(REGEX MATCH "^ +" lead "${trimmed}")
            string(LENGTH "${lead}" width)
            if(width LESS indent)
                set(indent ${width})
            endif()
        endif()
    endforeach()
    set(result "")
    foreach(line IN LISTS lines)
        string(REGEX REPLACE " +$" "" trimmed "${line}")
        if(trimmed STREQUAL "")
            string(APPEND result "\n")
        else()
            string(SUBSTRING "${trimmed}" ${indent} -1 trimmed)
            string(APPEND result "${trimmed}\n")
        endif()
    endforeach()
    string(REGEX REPLACE "^\n+" "" result "${result}")
    string(REGEX REPLACE "\n+$" "" result "${result}")
    set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

set(failures 0)

# ---- Snippets of the test source --------------------------------------------
read_lines("${SOURCE}" source_lines)
set(source_names "")
set(current "")
foreach(line IN LISTS source_lines)
    if(current STREQUAL "")
        if(line MATCHES "^ *// @LB@snippet ([A-Za-z0-9_]+)@RB@ *$")
            set(current "${CMAKE_MATCH_1}")
            if("${current}" IN_LIST source_names)
                message(SEND_ERROR "test_guide.cpp: snippet '${current}' is defined twice")
                math(EXPR failures "${failures} + 1")
            endif()
            list(APPEND source_names "${current}")
            set(block "")
        endif()
    elseif(line MATCHES "^ *// @LB@/snippet@RB@ *$")
        normalize_block("${block}" normalized)
        set("source_${current}" "${normalized}")
        set(current "")
    else()
        string(APPEND block "${line}\n")
    endif()
endforeach()
if(NOT current STREQUAL "")
    message(SEND_ERROR "test_guide.cpp: snippet '${current}' has no closing // [/snippet]")
    math(EXPR failures "${failures} + 1")
endif()

# ---- Snippets of the guide ----------------------------------------------------
read_lines("${GUIDE}" guide_lines)
set(guide_names "")
set(state "idle")
set(pending "")
set(line_number 0)
foreach(line IN LISTS guide_lines)
    math(EXPR line_number "${line_number} + 1")
    if(state STREQUAL "idle")
        if(line MATCHES "^<!-- snippet: ([A-Za-z0-9_]+) -->$")
            set(pending "${CMAKE_MATCH_1}")
            set(state "fence")
        endif()
    elseif(state STREQUAL "fence")
        if(line MATCHES "^```cpp$")
            set(state "code")
            set(block "")
        elseif(NOT line STREQUAL "")
            message(SEND_ERROR "guide.md:${line_number}: snippet '${pending}' is not followed by a ```cpp block")
            math(EXPR failures "${failures} + 1")
            set(state "idle")
        endif()
    elseif(state STREQUAL "code")
        if(line MATCHES "^```$")
            normalize_block("${block}" normalized)
            if("${pending}" IN_LIST guide_names)
                message(SEND_ERROR "guide.md: snippet '${pending}' is shown twice")
                math(EXPR failures "${failures} + 1")
            endif()
            list(APPEND guide_names "${pending}")
            if(NOT "${pending}" IN_LIST source_names)
                message(SEND_ERROR "guide.md: snippet '${pending}' does not exist in test_guide.cpp")
                math(EXPR failures "${failures} + 1")
            elseif(NOT normalized STREQUAL "${source_${pending}}")
                restore_text("${normalized}" shown)
                restore_text("${source_${pending}}" tested)
                message(SEND_ERROR "snippet '${pending}' differs\n--- guide.md ---\n${shown}\n--- test_guide.cpp ---\n${tested}\n")
                math(EXPR failures "${failures} + 1")
            endif()
            set(state "idle")
        else()
            string(APPEND block "${line}\n")
        endif()
    endif()
endforeach()

foreach(name IN LISTS source_names)
    if(NOT "${name}" IN_LIST guide_names)
        message(SEND_ERROR "test_guide.cpp: snippet '${name}' is not shown in guide.md")
        math(EXPR failures "${failures} + 1")
    endif()
endforeach()

list(LENGTH guide_names checked)
if(failures GREATER 0)
    message(FATAL_ERROR "${failures} snippet problem(s)")
endif()
message(STATUS "guide snippets match: ${checked} checked")
