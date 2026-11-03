# ============================================================================
# File purpose: Top-level build script. Declares the header-only spanio
#               library as an INTERFACE target (spanio::spanio) that carries
#               the include path, C++17 requirement, platform libraries and
#               feature macros, and optionally builds the unit tests.
# Usage:        cmake -S . -B build
#               cmake --build build --config Debug
#               ctest --test-dir build -C Debug --output-on-failure
# Developer:    lzwcracker@gmail.com
# Date:         2026-11-03
# ============================================================================

cmake_minimum_required(VERSION 3.15)

project(SpanIO VERSION 0.1.0 LANGUAGES CXX)

# Tests are built by default only when SpanIO is the top-level project, so
# projects that pull SpanIO in with add_subdirectory do not build them
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(SPANIO_IS_TOP_LEVEL ON)
else()
    set(SPANIO_IS_TOP_LEVEL OFF)
endif()

option(SPANIO_BUILD_TESTS "Build the SpanIO unit tests" ${SPANIO_IS_TOP_LEVEL})

# Feature switches. Each maps to the macro of the same name and is off by
# default to keep the core small. Options for SSL, io_uring and coroutines
# will be added together with those modules.
option(SPANIO_ENABLE_LOG "Enable the built-in lightweight logging" OFF)

find_package(Threads REQUIRED)

add_library(spanio INTERFACE)
add_library(spanio::spanio ALIAS spanio)

target_include_directories(spanio INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_compile_features(spanio INTERFACE cxx_std_17)

# The event loop and thread pool need the platform thread library
target_link_libraries(spanio INTERFACE Threads::Threads)

# Windows sockets live in ws2_32 (MSVC also gets it via #pragma comment;
# this covers MinGW)
if(WIN32)
    target_link_libraries(spanio INTERFACE ws2_32)
endif()

if(SPANIO_ENABLE_LOG)
    target_compile_definitions(spanio INTERFACE SPANIO_ENABLE_LOG)
endif()

if(SPANIO_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ----------------------------------------------------------------------------
# IDE support (Visual Studio solution). An INTERFACE library has no project of
# its own, so these targets exist only to make the headers and docs visible
# and editable in Solution Explorer. They build nothing.
# ----------------------------------------------------------------------------
if(SPANIO_IS_TOP_LEVEL)
    # Group targets into solution folders (spanio / tests / CMakePredefinedTargets)
    set_property(GLOBAL PROPERTY USE_FOLDERS ON)

    # CONFIGURE_DEPENDS re-runs CMake when headers are added or removed, so
    # new files appear in the solution after the next build
    file(GLOB_RECURSE SPANIO_HEADER_FILES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")
    add_custom_target(spanio_headers SOURCES ${SPANIO_HEADER_FILES})
    # Mirror the include/ directory tree in Solution Explorer
    source_group(TREE "${CMAKE_CURRENT_SOURCE_DIR}/include"
        PREFIX "include" FILES ${SPANIO_HEADER_FILES})
    set_target_properties(spanio_headers PROPERTIES FOLDER "spanio")

    file(GLOB SPANIO_DOC_FILES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/docs/*.md")
    set(SPANIO_PROJECT_FILES
        ${SPANIO_DOC_FILES}
        "${CMAKE_CURRENT_SOURCE_DIR}/CLAUDE.md"
        "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt")
    add_custom_target(spanio_docs SOURCES ${SPANIO_PROJECT_FILES})
    source_group(TREE "${CMAKE_CURRENT_SOURCE_DIR}" FILES ${SPANIO_PROJECT_FILES})
    set_target_properties(spanio_docs PROPERTIES FOLDER "spanio")

    # F5 in Visual Studio runs the unit tests instead of ALL_BUILD
    if(SPANIO_BUILD_TESTS)
        set_property(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
            PROPERTY VS_STARTUP_PROJECT test_socket_ops)
    endif()
endif()
