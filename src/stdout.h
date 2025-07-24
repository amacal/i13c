#pragma once

#include "runner.h"
#include "typing.h"

/// @brief Prints a formatted string to stdout (like writef).
/// @param fmt Format string.
extern void writef(const char *fmt, ...);

#if defined(I13C_TESTS)

/// @brief Registers stdout test cases.
/// @param ctx Pointer to the runner_context structure.
extern void stdout_test_cases(struct runner_context *ctx);

#endif
