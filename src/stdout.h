#pragma once

#include "error.h"
#include "runner.h"
#include "typing.h"

enum stdout_error {
  // indicates that the output buffer is too small
  STDOUT_ERROR_BUFFER_TOO_SMALL = FORMAT_ERROR_BASE | 0x01,
};

/// @brief Prints a formatted string to stdout (like writef).
/// @param fmt Format string.
extern void writef(const char *fmt, ...);

#if defined(I13C_TESTS)

/// @brief Registers stdout test cases.
/// @param ctx Pointer to the runner_context structure.
extern void stdout_test_cases(struct runner_context *ctx);

#endif
