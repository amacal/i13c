#pragma once

#include "error.h"
#include "runner.h"

enum format_error {
  // indicates that the output buffer is too small
  FORMAT_ERROR_BUFFER_TOO_SMALL = FORMAT_ERROR_BASE - 0x01,
};

struct format_context {
  const char *fmt;   // format string
  char *buffer;      // output buffer
  u32 buffer_offset; // current offset in the buffer
  u32 buffer_size;   // size of the output buffer
  void **vargs;      // variable arguments
  u32 vargs_offset;  // offset in the variable arguments
  u32 vargs_max;     // maximum number of variable arguments
};

/// @brief Formats the output.
/// @param ctx Pointer to the context structure.
/// @return The number of bytes written to the buffer, or a negative error code.
extern i64 format(struct format_context *ctx);

#if defined(I13C_TESTS)

/// @brief Registers format test cases.
/// @param ctx Pointer to the runner_context structure.
extern void format_test_cases(struct runner_context *ctx);

#endif
