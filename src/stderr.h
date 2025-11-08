#pragma once

#include "format.base.h"
#include "typing.h"

/// @brief Prints a formatted string to stderr (like printf).
/// @param fmt Format string.
extern void errorf(const char *fmt, ...);

/// @brief Flushes the stderr buffer.
/// @param ctx Pointer to the format context.
/// @return Number of bytes written, or a negative error code.
extern i64 stderr_flush(struct format_context *ctx);
