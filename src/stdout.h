#pragma once

#include "format.base.h"
#include "typing.h"

/// @brief Prints a formatted string to stdout (like printf).
/// @param fmt Format string.
extern void writef(const char *fmt, ...);

/// @brief Flushes the stdout buffer.
/// @param ctx Pointer to the format context.
/// @return Number of bytes written, or a negative error code.
extern i64 stdout_flush(struct format_context *ctx);
