#pragma once

#include "typing.h"

/// @brief Prints a text to stdout.
/// @param len Length of the text.
/// @param data Pointer to the text.
/// @return 0 on success, or a negative error code on failure.
extern i64 stdout_print(u64 len, const char* data);

/// @brief Prints a formatted string to stdout (like printf).
/// @param fmt Format string.
/// @return 0 on success, or a negative error code on failure.
extern i64 stdout_printf(const char* fmt, ...);
