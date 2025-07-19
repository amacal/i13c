#pragma once

#include "typing.h"

/// @brief Read data from standard input.
/// @param buffer The buffer to read data into.
/// @param size The number of bytes to read.
/// @return The number of bytes read, or negative on error.
extern i64 stdin_read(void *buffer, u64 size);
