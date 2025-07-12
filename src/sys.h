#pragma once

#include "typing.h"

/// @brief Exits the program with the given status code.
/// @param status Exit status code.
extern void sys_exit(i32 status);

/// @brief Writes data to a file descriptor.
/// @param fd File descriptor to write to.
/// @param buf Buffer containing data to write.
/// @param count Number of bytes to write.
extern i64 sys_write(i32 fd, const char *buf, u64 count);
