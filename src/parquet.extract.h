#pragma once

#include "typing.h"

/// @brief Extract the binary slice of a Parquet file.
/// @param argc Number of command-line arguments.
/// @param argv Array of command-line argument strings.
/// @return 0 on success, or a negative error code on failure.
extern i32 parquet_extract(u32 argc, const char **argv);
