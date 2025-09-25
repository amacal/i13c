#pragma once

#include "error.h"
#include "runner.h"
#include "typing.h"

enum argv_error {
  // indicates that no matching command was found
  ARGV_ERROR_NO_MATCH = ARGV_ERROR_BASE - 0x01,
};

/// @brief Command match callback function type.
/// @param argc Number of command-line arguments.
/// @param argv Array of command-line argument strings.
/// @return 0 on success, or a negative error code on failure.
typedef i32 (*argv_match_fn)(u32 argc, const char **argv);

/// @brief Matches command-line arguments against a list of known commands.
/// @param argc Number of command-line arguments.
/// @param argv Array of command-line argument strings.
/// @param commands NULL-terminated array of command strings to match against.
/// @param selected Pointer to a variable where the index of the matched command will be stored.
/// @return 0 on success, or a negative error code on failure.
extern i64 argv_match(u32 argc, const char **argv, const char **commands, u64 *selected);
