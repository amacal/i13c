#pragma once

#include "runner.h"
#include "typing.h"

#define ERROR_BASE -256
#define ERROR_BLOCK_SIZE 16
#define ERROR_NAME_MAX_LENGTH 16

#define THRIFT_ERROR_BASE (ERROR_BASE - 0 * ERROR_BLOCK_SIZE)
#define THRIFT_ERROR_NAME "thrift"

#define MALLOC_ERROR_BASE (ERROR_BASE - 1 * ERROR_BLOCK_SIZE)
#define MALLOC_ERROR_NAME "malloc"

#define PARQUET_ERROR_BASE (ERROR_BASE - 2 * ERROR_BLOCK_SIZE)
#define PARQUET_ERROR_NAME "parquet"

#define DOM_ERROR_BASE (ERROR_BASE - 3 * ERROR_BLOCK_SIZE)
#define DOM_ERROR_NAME "dom"

#define FORMAT_ERROR_BASE (ERROR_BASE - 4 * ERROR_BLOCK_SIZE)
#define FORMAT_ERROR_NAME "format"

#define ARENA_ERROR_BASE (ERROR_BASE - 5 * ERROR_BLOCK_SIZE)
#define ARENA_ERROR_NAME "arena"

#define ARGV_ERROR_BASE (ERROR_BASE - 6 * ERROR_BLOCK_SIZE)
#define ARGV_ERROR_NAME "argv"

#define ERROR_BASE_MAX ARGV_ERROR_BASE

/// @brief Converts a result to a string representation.
/// @param result Result value to convert.
/// @return Pointer to the string representation of the result value, or NULL if the value is not recognized.
extern const char *res2str(i64 result);

/// @brief Truncates a result to an error offset.
/// @param result Result value to truncate.
/// @return Truncated error offset.
extern i64 res2off(i64 result);

#if defined(I13C_TESTS)

/// @brief Registers error test cases.
/// @param ctx Pointer to the runner_context structure.
extern void error_test_cases(struct runner_context *ctx);

#endif
