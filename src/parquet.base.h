#pragma once

#include "arena.h"
#include "error.h"
#include "malloc.h"
#include "runner.h"
#include "typing.h"

#define PARQUET_UNKNOWN_VALUE -1
#define PARQUET_NULL_VALUE NULL

enum parquet_error {
  PARQUET_INVALID_ARGUMENTS = PARQUET_ERROR_BASE - 0x01,

  // indicates that the read field type is invalid
  PARQUET_ERROR_INVALID_TYPE = PARQUET_ERROR_BASE - 0x02,

  // indicates that the read field value is invalid
  PARQUET_ERROR_INVALID_VALUE = PARQUET_ERROR_BASE - 0x03,

  // indicates that the read file is invalid
  PARQUET_ERROR_INVALID_FILE = PARQUET_ERROR_BASE - 0x04,

  // indicates that the buffer is too small to hold the data
  PARQUET_ERROR_BUFFER_TOO_SMALL = PARQUET_ERROR_BASE - 0x05,

  // indicates that the metadata iterator has reached its capacity
  PARQUET_ERROR_CAPACITY_OVERFLOW = PARQUET_ERROR_BASE - 0x06,
};

struct parquet_footer {
  u64 size; // the size of the footer in bytes

  char *start; // pointer to the start of the footer
  char *end;   // pointer to the end of the footer

  struct malloc_lease lease; // lease for the footer memory
};

struct parquet_file {
  u32 fd;                   // file descriptor for the parquet file
  struct malloc_pool *pool; // memory pool for buffer allocation

  struct arena_allocator arena; // parse/schema allocator
  struct parquet_footer footer; // footer of the parquet file
};

/// @brief Initializes a parquet file structure.
/// @param file Pointer to the parquet_file structure.
/// @param pool Pointer to the malloc_pool structure for memory management.
extern void parquet_init(struct parquet_file *file, struct malloc_pool *pool);

/// @brief Opens a parquet file for reading.
/// @param file Pointer to the parquet_file structure.
/// @param path Path to the parquet file.
/// @return 0 on success, or a negative error code on failure.
extern i64 parquet_open(struct parquet_file *file, const char *path);

/// @brief Closes a parquet file.
/// @param file Pointer to the parquet_file structure.
extern void parquet_close(struct parquet_file *file);

#if defined(I13C_TESTS)

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases_base(struct runner_context *ctx);

#endif
