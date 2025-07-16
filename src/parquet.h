#pragma once

#include "malloc.h"
#include "typing.h"

struct parquet_file {
  u32 fd;                   // file descriptor for the parquet file
  char *buffer;             // allocated buffer for the footer
  char *buffer_start;       // pointer to the start of the footer
  char *buffer_end;         // pointer to the end of the footer
  u64 buffer_size;          // size of the allocated buffer
  u64 footer_size;          // size of the footer in bytes
  struct malloc_pool *pool; // memory pool for buffer allocation
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

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases(struct runner_context *ctx);
