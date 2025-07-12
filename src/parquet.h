#pragma once

#include "typing.h"

typedef struct {
  u32 fd;
} parquet_file;

/// @brief Initializes a Parquet file structure.
/// @param file Pointer to the parquet_file structure.
extern void parquet_init(parquet_file *file);

/// @brief Opens a Parquet file for reading.
/// @param file Pointer to the parquet_file structure.
/// @param path Path to the Parquet file.
/// @return 0 on success, or a negative error code on failure.
extern i64 parquet_open(parquet_file *file, const char* path);

/// @brief Closes a Parquet file.
/// @param file Pointer to the parquet_file structure.
extern void parquet_close(parquet_file *file);
