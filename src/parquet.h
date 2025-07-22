#pragma once

#include "malloc.h"
#include "typing.h"

enum parquet_schema_type {
  PARQUET_SCHEMA_TYPE_BOOLEAN = 1,
  PARQUET_SCHEMA_TYPE_INT32 = 0,
  PARQUET_SCHEMA_TYPE_INT64 = 1,
  PARQUET_SCHEMA_TYPE_INT96 = 2,
  PARQUET_SCHEMA_TYPE_FLOAT = 3,
  PARQUET_SCHEMA_TYPE_DOUBLE = 4,
  PARQUET_SCHEMA_TYPE_BYTE_ARRAY = 5,
  PARQUET_SCHEMA_TYPE_BYTE_ARRAY_FIXED = 6,
  PARQUET_SCHEMA_TYPE_SIZE,
};

enum parquet_converted_type {
  PARQUET_CONVERTED_TYPE_UTF8 = 1,
  PARQUET_CONVERTED_TYPE_MAP = 2,
  PARQUET_CONVERTED_TYPE_MAP_KEY_VALUE = 3,
  PARQUET_CONVERTED_TYPE_LIST = 4,
  PARQUET_CONVERTED_TYPE_SIZE,
};

struct parquet_schema_element {
  char *name;       // name of the schema element
  i32 num_children; // number of children in the schema
  i32 type_length;  // if type is FIXED_BYTE_ARRAY, this is the length of the array

  enum parquet_schema_type type;              // data type for this field, set only in leaf-node
  enum parquet_converted_type converted_type; // the original type to help with cross conversion
};

struct parquet_metadata {
  i32 version;      // parquet file version
  i64 num_rows;     // number of rows
  char *created_by; // created by string, optional

  struct parquet_schema_element *schemas; // array of schema elements
  u32 schemas_size;                       // size of the schemas array
};

struct parquet_file {
  struct malloc_pool *pool; // memory pool for buffer allocation
  u32 fd;                   // file descriptor for the parquet file

  u64 footer_size;           // size of the footer in bytes
  char *footer_buffer;       // allocated buffer for the footer
  u64 footer_buffer_size;    // size of the allocated buffer
  char *footer_buffer_start; // pointer to the start of the footer
  char *footer_buffer_end;   // pointer to the end of the footer

  char *metadata_buffer;    // buffer for metadata
  u64 metadata_buffer_size; // size of the metadata buffer
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

/// @brief Parses the footer of a parquet file.
/// @param file Pointer to the parquet_file structure.
/// @param metadata Pointer to the parquet_metadata structure to fill.
/// @return 0 on success, or a negative error code on failure.
extern i64 parquet_parse(struct parquet_file *file, struct parquet_metadata *metadata);

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases(struct runner_context *ctx);
