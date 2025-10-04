#pragma once

#include "arena.h"
#include "parquet.base.h"
#include "parquet.parse.h"
#include "typing.h"

// forward declaration
struct parquet_schema;

struct parquet_schema_array {
  u64 count;                        // number of elements in the array
  struct parquet_schema **elements; // array of schema elements
};

struct parquet_schema {
  char *name;                           // name of the schema element
  struct parquet_schema_array children; // nested children if any

  i32 repeated_type;  // distinguish between required and optional
  i32 data_type;      // physical type of the data
  i32 type_length;    // length of the type if fixed
  i32 converted_type; // logical type of the data
};

/// @brief Opens a parquet schema.
/// @param arena Pointer to the arena_allocator structure.
/// @param metadata Pointer to the parquet_schema_element array.
/// @param schema Pointer to the parquet_schema structure to fill.
/// @return 0 on success, or a negative error code on failure.
extern i64 parquet_open_schema(struct arena_allocator *arena,
                               struct parquet_schema_element **metadata,
                               struct parquet_schema *schema);

#if defined(I13C_TESTS)

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases_schema_open(struct runner_context *ctx);

#endif
