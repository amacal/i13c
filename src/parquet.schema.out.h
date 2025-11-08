#pragma once

#include "format.base.h"
#include "malloc.h"
#include "parquet.schema.open.h"
#include "runner.h"
#include "typing.h"
#include "vargs.h"

#define PARQUET_SCHEMA_OUT_CONTEXT_MAX_DEPTH 10
#define PARQUET_SCHEMA_OUT_CONTEXT_MAX_VARGS 8

struct parquet_schema_out_context {
  u32 depth;                                         // current depth in the schema
  u32 indices[PARQUET_SCHEMA_OUT_CONTEXT_MAX_DEPTH]; // current index in each depth

  struct parquet_schema **stack[PARQUET_SCHEMA_OUT_CONTEXT_MAX_DEPTH]; // stack of array of schema elements
  struct parquet_schema *root[2];                                      // root schema element array
};

struct parquet_schema_out_state {
  void *vargs[PARQUET_SCHEMA_OUT_CONTEXT_MAX_VARGS]; // variable arguments
  struct malloc_lease *buffer;                       // allocated output

  struct format_context fmt;             // format context
  struct parquet_schema_out_context ctx; // internal context
};

/// @brief Initializes the parquet schema output context.
/// @param state Pointer to the parquet_schema_out_state structure.
/// @param buffer Pointer to the allocated output buffer.
/// @param schema Pointer to the parquet_schema structure to output.
extern void parquet_schema_out_init(struct parquet_schema_out_state *state,
                                    struct malloc_lease *buffer,
                                    struct parquet_schema *schema);

/// @brief Outputs the next part of the schema.
/// @param state Pointer to the parquet_schema_out_state structure.
/// @return Zero on success, or a negative error code on failure.
extern i64 parquet_schema_out_next(struct parquet_schema_out_state *state);

/// @brief Flushes the output buffer.
/// @param state Pointer to the parquet_schema_out_state structure.
/// @return Zero on success, or a negative error code on failure.
extern i64 parquet_schema_out_flush(struct parquet_schema_out_state *state);

#if defined(I13C_TESTS)

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases_schema_out(struct runner_context *ctx);

#endif
