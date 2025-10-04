#pragma once

#include "dom.h"
#include "parquet.parse.h"
#include "runner.h"

#define PARQUET_METADATA_TOKENS_SIZE 256
#define PARQUET_METADATA_QUEUE_SIZE 256

struct parquet_metadata_iterator;

typedef void *parquet_metadata_iterator_ctx;
typedef const char *parquet_metadata_iterator_name;
typedef i64 (*parquet_metadata_iterator_fn)(struct parquet_metadata_iterator *iterator, u32 index);

union parquet_metadata_iterator_args {
  u64 value;        // the row value
  const char *name; // the static item name
};

struct parquet_metadata_iterator_item {
  void *ctx; // context of the item

  union parquet_metadata_iterator_args ctx_args; // the context args
  parquet_metadata_iterator_fn ctx_fn;           // the context handler

  union parquet_metadata_iterator_args item_args; // the item args
  parquet_metadata_iterator_fn item_fn;           // the item handler
};

struct parquet_metadata_iterator_tokens {
  u32 count;    // the number of currently occupied tokens
  u32 capacity; // the total number of all available tokens

  // it actually represents all tokens in the iterator
  struct dom_token items[PARQUET_METADATA_TOKENS_SIZE];
};

struct parquet_metadata_iterator_queue {
  u32 count;    // the number of currently occupied slots
  u32 capacity; // the total number of all slots in the queue

  // it actually represents all slots in the queue
  struct parquet_metadata_iterator_item items[PARQUET_METADATA_QUEUE_SIZE];
};

struct parquet_metadata_iterator {
  struct parquet_metadata *metadata;
  struct parquet_metadata_iterator_queue queue;
  struct parquet_metadata_iterator_tokens tokens;
};

/// @brief Initializes a parquet metadata iterator.
/// @param iterator Pointer to the parquet_metadata_iterator structure.
/// @param metadata Pointer to the parquet_metadata structure to iterate.
extern void parquet_metadata_iter(struct parquet_metadata_iterator *iterator, struct parquet_metadata *metadata);

/// @brief Advances the parquet metadata iterator to the next batch of tokens.
/// @param iterator Pointer to the parquet_metadata_iterator structure.
/// @return Zero on success, or a negative error code.
extern i64 parquet_metadata_next(struct parquet_metadata_iterator *iterator);

extern const char *const PARQUET_CONVERTED_TYPE_NAMES[PARQUET_CONVERTED_TYPE_SIZE];
extern const char *const PARQUET_DATA_TYPE_NAMES[PARQUET_DATA_TYPE_SIZE];
extern const char *const PARQUET_REPETITION_TYPE_NAMES[PARQUET_REPETITION_TYPE_SIZE];

#if defined(I13C_TESTS)

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases_iter(struct runner_context *ctx);

#endif
