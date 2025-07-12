#pragma once

#include "runner.h"
#include "typing.h"

#define MALLOC_SLOTS 10

struct malloc_slot {};

typedef struct {
  struct malloc_slot *next;
  void *ptr;
  u64 size;
} malloc_slot;

typedef struct {
  malloc_slot *slots[MALLOC_SLOTS];
} malloc_pool;

/// @brief Initializes the memory pool.
/// @param pool Pointer to the malloc_pool structure.
extern void malloc_init(malloc_pool *pool);

/// @brief Destroys the memory pool, freeing all allocated memory.
extern void malloc_destroy(malloc_pool *pool);

/// @brief Allocates memory from the pool.
/// @param pool Pointer to the malloc_pool structure.
/// @param size Size of the memory block to allocate.
/// @return Pointer to the allocated memory block, or NULL on failure.
extern void *malloc(malloc_pool *pool, u64 size);

/// @brief Frees a previously allocated memory block.
/// @param pool Pointer to the malloc_pool structure.
/// @param ptr Pointer to the memory block to free.
/// @param size Size of the memory block to free.
extern void free(malloc_pool *pool, void *ptr, u64 size);

/// @brief Registers malloc test cases.
/// @param ctx Pointer to the runner_context structure.
extern void malloc_test_cases(runner_context *ctx);
