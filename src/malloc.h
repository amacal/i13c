#pragma once

#include "runner.h"
#include "typing.h"

#define MALLOC_SLOTS 5 // from 4096 to 65536 bytes, 5 slots
struct malloc_slot;

struct malloc_slot {
  struct malloc_slot *next;
  void *ptr;
  u64 size;
};

struct malloc_pool {
  struct malloc_slot *slots[MALLOC_SLOTS];
};

/// @brief Initializes the memory pool.
/// @param pool Pointer to the malloc_pool structure.
extern void malloc_init(struct malloc_pool *pool);

/// @brief Destroys the memory pool, freeing all allocated memory.
extern void malloc_destroy(struct malloc_pool *pool);

/// @brief Allocates memory from the pool.
/// @param pool Pointer to the malloc_pool structure.
/// @param size Size of the memory block to allocate.
/// @return Pointer to the allocated memory on success, or NULL on failure.
extern i64 malloc(struct malloc_pool *pool, u64 size);

/// @brief Frees a previously allocated memory block.
/// @param pool Pointer to the malloc_pool structure.
/// @param ptr Pointer to the memory block to free.
/// @param size Size of the memory block to free.
extern void free(struct malloc_pool *pool, void *ptr, u64 size);

#if defined(I13C_TESTS)

/// @brief Registers malloc test cases.
/// @param ctx Pointer to the runner_context structure.
extern void malloc_test_cases(struct runner_context *ctx);

#endif
