#pragma once

#include "runner.h"
#include "typing.h"

#define MALLOC_SLOTS 5 // from 4096 to 65536 bytes, 5 slots

struct malloc_slot {
  struct malloc_slot *next; // next slot in the linked list
  void *ptr;                // pointer to the allocated memory
  u64 size;                 // size of the allocated memory
};

struct malloc_pool {
  struct malloc_slot *slots[MALLOC_SLOTS]; // predefined number of slots
};

struct malloc_lease {
  void *ptr; // pointer to the allocated memory
  u64 size;  // size of the allocated memory
};

/// @brief Initializes the memory pool.
/// @param pool Pointer to the malloc_pool structure.
extern void malloc_init(struct malloc_pool *pool);

/// @brief Destroys the memory pool, freeing all allocated memory.
extern void malloc_destroy(struct malloc_pool *pool);

/// @brief Acquires memory from the pool.
/// @param pool Pointer to the malloc_pool structure.
/// @param lease Pointer to the malloc_lease structure to fill.
/// @return Zero on success, or NULL on failure.
extern i64 malloc_acquire(struct malloc_pool *pool, struct malloc_lease *lease);

/// @brief Releases a previously allocated memory block.
/// @param pool Pointer to the malloc_pool structure.
/// @param lease Pointer to the malloc_lease structure to release.
extern void malloc_release(struct malloc_pool *pool, struct malloc_lease *lease);

#if defined(I13C_TESTS)

/// @brief Registers malloc test cases.
/// @param ctx Pointer to the runner_context structure.
extern void malloc_test_cases(struct runner_context *ctx);

#endif
