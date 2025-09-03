#pragma once

#include "error.h"
#include "runner.h"
#include "typing.h"

#define MALLOC_SLOTS 8 // from 4096 to 262144 bytes, 8 slots

enum malloc_error {
  // indicates that the size is not acceptable, e.g., too small, too big or not a power of two
  MALLOC_ERROR_INVALID_SIZE = MALLOC_ERROR_BASE - 0x01,
};

struct malloc_slot {
  struct malloc_slot *next; // next slot in the linked list
  void *ptr;                // pointer to the allocated memory
  u64 size;                 // size of the allocated memory
};

struct malloc_pool {
  u64 acquired;
  u64 released;

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
/// @return NULL on success, or a negative error code on failure.
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
