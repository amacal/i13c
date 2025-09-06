#pragma once

#include "malloc.h"
#include "runner.h"
#include "typing.h"

enum arena_error {
  // indicates that the requested chunk is too large
  ARENA_ERROR_REQUEST_TOO_LARGE = ARENA_ERROR_BASE - 0x01,

  // indicates that arena is already exhausted
  ARENA_ERROR_OUT_OF_MEMORY = ARENA_ERROR_BASE - 0x02,

  // indicates that the release request is invalid
  ARENA_ERROR_INVALID_RELEASE = ARENA_ERROR_BASE - 0x03
};

struct arena_node {
  struct arena_node *next;
  struct malloc_lease data;
};

struct arena_allocator {
  u32 step;
  u32 limit;
  u64 cursor;

  struct arena_node data;
  struct arena_node *head;
  struct malloc_pool *pool;
};

/// @brief Initializes the arena allocator.
/// @param allocator Pointer to the arena_allocator structure.
/// @param pool Pointer to the malloc_pool structure for memory management.
/// @param step Allocation step size in bytes.
/// @param maximum Maximum allocation size in bytes.
extern void arena_init(struct arena_allocator *allocator, struct malloc_pool *pool, u32 step, u32 maximum);

/// @brief Destroys the arena allocator and frees associated resources.
/// @param allocator Pointer to the arena_allocator structure.
extern void arena_destroy(struct arena_allocator *allocator);

/// @brief Acquires a block of memory from the arena allocator.
/// @param allocator Pointer to the arena_allocator structure.
/// @param size Size of the memory block to acquire.
/// @param ptr Pointer to the location where the allocated memory address will be stored.
/// @return 0 on success, or negative error code on failure.
extern i64 arena_acquire(struct arena_allocator *allocator, u32 size, void **ptr);

/// @brief Reverts to a previous state in the arena allocator.
/// @param allocator Pointer to the arena_allocator structure.
/// @param cursor Cursor position to revert to.
/// @return 0 on success, or negative error code on failure.
extern i64 arena_revert(struct arena_allocator *allocator, u64 cursor);

#if defined(I13C_TESTS)

/// @brief Gets the current available bytes of the arena allocator.
/// @param allocator Pointer to the arena_allocator structure.
/// @return Current available bytes in bytes.
extern u32 arena_available(struct arena_allocator *allocator);

/// @brief Gets the current occupied bytes of the arena allocator.
/// @param allocator Pointer to the arena_allocator structure.
/// @return Current occupied bytes in bytes.
extern u32 arena_occupied(struct arena_allocator *allocator);

/// @brief Registers arena test cases.
/// @param ctx Pointer to the runner_context structure.
extern void arena_test_cases(struct runner_context *ctx);

#endif
