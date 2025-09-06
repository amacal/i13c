#include "arena.h"
#include "malloc.h"
#include "typing.h"

#define ARENA_NODE_SIZE ((sizeof(struct arena_node) + 7) & ~7)

void arena_init(struct arena_allocator *allocator, struct malloc_pool *pool, u32 step, u32 maximum) {
  allocator->step = step;
  allocator->limit = maximum;

  allocator->pool = pool;
  allocator->head = NULL;
  allocator->cursor = 0;
}

void arena_destroy(struct arena_allocator *allocator) {
  struct arena_node *head;

  // start from the head
  head = allocator->head;

  // and destroy each lease
  while (head) {
    struct arena_node *next = head->next;

    // it may happen that the alloc failed before
    if (head->data.ptr) {
      malloc_release(allocator->pool, &head->data);
    }

    // go to the next node
    head = next;
  }
}

i64 arena_acquire(struct arena_allocator *allocator, u32 size, void **ptr) {
  i64 result;
  u64 offset;
  struct arena_node *head, *next;

  // check if the request cannot be met within any block
  if (size > allocator->step - ARENA_NODE_SIZE) {
    return ARENA_ERROR_REQUEST_TOO_LARGE;
  }

  // allocate head if not already allocated
  if (allocator->head == NULL) {
    allocator->head = &allocator->data;
    allocator->head->next = NULL;
    allocator->head->data.size = allocator->step;

    // call page allocator
    result = malloc_acquire(allocator->pool, &allocator->head->data);
    if (result < 0) return result;

    // adjust limit and find new cursor
    allocator->limit -= allocator->step;
    allocator->cursor = (u64)allocator->head->data.ptr + ARENA_NODE_SIZE;
  }

  // get the current head
  head = allocator->head;

  // check if the current request won't fit into the remaining limit
  if ((u64)head->data.ptr + head->data.size - allocator->cursor < size) {
    if (allocator->limit == 0) return ARENA_ERROR_OUT_OF_MEMORY;
  }

  // allocate new block if the current is too small
  if ((u64)head->data.ptr + head->data.size - allocator->cursor < size) {
    next = (struct arena_node *)allocator->head->data.ptr;
    next->next = allocator->head;
    next->data.size = allocator->step;
    head = next;

    // call page allocator
    result = malloc_acquire(allocator->pool, &next->data);
    if (result < 0) return result;

    // adjust values
    allocator->head = next;
    allocator->limit -= allocator->step;
    allocator->cursor = (u64)allocator->head->data.ptr + ARENA_NODE_SIZE;
  }

  // update indices
  offset = allocator->cursor;
  allocator->cursor += (size + 7) & ~7;

  // return pointer
  *ptr = (void *)offset;

  // success
  return 0;
}

i64 arena_revert(struct arena_allocator *allocator, u64 cursor) {
  struct arena_node *next, *prev;

  // start from the head
  next = allocator->head;

  // assert the node that contains the cursor
  if (cursor) {
    while (next) {
      if (cursor >= (u64)next->data.ptr) {
        if (cursor < (u64)next->data.ptr + next->data.size) {
          break;
        }
      }

      // go to the next node
      next = next->next;
    }

    // check if we didn't find the cursor
    if (next == NULL) {
      return ARENA_ERROR_INVALID_RELEASE;
    }

    // start from the head
    next = allocator->head;
  }

  // release all nodes until we find the cursor
  while (next) {
    if (cursor >= (u64)next->data.ptr) {
      if (cursor < (u64)next->data.ptr + next->data.size) {
        break;
      }
    }

    prev = next;
    next = next->next;

    allocator->head = next;
    allocator->limit += allocator->step;

    malloc_release(allocator->pool, &prev->data);
  }

  // revert the cursor
  allocator->cursor = cursor;

  // success
  return 0;
}

#if defined(I13C_TESTS)

u32 arena_available(struct arena_allocator *allocator) {
  u32 limit;
  u64 available;
  struct arena_node *head;

  // start from the head
  available = 0;
  head = allocator->head;
  limit = allocator->limit;

  // consider current head
  if (head) {
    available += (u64)head->data.ptr + head->data.size - allocator->cursor;
    head = head->next;
  }

  // and sum up all available space
  while (head) {
    available += head->data.size - ARENA_NODE_SIZE;
    head = head->next;
  }

  // add all the remaining limit
  while (limit) {
    available += (allocator->step - ARENA_NODE_SIZE);
    limit -= allocator->step;
  }

  // return the available space
  return (u32)available;
}

u32 arena_occupied(struct arena_allocator *allocator) {
  struct arena_node *head;
  u32 occupied;

  // start from the head
  occupied = 0;
  head = allocator->head;

  // consider current head
  if (head) {
    occupied += allocator->cursor - (u64)head->data.ptr - ARENA_NODE_SIZE;
    head = head->next;
  }

  // and sum up all occupied space
  while (head) {
    occupied += head->data.size - ARENA_NODE_SIZE;
    head = head->next;
  }

  // return the occupied space
  return occupied;
}

static void can_init_and_destroy_arena() {
  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // verify
  assert(allocator.head == NULL, "should not allocate head");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 0, "pool should have acquired 0 bytes");
  assert(pool.released == 0, "pool should have released 0 bytes");
}

static void can_allocate_and_free_memory() {
  void *ptr1, *ptr2;
  i64 result;
  u64 cursor;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  result = arena_acquire(&allocator, 128, &ptr1);
  assert(result == 0, "acquire should succeed");
  assert(ptr1 != NULL, "pointer should not be null");

  cursor = allocator.cursor;
  result = arena_acquire(&allocator, 256, &ptr2);
  assert(result == 0, "acquire should succeed");
  assert(ptr2 != NULL, "pointer should not be null");

  // revert the cursor
  arena_revert(&allocator, cursor);

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 4096, "pool should have acquired 4096 bytes");
  assert(pool.released == 4096, "pool should have released 4096 bytes");
}

static void can_allocate_aligned_blocks() {
  void *ptr1, *ptr2;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  result = arena_acquire(&allocator, 3, &ptr1);
  assert(result == 0, "acquire should succeed");
  assert(ptr1 != NULL, "pointer should not be null");
  assert((u64)ptr1 % 8 == 0, "pointer should be aligned");
  assert((u64)ptr1 == (u64)allocator.head->data.ptr + ARENA_NODE_SIZE, "pointer should be after the node");

  result = arena_acquire(&allocator, 17, &ptr2);
  assert(result == 0, "acquire should succeed");
  assert(ptr2 != NULL, "pointer should not be null");
  assert((u64)ptr2 % 8 == 0, "pointer should be aligned");
  assert((u64)ptr2 > (u64)ptr1 + 3, "pointer should be after the first");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 4096, "pool should have acquired 4096 bytes");
  assert(pool.released == 4096, "pool should have released 4096 bytes");
}

static void can_allocate_multiple_blocks() {
  void *ptr1, *ptr2, *ptr3;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 16384);

  // allocate some memory
  result = arena_acquire(&allocator, 1000, &ptr1);
  assert(result == 0, "acquire should succeed");
  assert(ptr1 != NULL, "pointer should not be null");

  result = arena_acquire(&allocator, 4000, &ptr2);
  assert(result == 0, "acquire should succeed");
  assert(ptr2 != NULL, "pointer should not be null");

  result = arena_acquire(&allocator, 3000, &ptr3);
  assert(result == 0, "acquire should succeed");
  assert(ptr3 != NULL, "pointer should not be null");

  assert(((u64)ptr1 & ~0x7ff) != ((u64)ptr2 & ~0x7ff), "pointers should be in different blocks");
  assert(((u64)ptr1 & ~0x7ff) != ((u64)ptr3 & ~0x7ff), "pointers should be in different blocks");
  assert(((u64)ptr2 & ~0x7ff) != ((u64)ptr3 & ~0x7ff), "pointers should be in different blocks");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 3 * 4096, "pool should have acquired 3 * 4096 bytes");
  assert(pool.released == 3 * 4096, "pool should have released 3 * 4096 bytes");
}

static void can_detect_too_large_request() {
  void *ptr;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // acquire
  ptr = NULL;
  result = arena_acquire(&allocator, 4096, &ptr);
  assert(result == ARENA_ERROR_REQUEST_TOO_LARGE, "should fail with ARENA_ERROR_REQUEST_TOO_LARGE");
  assert(ptr == NULL, "pointer should not be delivered");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 0, "pool should have acquired 0 bytes");
  assert(pool.released == 0, "pool should have released 0 bytes");
}

static void can_detect_out_of_memory() {
  i64 result;
  void *ptr1, *ptr2;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // acquire
  ptr1 = NULL;
  result = arena_acquire(&allocator, 2048, &ptr1);
  assert(result == 0, "should succeed");
  assert(ptr1 != NULL, "pointer should be delivered");

  ptr2 = NULL;
  result = arena_acquire(&allocator, 2048, &ptr2);
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(ptr2 == NULL, "pointer should not be delivered");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 4096, "pool should have acquired 4096 bytes");
  assert(pool.released == 4096, "pool should have released 4096 bytes");
}

static void can_revert_aligned_blocks() {
  void *ptr1, *ptr2, *ptr3;
  i64 result;
  u64 cursor;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  result = arena_acquire(&allocator, 3, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  cursor = allocator.cursor;
  result = arena_acquire(&allocator, 17, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  // now revert it
  result = arena_revert(&allocator, cursor);
  assert(result == 0, "revert ptr2 should succeed");

  // allocate again
  result = arena_acquire(&allocator, 17, &ptr3);
  assert(result == 0, "acquire ptr3 should succeed");
  assert(ptr2 == ptr3, "should allocate the same pointer again");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 4096, "pool should have acquired 4096 bytes");
  assert(pool.released == 4096, "pool should have released 4096 bytes");
}

static void can_revert_multiple_blocks() {
  void *ptr1, *ptr2, *ptr3;
  i64 result;
  u64 cursor;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 8192);

  // allocate some memory
  result = arena_acquire(&allocator, 4000, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  cursor = allocator.cursor;
  result = arena_acquire(&allocator, 2000, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  // now revert it
  result = arena_revert(&allocator, cursor);
  assert(result == 0, "revert ptr2 should succeed");

  // allocate again
  result = arena_acquire(&allocator, 100, &ptr3);
  assert(result == 0, "acquire ptr3 should succeed");
  assert(((u64)ptr2 & ~0x7ff) == ((u64)ptr3 & ~0x7ff), "pointers should be in same block");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 3 * 4096, "pool should have acquired 3 * 4096 bytes");
  assert(pool.released == 3 * 4096, "pool should have released 3 * 4096 bytes");
}

static void can_revert_wasted_space() {
  void *ptr1, *ptr2, *ptr3, *ptr4;
  u64 cursor1, cursor2;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 8192);

  // allocate some memory
  result = arena_acquire(&allocator, 3000, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  cursor1 = allocator.cursor;
  result = arena_acquire(&allocator, 500, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  cursor2 = allocator.cursor;
  result = arena_acquire(&allocator, 2000, &ptr3);
  assert(result == 0, "acquire ptr3 should succeed");

  // now revert them
  result = arena_revert(&allocator, cursor2);
  assert(result == 0, "revert ptr3 should succeed");

  result = arena_revert(&allocator, cursor1);
  assert(result == 0, "revert ptr2 should succeed");

  // allocate again
  result = arena_acquire(&allocator, 30, &ptr4);
  assert(result == 0, "acquire ptr4 should succeed");
  assert(ptr2 == ptr4, "should allocate the same block again");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 8192, "pool should have acquired 8192 bytes");
  assert(pool.released == 8192, "pool should have released 8192 bytes");
}

static void can_revert_initial_state() {
  void *ptr1, *ptr2;
  i64 result;
  u64 cursor;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  cursor = allocator.cursor;
  result = arena_acquire(&allocator, 3, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  // now revert it
  result = arena_revert(&allocator, cursor);
  assert(result == 0, "revert ptr1 should succeed");

  // allocate again
  result = arena_acquire(&allocator, 17, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");
  assert(ptr2 == ptr1, "should allocate the same pointer again");

  // destroy
  arena_destroy(&allocator);
  malloc_destroy(&pool);

  // verify the pool state
  assert(pool.acquired == 8192, "pool should have acquired 8192 bytes");
  assert(pool.released == 8192, "pool should have released 8192 bytes");
}

void arena_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can init and destroy arena", can_init_and_destroy_arena);
  test_case(ctx, "can allocate and free memory", can_allocate_and_free_memory);

  test_case(ctx, "can allocate aligned blocks", can_allocate_aligned_blocks);
  test_case(ctx, "can allocate multiple blocks", can_allocate_multiple_blocks);
  test_case(ctx, "can detect too large request", can_detect_too_large_request);
  test_case(ctx, "can detect out of memory", can_detect_out_of_memory);

  test_case(ctx, "can revert aligned blocks", can_revert_aligned_blocks);
  test_case(ctx, "can revert multiple blocks", can_revert_multiple_blocks);
  test_case(ctx, "can revert wasted space", can_revert_wasted_space);
  test_case(ctx, "can revert initial state", can_revert_initial_state);
}

#endif
