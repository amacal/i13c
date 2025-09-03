#include "arena.h"
#include "malloc.h"
#include "typing.h"

#define ARENA_NODE_SIZE ((sizeof(struct arena_node) + 7) & ~7)

void arena_init(struct arena_allocator *allocator, struct malloc_pool *pool, u32 step, u32 maximum) {
  allocator->alloc_step = step;
  allocator->alloc_max = maximum;

  allocator->pool = pool;
  allocator->head = NULL;
}

void arena_destroy(struct arena_allocator *allocator) {
  struct arena_node *head;

  // start from the head
  head = allocator->head;

  // and destroy each lease
  while (head && head->data.ptr) {
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
  u32 offset;
  struct arena_node *head, *next;

  // check if the request cannot be met within any block
  if (size > allocator->alloc_step - ARENA_NODE_SIZE) {
    return ARENA_ERROR_REQUEST_TOO_LARGE;
  }

  // allocate head if not already allocated
  if (allocator->head == NULL) {
    allocator->head = &allocator->data;
    allocator->head->next = NULL;
    allocator->head->data.size = allocator->alloc_step;

    // call page allocator
    result = malloc_acquire(allocator->pool, &allocator->head->data);
    if (result < 0) return result;

    // adjust values
    allocator->alloc_max -= allocator->alloc_step;
    allocator->head->tail = ARENA_NODE_SIZE;
  }

  // get the current head
  head = allocator->head;

  // check if arena is exhausted to allocate next block
  if (allocator->alloc_step - head->tail < size) {
    if (allocator->alloc_max == 0) return ARENA_ERROR_OUT_OF_MEMORY;
  }

  // allocate new block if the current is too small
  if (allocator->alloc_step - head->tail < size) {
    next = (struct arena_node *)allocator->head->data.ptr;
    next->next = allocator->head;
    next->data.size = allocator->alloc_step;

    // call page allocator
    result = malloc_acquire(allocator->pool, &next->data);
    if (result < 0) return result;

    // adjust values
    allocator->alloc_max -= allocator->alloc_step;
    allocator->head = next;
    allocator->head->tail = ARENA_NODE_SIZE;
  }

  // update indices
  offset = allocator->head->tail;
  allocator->head->tail += (size + 7) & ~7;

  // return pointer
  *ptr = allocator->head->data.ptr + offset;

  // success
  return 0;
}

i64 arena_release(struct arena_allocator *allocator, u32 size, void *ptr) {
  struct arena_node *next, *prev;

  // align the size
  size = (size + 7) & ~7;

  // check if the head is ready
  if (allocator->head == NULL) return ARENA_ERROR_INVALID_RELEASE;

  // check if the release follows LIFO
  if (ptr != allocator->head->data.ptr + allocator->head->tail - size) {
    return ARENA_ERROR_INVALID_RELEASE;
  }

  // move back
  allocator->head->tail -= size;

  // optionally try to switch the head
  if (allocator->head->tail == ARENA_NODE_SIZE) {
    if (allocator->head->next) {
      prev = allocator->head;
      next = allocator->head->next;

      allocator->head = next;
      allocator->alloc_max += allocator->alloc_step;

      malloc_release(allocator->pool, &prev->data);
    }
  }

  // success
  return 0;
}

#if defined(I13C_TESTS)

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

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  result = arena_acquire(&allocator, 128, &ptr1);
  assert(result == 0, "acquire should succeed");
  assert(ptr1 != NULL, "pointer should not be null");

  result = arena_acquire(&allocator, 256, &ptr2);
  assert(result == 0, "acquire should succeed");
  assert(ptr2 != NULL, "pointer should not be null");

  // free the memory
  arena_release(&allocator, 256, ptr1);
  arena_release(&allocator, 128, ptr2);

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

static void can_release_aligned_blocks() {
  void *ptr1, *ptr2, *ptr3;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 4096);

  // allocate some memory
  result = arena_acquire(&allocator, 3, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  result = arena_acquire(&allocator, 17, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  // now destroy it
  result = arena_release(&allocator, 17, ptr2);
  assert(result == 0, "release ptr2 should succeed");

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

static void can_release_multiple_blocks() {
  void *ptr1, *ptr2, *ptr3;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 8192);

  // allocate some memory
  result = arena_acquire(&allocator, 4000, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  result = arena_acquire(&allocator, 2000, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  // now destroy it
  result = arena_release(&allocator, 2000, ptr2);
  assert(result == 0, "release ptr2 should succeed");

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

static void can_release_wasted_space() {
  void *ptr1, *ptr2, *ptr3, *ptr4;
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator allocator;

  // initialize
  malloc_init(&pool);
  arena_init(&allocator, &pool, 4096, 8192);

  // allocate some memory
  result = arena_acquire(&allocator, 3000, &ptr1);
  assert(result == 0, "acquire ptr1 should succeed");

  result = arena_acquire(&allocator, 500, &ptr2);
  assert(result == 0, "acquire ptr2 should succeed");

  result = arena_acquire(&allocator, 2000, &ptr3);
  assert(result == 0, "acquire ptr3 should succeed");

  // now destroy it
  result = arena_release(&allocator, 2000, ptr3);
  assert(result == 0, "release ptr3 should succeed");

  result = arena_release(&allocator, 500, ptr2);
  assert(result == 0, "release ptr2 should succeed");

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

void arena_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can init and destroy arena", can_init_and_destroy_arena);
  test_case(ctx, "can allocate and free memory", can_allocate_and_free_memory);

  test_case(ctx, "can allocate aligned blocks", can_allocate_aligned_blocks);
  test_case(ctx, "can allocate multiple blocks", can_allocate_multiple_blocks);
  test_case(ctx, "can detect too large request", can_detect_too_large_request);
  test_case(ctx, "can detect out of memory", can_detect_out_of_memory);

  test_case(ctx, "can release aligned blocks", can_release_aligned_blocks);
  test_case(ctx, "can release multiple blocks", can_release_multiple_blocks);
  test_case(ctx, "can release wasted space", can_release_wasted_space);
}

#endif
