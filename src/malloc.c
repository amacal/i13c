#include "malloc.h"
#include "runner.h"
#include "sys.h"
#include "typing.h"

void malloc_init(struct malloc_pool *pool) {
  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    pool->slots[i] = NULL;
  }
}

void malloc_destroy(struct malloc_pool *pool) {
  struct malloc_slot *slot, *next;

  // visit each bucket in the pool
  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    slot = pool->slots[i];
    pool->slots[i] = NULL;

    // traverse linked list and free each slot
    while (slot) {
      next = slot->next;
      sys_munmap(slot->ptr, slot->size);
      slot = next;
    }
  }
}

i64 malloc(struct malloc_pool *pool, u64 size) {
  u32 index;
  struct malloc_slot *slot;

  // check if the size if too small
  if (size < 4096) {
    return (i64)NULL;
  }

  // check if the size is a power of two
  if (__builtin_popcount(size) != 1) {
    return (i64)NULL;
  }

  // check if the size if too large
  if ((index = __builtin_ctzl(size >> 12)) >= MALLOC_SLOTS) {
    return (i64)NULL;
  }

  // check if there's a free slot in the pool
  if ((slot = pool->slots[index]) != NULL) {
    pool->slots[index] = slot->next;
    return (i64)slot->ptr;
  }

  return sys_mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
}

void free(struct malloc_pool *pool, void *ptr, u64 size) {
  u32 index;
  struct malloc_slot *slot;

  // if the size is too large, just release it
  if ((index = __builtin_ctzl(size >> 12)) >= MALLOC_SLOTS) {
    sys_munmap(ptr, size);
    return;
  }

  // prepare the slot
  slot = ptr;
  slot->ptr = ptr;
  slot->size = size;
  slot->next = pool->slots[index];

  // add as a head of the linked list
  pool->slots[index] = slot;
}

static void can_init_and_destroy_pool() {
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after init");
  }

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

static void can_allocate_and_free_memory() {
  void *ptr;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // allocate memory
  ptr = (void *)malloc(&pool, 4096);
  assert(ptr != NULL, "should allocate memory");

  // free the memory
  free(&pool, ptr, 4096);

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

static void can_reuse_deallocated_slot() {
  void *ptr1, *ptr2;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // allocate memory
  ptr1 = (void *)malloc(&pool, 4096);
  assert(ptr1 != NULL, "should allocate memory");

  // free the memory
  free(&pool, ptr1, 4096);

  // allocate again
  ptr2 = (void *)malloc(&pool, 4096);
  assert(ptr2 != NULL, "should allocate memory");
  assert(ptr1 == ptr2, "should reuse deallocated memory");

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

void malloc_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can init and destroy pool", can_init_and_destroy_pool);
  test_case(ctx, "can allocate and free memory", can_allocate_and_free_memory);
  test_case(ctx, "can reuse deallocated slot", can_reuse_deallocated_slot);
}
