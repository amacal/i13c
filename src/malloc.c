#include "malloc.h"
#include "runner.h"
#include "sys.h"
#include "typing.h"

void malloc_init(malloc_pool *pool) {
  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    pool->slots[i] = NULL;
  }
}

void malloc_destroy(malloc_pool *pool) {
  malloc_slot *slot, *next;

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

void *malloc(malloc_pool *pool, u64 size) {
  u32 index;
  i64 result;
  malloc_slot *slot;

  // check if the size if too large
  if ((index = __builtin_ctzl(size >> 12)) >= MALLOC_SLOTS) {
    return NULL;
  }

  // check if there's a free slot in the pool
  if ((slot = pool->slots[index]) != NULL) {
    pool->slots[index] = slot->next;
    return slot->ptr;
  }

  // check if kernel reserved memory is available
  if ((result = sys_mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)) < 0) {
    return NULL;
  }

  // success
  return result;
}

void free(malloc_pool *pool, void *ptr, u64 size) {
  u32 index;
  malloc_slot *slot;

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
  malloc_pool pool;

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
  malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // allocate memory
  ptr = malloc(&pool, 4096);
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
  malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // allocate memory
  ptr1 = malloc(&pool, 4096);
  assert(ptr1 != NULL, "should allocate memory");

  // free the memory
  free(&pool, ptr1, 4096);

  // allocate again
  ptr2 = malloc(&pool, 4096);
  assert(ptr2 != NULL, "should allocate memory");
  assert(ptr1 == ptr2, "should reuse deallocated memory");

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

void malloc_test_cases(runner_context *ctx) {
  test_case(ctx, "can init and destroy pool", can_init_and_destroy_pool);
  test_case(ctx, "can allocate and free memory", can_allocate_and_free_memory);
  test_case(ctx, "can reuse deallocated slot", can_reuse_deallocated_slot);
}
