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

i64 malloc_acquire(struct malloc_pool *pool, struct malloc_lease *lease) {
  u32 index;
  i64 result;
  struct malloc_slot *slot;

  // check if the size if too small
  if (lease->size < 4096) {
    return MALLOC_ERROR_INVALID_SIZE;
  }

  // check if the size is a power of two
  if (__builtin_popcountll(lease->size) != 1) {
    return MALLOC_ERROR_INVALID_SIZE;
  }

  // check if the size if too large
  if ((index = __builtin_ctzl(lease->size >> 12)) >= MALLOC_SLOTS) {
    return MALLOC_ERROR_INVALID_SIZE;
  }

  // check if there's a free slot in the pool
  if ((slot = pool->slots[index]) != NULL) {
    pool->slots[index] = slot->next;
    lease->ptr = slot->ptr;
    return 0;
  }

  // allocate memory using mmap
  result = sys_mmap(NULL, lease->size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (result < 0) return result;

  // prepare the lease
  lease->ptr = (void *)result;

  // success
  return 0;
}

void malloc_release(struct malloc_pool *pool, struct malloc_lease *lease) {
  u32 index;
  struct malloc_slot *slot;

  // if the size is too large, just release it
  if ((index = __builtin_ctzl(lease->size >> 12)) >= MALLOC_SLOTS) {
    sys_munmap(lease->ptr, lease->size);
    return;
  }

  // prepare the slot
  slot = (struct malloc_slot *)lease->ptr;
  slot->ptr = lease->ptr;
  slot->size = lease->size;
  slot->next = pool->slots[index];

  // add as a head of the linked list
  pool->slots[index] = slot;

  // clear the lease pointer
  lease->ptr = NULL;
  lease->size = 0;
}

#if defined(I13C_TESTS)

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
  struct malloc_pool pool;
  struct malloc_lease lease;

  // initialize the pool
  malloc_init(&pool);

  // prepare the lease
  lease.size = 4096;

  // acquire memory
  assert(malloc_acquire(&pool, &lease) == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

static void can_reuse_deallocated_slot() {
  void *ptr;
  struct malloc_pool pool;
  struct malloc_lease lease1, lease2;

  // initialize the pool
  malloc_init(&pool);

  // prepare the leases
  lease1.size = 4096;
  lease2.size = 4096;

  // allocate memory
  assert(malloc_acquire(&pool, &lease1) == 0, "should allocate initial memory");
  assert(lease1.ptr != NULL, "lease ptr should be set");

  // release the memory
  ptr = lease1.ptr;
  malloc_release(&pool, &lease1);

  // acquire again
  assert(malloc_acquire(&pool, &lease2) == 0, "should allocate reused memory");
  assert(ptr == lease2.ptr, "should reuse deallocated memory");

  // release the memory
  malloc_release(&pool, &lease2);

  // destroy the pool
  malloc_destroy(&pool);

  for (u32 i = 0; i < MALLOC_SLOTS; i++) {
    assert(pool.slots[i] == NULL, "pool slot should be NULL after destroy");
  }
}

static void cannot_allocate_too_small_lease() {
  struct malloc_pool pool;
  struct malloc_lease lease;

  // initialize the pool
  malloc_init(&pool);

  // prepare the lease
  lease.size = 1024;

  // try to acquire memory
  assert(malloc_acquire(&pool, &lease) == MALLOC_ERROR_INVALID_SIZE, "should not allocate too small lease");

  // destroy the pool
  malloc_destroy(&pool);
}

static void cannot_allocate_too_large_lease() {
  struct malloc_pool pool;
  struct malloc_lease lease;

  // initialize the pool
  malloc_init(&pool);

  // prepare the lease
  lease.size = 1048576;

  // try to acquire memory
  assert(malloc_acquire(&pool, &lease) == MALLOC_ERROR_INVALID_SIZE, "should not allocate too large lease");

  // destroy the pool
  malloc_destroy(&pool);
}

static void cannot_allocate_not_power_of_two() {
  struct malloc_pool pool;
  struct malloc_lease lease;

  // initialize the pool
  malloc_init(&pool);

  // prepare the lease
  lease.size = 5000;

  // try to acquire memory
  assert(malloc_acquire(&pool, &lease) == MALLOC_ERROR_INVALID_SIZE, "should not allocate not power of two");

  // destroy the pool
  malloc_destroy(&pool);
}

void malloc_test_cases(struct runner_context *ctx) {
  // positive cases
  test_case(ctx, "can init and destroy pool", can_init_and_destroy_pool);
  test_case(ctx, "can allocate and free memory", can_allocate_and_free_memory);
  test_case(ctx, "can reuse deallocated slot", can_reuse_deallocated_slot);

  // negative cases
  test_case(ctx, "cannot allocate too small lease", cannot_allocate_too_small_lease);
  test_case(ctx, "cannot allocate too large lease", cannot_allocate_too_large_lease);
  test_case(ctx, "cannot allocate not power of two", cannot_allocate_not_power_of_two);
}

#endif
