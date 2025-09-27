#include "parquet.base.h"
#include "malloc.h"
#include "runner.h"
#include "sys.h"
#include "typing.h"

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;

  file->footer.lease.ptr = NULL;
  file->footer.lease.size = 0;

  file->footer.start = NULL;
  file->footer.end = NULL;

  arena_init(&file->arena, pool, 4096, 32 * 4096);
}

i64 parquet_open(struct parquet_file *file, const char *path) {
  const u64 DEFAULT_BUFFER_SIZE = 4096;

  i64 result;
  file_stat stat;
  u64 offset, completed, remaining;

  // check if the path can be opened
  result = sys_open(path, O_RDONLY, 0);
  if (result < 0) goto cleanup;
  else file->fd = result;

  // check the size of the file behind the file descriptor
  result = sys_fstat(file->fd, &stat);
  if (result < 0) goto cleanup_file;

  // check if the file is too small to be a parquet file
  if (stat.st_size < 8) {
    result = PARQUET_ERROR_INVALID_FILE;
    goto cleanup_file;
  }

  // set default parameters
  file->footer.lease.size = DEFAULT_BUFFER_SIZE;

alloc:
  // allocate a buffer for the file content
  result = malloc_acquire(file->pool, &file->footer.lease);
  if (result < 0) goto cleanup_file;

  // adjust buffer pointers
  if (stat.st_size < (i64)file->footer.lease.size) {
    file->footer.start = file->footer.lease.ptr + file->footer.lease.size - stat.st_size;
    file->footer.end = file->footer.lease.ptr + file->footer.lease.size;
  } else {
    file->footer.start = file->footer.lease.ptr;
    file->footer.end = file->footer.lease.ptr + file->footer.lease.size;
  }

  completed = 0;
  remaining = file->footer.end - file->footer.start;
  offset = stat.st_size - remaining;

  // fill the buffer
  while (remaining > 0) {
    // read next chunk of the footer
    result = sys_pread(file->fd, file->footer.start + completed, remaining, offset);
    if (result < 0) goto cleanup_buffer;

    // check if the read was as expected
    if (result == 0) {
      result = PARQUET_ERROR_INVALID_FILE;
      goto cleanup_buffer;
    }

    // move the offset and completed bytes
    offset += result;
    completed += result;
    remaining -= result;
  }

  // recompute buffer boundaries, next op is properly aligned
  file->footer.size = *(u32 *)(file->footer.end - 8);
  file->footer.end = file->footer.end - 8;
  file->footer.start = file->footer.end - file->footer.size;

  // check if the buffer is too small to handle the footer
  if (file->footer.start < (char *)file->footer.lease.ptr) {
    if (file->footer.lease.size > DEFAULT_BUFFER_SIZE) {
      result = PARQUET_ERROR_INVALID_FILE;
      goto cleanup_buffer;
    }

    // there is a hope for better buffer size
    malloc_release(file->pool, &file->footer.lease);
    file->footer.start = NULL;
    file->footer.end = NULL;

    // set the expected value aligned to the next power of 2
    file->footer.lease.size = 1 << (64 - __builtin_clzll(file->footer.size + 7));
    goto alloc;
  }

  // success
  result = 0;
  goto cleanup_file;

cleanup_buffer:
  // release the buffer and clear the pointers
  malloc_release(file->pool, &file->footer.lease);
  file->footer.start = NULL;
  file->footer.end = NULL;

cleanup_file:
  // close the file descriptor and clear it
  sys_close(file->fd);
  file->fd = 0;

cleanup:
  return result;
}

void parquet_close(struct parquet_file *file) {
  // release the buffer lease using the pool
  malloc_release(file->pool, &file->footer.lease);
  file->footer.start = NULL;
  file->footer.end = NULL;

  // close the file descriptor
  if (file->fd > 0) {
    sys_close(file->fd);
    file->fd = 0;
  }

  // release arena
  arena_destroy(&file->arena);
}

#if defined(I13C_TESTS)

static void can_open_and_close_parquet_file() {
  struct parquet_file file;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  i64 result = parquet_open(&file, "data/test01.parquet");
  assert(result == 0, "should open parquet file");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_non_existing_parquet_file() {
  struct parquet_file file;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // try to open a non-existing parquet file
  i64 result = parquet_open(&file, "data/none.parquet");
  assert(result < 0, "should not open non-existing parquet file");

  // destroy the pool
  malloc_destroy(&pool);
}

void parquet_test_cases_base(struct runner_context *ctx) {
  // opening and closing cases
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);
}

#endif
