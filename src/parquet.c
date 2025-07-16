#include "parquet.h"
#include "malloc.h"
#include "runner.h"
#include "stdout.h"
#include "sys.h"
#include "typing.h"

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;
  file->buffer = NULL;
  file->buffer_start = NULL;
  file->buffer_end = NULL;
  file->buffer_size = 0;
}

i64 parquet_open(struct parquet_file *file, const char *path) {
  i64 result;
  file_stat stat;
  u64 offset, completed, remaining;

  // check if the path can be opened
  result = sys_open(path, O_RDONLY, 0);
  if (result < 0) goto cleanup;
  else file->fd = result;

  // check the size of the file behind the file descriptor
  result = sys_fstat(file->fd, &stat);
  if (result < 0) goto error_stat;

  // check if the file is too small to be a parquet file
  if (stat.st_size < 8) {
    result = -1;
    goto error_stat;
  }

  // allocate a buffer for the file content
  file->buffer_size = 4096;
  result = malloc(file->pool, file->buffer_size);
  if (result == 0) goto error_malloc;
  else file->buffer = (char *)result;

  // adjust buffer pointers
  if (stat.st_size < 4096) {
    file->buffer_start = file->buffer + 4096 - stat.st_size;
    file->buffer_end = file->buffer + 4096;
  } else {
    file->buffer_start = file->buffer;
    file->buffer_end = file->buffer + 4096;
  }

  completed = 0;
  remaining = file->buffer_end - file->buffer_start;
  offset = stat.st_size - remaining;

  // fill the buffer
  while (remaining > 0) {
    result = sys_pread(file->fd, file->buffer_start + completed, remaining, offset);
    if (result < 0) goto error_read;

    offset += result;
    completed += result;
    remaining -= result;
  }

  // recompute buffer boundaries
  file->footer_size = *(u32 *)(file->buffer_end - 8);
  file->buffer_end = file->buffer_end - 8;
  file->buffer_start = file->buffer_end - file->footer_size - 8;

  // check if the buffer is too small to handle the footer
  if (file->buffer_start < file->buffer) {
    result = -1;
    goto error_read;
  }

  // success
  return 0;

error_read:
  free(file->pool, file->buffer, file->buffer_size);
  file->buffer = NULL;
  file->buffer_size = 0;

error_malloc:
error_stat:
  sys_close(file->fd);
  file->fd = 0;

cleanup:
  return result;
}

void parquet_close(struct parquet_file *file) {
  // free the buffer using the pool
  free(file->pool, file->buffer, file->buffer_size);
  file->buffer = NULL;
  file->buffer_size = 0;

  // close the file descriptor
  sys_close(file->fd);
  file->fd = 0;
}

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

void parquet_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);
}
