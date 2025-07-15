#include "parquet.h"
#include "malloc.h"
#include "runner.h"
#include "stdout.h"
#include "sys.h"
#include "typing.h"

void parquet_init(parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;
  file->buffer = NULL;
  file->buffer_footer = NULL;
  file->buffer_size = 0;
  file->buffer_used = 0;
}

i64 parquet_open(parquet_file *file, const char *path) {
  char *buffer;
  i64 fd, result;
  u64 read;
  u64 buffer_size;
  u64 buffer_used;
  u64 buffer_offset;
  u64 buffer_start;
  u32 footer_offset;
  file_stat stat;

  // check if the path can be opened
  result = sys_open(path, O_RDONLY, 0);
  if (result < 0) goto cleanup;
  else fd = result;

  // check the size of the file behind the file descriptor
  result = sys_fstat(fd, &stat);
  if (result < 0) goto error_stat;

  // adjust buffer counters
  if (stat.st_size < 4096) {
    buffer_offset = 0;
    buffer_size = 4096;
    buffer_used = stat.st_size;
    buffer_start = 4096 - stat.st_size;
  } else {
    buffer_start = 0;
    buffer_size = 4096;
    buffer_used = 4096;
    buffer_offset = stat.st_size - 4096;
  }

  // allocate a buffer for the file content
  result = malloc(file->pool, buffer_size);
  if (result == 0) goto error_malloc;
  else buffer = (char *)result;

  // fill the buffer
  read = 0;
  while (read < buffer_used) {
    result = sys_pread(fd, buffer + buffer_start + read, buffer_used - read, buffer_offset + read);
    if (result < 0) goto error_read;

    read += result;
  }

  if ((footer_offset = *(u32 *)(buffer + buffer_used - 8)) + 8 > buffer_used) {
    // TODO: reallocate and reread the footer
  }

  // printf("%x", footer_offset);

  // prepare the struct
  file->fd = fd;
  file->buffer = buffer;
  file->buffer_footer = buffer + buffer_size - 8 - footer_offset;
  file->buffer_size = buffer_size;
  file->buffer_used = buffer_used;

  // success
  return 0;

error_read:
  free(file->pool, buffer, buffer_size);

error_malloc:
error_stat:
  sys_close(fd);

cleanup:
  return result;
}

void parquet_close(parquet_file *file) {
  // free the buffer using the pool
  free(file->pool, file->buffer, file->buffer_size);

  // close the file descriptor
  sys_close(file->fd);
}

static void can_open_and_close_parquet_file() {
  parquet_file file;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  i64 result = parquet_open(&file, "/workspaces/i13c/data/test01.parquet");
  assert(result == 0, "should open parquet file");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_non_existing_parquet_file() {
  parquet_file file;
  struct malloc_pool pool;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // try to open a non-existing parquet file
  i64 result = parquet_open(&file, "/workspaces/i13c/data/none.parquet");
  assert(result < 0, "should not open non-existing parquet file");

  // destroy the pool
  malloc_destroy(&pool);
}

void parquet_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);
}
