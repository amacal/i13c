#include "parquet.h"
#include "malloc.h"
#include "runner.h"
#include "stdout.h"
#include "sys.h"
#include "thrift.h"
#include "typing.h"

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;

  file->buffer = NULL;
  file->buffer_start = NULL;
  file->buffer_end = NULL;
  file->buffer_size = 0;
  file->footer_size = 0;

  file->metadata = NULL;
  file->metadata_size = 0;
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
  file->buffer_start = file->buffer_end - file->footer_size;

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

  // free the metadata if it exists
  if (file->metadata) {
    free(file->pool, file->metadata, file->metadata_size);
    file->metadata = NULL;
    file->metadata_size = 0;
  }
}

static i64 parquet_read_version(void *target, enum field_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_metadata *metadata = (struct parquet_metadata *)target;

  // check if the field type is correct
  if (field_type != FIELD_TYPE_I32) {
    return -1;
  }

  return thrift_read_i32(&metadata->version, buffer, buffer_size);
}

static i64 parquet_read_num_rows(void *target, enum field_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_metadata *metadata = (struct parquet_metadata *)target;

  // check if the field type is correct
  if (field_type != FIELD_TYPE_I64) {
    return -1;
  }

  return thrift_read_i64(&metadata->num_rows, buffer, buffer_size);
}

i64 parquet_parse(struct parquet_file *file) {
  i64 result;
  thrift_field fields[3];
  struct parquet_metadata metadata;

  fields[1] = (thrift_field)parquet_read_version;  // version
  fields[2] = (thrift_field)parquet_read_num_rows; // num_rows

  result = thrift_read_struct(&metadata, fields, 3, file->buffer_start, file->footer_size);
  printf("result: %x, Parquet file version: %x, number of rows: %x\n", result, metadata.version, metadata.num_rows);

  return 0;
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
