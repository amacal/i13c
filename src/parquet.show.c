#include "dom.h"
#include "malloc.h"
#include "parquet.base.h"
#include "parquet.iter.h"
#include "parquet.parse.h"
#include "stdout.h"
#include "typing.h"

#if defined(I13C_PARQUET)

i32 parquet_show(u32 argc, const char **argv) {
  i64 result;
  u32 tokens;
  bool small;
  u32 written;

  struct parquet_file file;
  struct malloc_pool pool;
  struct malloc_lease output;
  struct dom_state dom;
  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // check for required arguments
  result = PARQUET_INVALID_ARGUMENTS;
  if (argc < 1) goto cleanup;

  // default
  result = 0;

  // initialize memory and parquet file
  malloc_init(&pool);
  parquet_init(&file, &pool);

  // try to open parquet file
  result = parquet_open(&file, argv[0]);
  if (result < 0) goto cleanup_memory;

  // try to parse metadata
  result = parquet_parse(&file, &metadata);
  if (result < 0) goto cleanup_file;

  // allocate output buffer
  output.size = 4096;
  result = malloc_acquire(&pool, &output);
  if (result < 0) goto cleanup_file;

  // initialize DOM and parquet iterator
  dom_init(&dom, &output);
  parquet_metadata_iter(&iterator, &metadata);

  do {
    // next batch of tokens
    result = parquet_metadata_next(&iterator);
    if (result < 0) goto cleanup_buffer;

    // initial counters
    written = 0;
    tokens = iterator.tokens.count;

    while (tokens > 0) {
      // try to write them
      result = dom_write(&dom, iterator.tokens.items + written, &tokens);

      // determine new counters
      written += tokens;
      tokens = iterator.tokens.count - written;

      // check if we need to retry it later
      small = result == FORMAT_ERROR_BUFFER_TOO_SMALL;
      if (result < 0 && !small) goto cleanup_buffer;

      // flush partially written data
      result = stdout_flush(&dom.format);
      if (result < 0) goto cleanup_buffer;

      // perhaps we need to flush the DOM buffer
      if (small) {
        result = dom_flush(&dom);
        if (result < 0) goto cleanup_buffer;

        // clear the flag
        small = FALSE;
      }
    }

  } while (iterator.tokens.count > 0);

  result = stdout_flush(&dom.format);
  if (result < 0) goto cleanup_buffer;

  // success
  result = 0;

cleanup_buffer:
  malloc_release(&pool, &output);

cleanup_file:
  parquet_close(&file);

cleanup_memory:
  malloc_destroy(&pool);

cleanup:
  return result;
}

#endif
