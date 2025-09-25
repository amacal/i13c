#include "parquet.extract.h"
#include "malloc.h"
#include "parquet.base.h"
#include "sys.h"
#include "typing.h"

i32 parquet_extract(u32 argc, const char **argv) {
  i64 result;
  u64 remaining, written;

  struct parquet_file file;
  struct malloc_pool pool;

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

  written = 0;
  remaining = file.footer.size;

  while (remaining > 0) {
    // write to stdout
    result = sys_write(1, file.footer.start + written, remaining);
    if (result < 0) goto cleanup_memory;

    written += result;
    remaining -= result;
  }

  // success
  result = 0;

cleanup_memory:
  malloc_destroy(&pool);

cleanup:
  return result;
}
