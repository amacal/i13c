#include "malloc.h"
#include "parquet.h"
#include "stdout.h"
#include "typing.h"

i32 main() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  malloc_init(&pool);
  parquet_init(&file, &pool);

  result = parquet_open(&file, "data/test01.parquet");
  writef("opening parquet file ... %x, %x\n", result, file.fd);

  result = parquet_parse(&file);
  writef("parsing parquet file ... %x\n", result);

  parquet_close(&file);
  malloc_destroy(&pool);

  return 0;
}
