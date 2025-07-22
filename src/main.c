#include "malloc.h"
#include "parquet.h"
#include "stdout.h"
#include "typing.h"

i32 main() {
  i64 result;
  u32 index;

  struct parquet_file file;
  struct malloc_pool pool;
  struct parquet_metadata metadata;

  malloc_init(&pool);
  parquet_init(&file, &pool);

  result = parquet_open(&file, "data/test01.parquet");
  writef("opening parquet file ... %x, %x\n", result, file.fd);

  result = parquet_parse(&file, &metadata);
  writef("parsing parquet file ... %x\n", result);

  writef("Parquet file version: %d, number of rows: %d\n", metadata.version, metadata.num_rows);

  if (metadata.created_by) {
    writef("Created by: %s\n", metadata.created_by);
  }

  for (index = 0; index < metadata.schemas_size; index++) {
    writef("Schema element %d: name=%s, type=%d, converted_type=%d\n", index, metadata.schemas[index].name,
           (i64)metadata.schemas[index].type, (i64)metadata.schemas[index].converted_type);
  }

  parquet_close(&file);
  malloc_destroy(&pool);

  return 0;
}
