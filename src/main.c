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
  struct parquet_schema_element *schema;

  malloc_init(&pool);
  parquet_init(&file, &pool);

  result = parquet_open(&file, "data/test01.parquet");
  writef("opening parquet file ... %d, %d\n", result, file.fd);

  result = parquet_parse(&file, &metadata);
  writef("parsing parquet file ... %d\n", result);

  writef("Parquet file version: %d, number of rows: %d\n", metadata.version, metadata.num_rows);

  if (metadata.created_by) {
    writef("Created by: %s\n", metadata.created_by);
  }

  if (metadata.schemas) {
    index = 0;
    schema = metadata.schemas[index];

    while (schema) {
      writef("Schema element, name=%s, data_type=%d, converted_type=%d, repetition_type=%d\n", schema->name,
             (i64)schema->data_type, (i64)schema->converted_type, (i64)schema->repetition_type);
      schema = metadata.schemas[++index];
    }
  }

  parquet_close(&file);
  malloc_destroy(&pool);

  return 0;
}
