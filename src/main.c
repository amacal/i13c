#include "malloc.h"
#include "parquet.h"
#include "stdout.h"
#include "typing.h"

i32 main() {
  i64 result;
  u32 index, column_index;

  i32 **encodings;
  char ***path_in_schema;

  struct parquet_file file;
  struct malloc_pool pool;
  struct parquet_metadata metadata;
  struct parquet_schema_element *schema;
  struct parquet_row_group *row_group;
  struct parquet_column_chunk *column;

  malloc_init(&pool);
  parquet_init(&file, &pool);

  result = parquet_open(&file, "data/test01.parquet");
  if (result < 0) return result;

  result = parquet_parse(&file, &metadata);
  writef("result %d\n", result);
  if (result < 0) return result;

  writef("File Version: %d\n", metadata.version);
  writef("Number of Rows: %d\n", metadata.num_rows);

  if (metadata.created_by) {
    writef("Created by: %s\n", metadata.created_by);
  }

  if (metadata.schemas) {
    writef("\nSchemas:\n");

    index = 0;
    schema = metadata.schemas[index];

    while (schema) {
      writef("- name=%s, data_type=%d, converted_type=%d, repetition_type=%d\n", schema->name, (i64)schema->data_type,
             (i64)schema->converted_type, (i64)schema->repetition_type);
      schema = metadata.schemas[++index];
    }
  }

  if (metadata.row_groups) {
    writef("\nRow Groups:\n");

    index = 0;
    row_group = metadata.row_groups[index];

    while (row_group) {
      writef("- num_rows=%d, total_byte_size=%d, total_compressed_size=%d\n", row_group->num_rows,
             row_group->total_byte_size, row_group->total_compressed_size);

      if (row_group->columns) {
        writef("  columns:\n");

        column_index = 0;
        column = row_group->columns[column_index];

        while (column) {
          writef("  - file_path=%s, file_offset=%d\n", column->file_path ? column->file_path : "NULL",
                 (i64)column->file_offset);

          if (column && column->meta) {
            writef("    meta: data_type=%d, encodings=%x\n", (i64)column->meta->data_type, column->meta->encodings);

            if (column->meta->path_in_schema) {
              path_in_schema = column->meta->path_in_schema;
              while (*path_in_schema) {
                writef("    path_in_schema=%s\n", **path_in_schema);
                path_in_schema++;
              }
            }

            if (column->meta->encodings) {
              encodings = column->meta->encodings;
              writef("    encodings: ");
              while (*encodings) {
                writef("%d, ", **encodings);
                encodings++;
              }
              writef("\n");
            }
          }

          column = row_group->columns[++column_index];
        }
      }

      row_group = metadata.row_groups[++index];
    }
  }

  parquet_close(&file);
  malloc_destroy(&pool);

  return 0;
}
