#include "parquet.h"
#include "malloc.h"
#include "runner.h"
#include "stdout.h"
#include "sys.h"
#include "thrift.h"
#include "typing.h"

struct parquet_parse_context {
  void *target;    // pointer to the target structure to fill
  char *buffer;    // the buffer behind metadata storage
  u64 buffer_size; // the size of the buffer
  u64 buffer_tail; // the tail of the buffer
};

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;

  file->footer_buffer = NULL;
  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;
  file->footer_buffer_size = 0;

  file->metadata_buffer = NULL;
  file->metadata_buffer_size = 0;
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
  file->footer_buffer_size = 4096;
  result = malloc(file->pool, file->footer_buffer_size);
  if (result == 0) goto error_malloc;
  else file->footer_buffer = (char *)result;

  // adjust buffer pointers
  if (stat.st_size < 4096) {
    file->footer_buffer_start = file->footer_buffer + 4096 - stat.st_size;
    file->footer_buffer_end = file->footer_buffer + 4096;
  } else {
    file->footer_buffer_start = file->footer_buffer;
    file->footer_buffer_end = file->footer_buffer + 4096;
  }

  completed = 0;
  remaining = file->footer_buffer_end - file->footer_buffer_start;
  offset = stat.st_size - remaining;

  // fill the buffer
  while (remaining > 0) {
    // read next chunk of the footer
    result = sys_pread(file->fd, file->footer_buffer_start + completed, remaining, offset);
    if (result < 0) goto error_read;

    // move the offset and completed bytes
    offset += result;
    completed += result;
    remaining -= result;
  }

  // recompute buffer boundaries, next op is properly aligned
  file->footer_size = *(u32 *)(file->footer_buffer_end - 8);
  file->footer_buffer_end = file->footer_buffer_end - 8;
  file->footer_buffer_start = file->footer_buffer_end - file->footer_size;

  // check if the buffer is too small to handle the footer
  if (file->footer_buffer_start < file->footer_buffer) {
    result = -1;
    goto error_read;
  }

  // success
  return 0;

error_read:
  free(file->pool, file->footer_buffer, file->footer_buffer_size);
  file->footer_buffer = NULL;
  file->footer_buffer_size = 0;
  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;

error_malloc:
error_stat:
  sys_close(file->fd);
  file->fd = 0;

cleanup:
  return result;
}

void parquet_close(struct parquet_file *file) {
  // free the buffer using the pool
  free(file->pool, file->footer_buffer, file->footer_buffer_size);
  file->footer_buffer = NULL;
  file->footer_buffer_size = 0;
  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;

  // close the file descriptor
  sys_close(file->fd);
  file->fd = 0;

  // free the metadata buffer, if exists
  if (file->metadata_buffer) {
    free(file->pool, file->metadata_buffer, file->metadata_buffer_size);
    file->metadata_buffer = NULL;
    file->metadata_buffer_size = 0;
  }
}

static i64 parquet_metadata_alloc(struct parquet_parse_context *ctx, u64 size) {
  u64 offset;

  // align to next 8 bytes
  size = (size + 7) & ~7;

  // check if there is enough space
  if (ctx->buffer_tail + size > ctx->buffer_size) return -1;

  // move tail
  offset = ctx->buffer_tail;
  ctx->buffer_tail += size;

  // allocated space
  return (i64)(ctx->buffer + offset);
}

static i64 parquet_read_version(struct parquet_parse_context *ctx,
                                enum thrift_type field_type,
                                const char *buffer,
                                u64 buffer_size) {
  struct parquet_metadata *metadata;

  // target points at the metadata structure
  metadata = (struct parquet_metadata *)ctx->target;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return -1;
  }

  // read i32 as the parquet version
  return thrift_read_i32(&metadata->version, buffer, buffer_size);
}

static i64 parquet_read_num_rows(struct parquet_parse_context *ctx,
                                 enum thrift_type field_type,
                                 const char *buffer,
                                 u64 buffer_size) {
  struct parquet_metadata *metadata;

  // target points at the metadata structure
  metadata = (struct parquet_metadata *)ctx->target;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I64) {
    return -1;
  }

  // read i64 as the number of rows
  return thrift_read_i64(&metadata->num_rows, buffer, buffer_size);
}

static i64 parquet_read_created_by(struct parquet_parse_context *ctx,
                                   enum thrift_type field_type,
                                   const char *buffer,
                                   u64 buffer_size) {
  struct parquet_metadata *metadata;
  i64 result, read;
  u32 size;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_BINARY) {
    return -1;
  }

  // read the size of the created_by string
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // allocate the space for the copy
  result = parquet_metadata_alloc(ctx, size + 1);
  if (result < 0) return result;

  // remember allocated memory slice
  metadata = (struct parquet_metadata *)ctx->target;
  metadata->created_by = (char *)result;

  // copy binary content
  result = thrift_read_binary_content(metadata->created_by, size, buffer, buffer_size);
  if (result < 0) return result;

  // successs
  return read + result;
}

static i64 parquet_read_schema_type(struct parquet_parse_context *ctx,
                                    enum thrift_type field_type,
                                    const char *buffer,
                                    u64 buffer_size) {
  struct parquet_schema_element *schema;
  i64 result;
  i32 value;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return -1;
  }

  // read the schema type as an i32
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value < 0 || value >= PARQUET_DATA_TYPE_SIZE) {
    return -1;
  }

  // remember allocated memory slice
  schema = (struct parquet_schema_element *)ctx->target;
  schema->data_type = (enum parquet_data_type)value;

  // success
  return result;
}

static i64 parquet_read_repetition_type(struct parquet_parse_context *ctx,
                                        enum thrift_type field_type,
                                        const char *buffer,
                                        u64 buffer_size) {
  struct parquet_schema_element *schema;
  i64 result;
  i32 value;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return -1;
  }

  // read the repetition type as an i32
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value < 0 || value >= PARQUET_REPETITION_TYPE_SIZE) {
    return -1;
  }

  // remember allocated memory slice
  schema = (struct parquet_schema_element *)ctx->target;
  schema->repetition_type = (enum parquet_repetition_type)value;

  // success
  return result;
}

static i64 parquet_read_schema_name(struct parquet_parse_context *ctx,
                                    enum thrift_type field_type,
                                    const char *buffer,
                                    u64 buffer_size) {
  struct parquet_schema_element *schema;
  i64 result, read;
  u32 size;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_BINARY) {
    return -1;
  }

  // read the size of the schema name string
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // allocate the space for the copy
  result = parquet_metadata_alloc(ctx, size + 1);
  if (result < 0) return result;

  // remember allocated memory slice
  schema = (struct parquet_schema_element *)ctx->target;
  schema->name = (char *)result;

  // copy binary content
  result = thrift_read_binary_content(schema->name, size, buffer, buffer_size);
  if (result < 0) return result;

  // successs
  return read + result;
}

static i64 parquet_read_num_children(struct parquet_parse_context *ctx,
                                     enum thrift_type field_type,
                                     const char *buffer,
                                     u64 buffer_size) {
  struct parquet_schema_element *schema;
  i64 result;
  i32 value;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return -1;
  }

  // read the number of children as an i32
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value < 0) return -1;

  // remember allocated memory slice
  schema = (struct parquet_schema_element *)ctx->target;
  schema->num_children = value;

  // success
  return result;
}

static i64 parquet_read_converted_type(struct parquet_parse_context *ctx,
                                       enum thrift_type field_type,
                                       const char *buffer,
                                       u64 buffer_size) {
  struct parquet_schema_element *schema;
  i64 result;
  i32 value;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return -1;
  }

  // read the schema type as an i32
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value < 0 || value >= PARQUET_CONVERTED_TYPE_SIZE) {
    return -1;
  }

  // remember allocated memory slice
  schema = (struct parquet_schema_element *)ctx->target;
  schema->converted_type = (enum parquet_converted_type)value;

  // success
  return result;
}

static i64 parquet_parse_schema_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_schema_element *schema;

  const u32 FIELDS_SLOTS = 7;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_schema_type;     // schema_type
  fields[2] = (thrift_read_fn)thrift_ignore_field;          // ignored
  fields[3] = (thrift_read_fn)parquet_read_repetition_type; // repetition_type
  fields[4] = (thrift_read_fn)parquet_read_schema_name;     // schema_name
  fields[5] = (thrift_read_fn)parquet_read_num_children;    // num_children
  fields[6] = (thrift_read_fn)parquet_read_converted_type;  // converted_type

  // get schema
  schema = (struct parquet_schema_element *)ctx->target;
  schema->data_type = -1;
  schema->type_length = -1;
  schema->repetition_type = -1;
  schema->name = NULL;
  schema->num_children = -1;
  schema->converted_type = -1;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(ctx, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

static i64 parquet_parse_schema(struct parquet_parse_context *ctx,
                                enum thrift_type field_type,
                                const char *buffer,
                                u64 buffer_size) {
  struct parquet_parse_context context;
  struct parquet_metadata *metadata;
  struct thrift_list_header header;

  i64 result, read;
  u32 index;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_LIST) {
    return -1;
  }

  // read the size of the schemas list
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // schemas must be a list of structs
  if (header.type != THRIFT_TYPE_STRUCT) {
    return -1;
  }

  // allocate memory for the pointers
  result = parquet_metadata_alloc(ctx, (1 + header.size) * sizeof(struct parquet_schema_element *));
  if (result < 0) return result;

  // remember allocated memory slice
  metadata = (struct parquet_metadata *)ctx->target;
  metadata->schemas = (struct parquet_schema_element **)result;

  // null-terminate the array
  metadata->schemas[header.size] = NULL;

  // allocate memory for the array
  result = parquet_metadata_alloc(ctx, header.size * sizeof(struct parquet_schema_element));
  if (result < 0) return result;

  // fill out the array of schema elements
  for (index = 0; index < header.size; index++) {
    metadata->schemas[index] = (struct parquet_schema_element *)(result);
    result += sizeof(struct parquet_schema_element);
  }

  // initialize nested context
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

  // parse each schema element
  for (index = 0; index < header.size; index++) {
    // set the target for the nested context
    context.target = metadata->schemas[index];

    // read the next schema element
    result = parquet_parse_schema_element(&context, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;

    // apply buffer tail
    ctx->buffer_tail = context.buffer_tail;
  }

  // success
  return read;
}

static i64 parquet_parse_footer(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;

  const u32 FIELDS_SLOTS = 7;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_version;    // version
  fields[2] = (thrift_read_fn)parquet_parse_schema;    // schema
  fields[3] = (thrift_read_fn)parquet_read_num_rows;   // num_rows
  fields[4] = (thrift_read_fn)thrift_ignore_field;     // ignored
  fields[5] = (thrift_read_fn)thrift_ignore_field;     // ignored
  fields[6] = (thrift_read_fn)parquet_read_created_by; // created_by

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(ctx, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

i64 parquet_parse(struct parquet_file *file, struct parquet_metadata *metadata) {
  i64 result;
  char *buffer;
  u64 buffer_size;
  struct parquet_parse_context ctx;

  // allocate memory for the metadata
  result = malloc(file->pool, file->footer_buffer_size);
  if (result <= 0) return -1;

  // initialize the context
  ctx.target = metadata;
  ctx.buffer = (char *)result;
  ctx.buffer_size = file->footer_buffer_size;
  ctx.buffer_tail = 0;

  // remember the metadata buffer
  file->metadata_buffer = ctx.buffer;
  file->metadata_buffer_size = file->footer_buffer_size;

  // initialize
  buffer = file->footer_buffer_start;
  buffer_size = file->footer_size;

  // parse the footer as the root structure
  result = parquet_parse_footer(&ctx, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  buffer += result;
  buffer_size -= result;

  return 0;
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

void parquet_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);
}

#endif
