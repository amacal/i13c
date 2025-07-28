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
  void *ptrs[16];  // pointers to the fields in the target structure
};

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;

  file->buffer_lease.ptr = NULL;
  file->buffer_lease.size = 0;

  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;

  file->metadata_lease.ptr = NULL;
  file->metadata_lease.size = 0;
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

  // prepare the buffer lease for the footer
  file->buffer_lease.size = 4096;

  // allocate a buffer for the file content
  result = malloc_acquire(file->pool, &file->buffer_lease);
  if (result < 0) goto error_malloc;

  // adjust buffer pointers
  if (stat.st_size < 4096) {
    file->footer_buffer_start = file->buffer_lease.ptr + 4096 - stat.st_size;
    file->footer_buffer_end = file->buffer_lease.ptr + 4096;
  } else {
    file->footer_buffer_start = file->buffer_lease.ptr;
    file->footer_buffer_end = file->buffer_lease.ptr + 4096;
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
  if (file->footer_buffer_start < (char *)file->buffer_lease.ptr) {
    result = -1;
    goto error_read;
  }

  // success
  return 0;

error_read:
  // release the buffer and clear the pointers
  malloc_release(file->pool, &file->buffer_lease);
  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;

error_malloc:
error_stat:
  // close the file descriptor and clear it
  sys_close(file->fd);
  file->fd = 0;

cleanup:
  return result;
}

void parquet_close(struct parquet_file *file) {
  // release the buffer lease using the pool
  malloc_release(file->pool, &file->buffer_lease);
  file->footer_buffer_start = NULL;
  file->footer_buffer_end = NULL;

  // close the file descriptor
  sys_close(file->fd);
  file->fd = 0;

  // release the metadata lease, if exists
  if (file->metadata_lease.ptr) {
    malloc_release(file->pool, &file->metadata_lease);
  }
}

static i64 parquet_metadata_acquire(struct parquet_parse_context *ctx, u64 size) {
  u64 offset;

  // align to next 8 bytes
  offset = (ctx->buffer_tail + 7) & ~7;

  // check if there is enough space
  if (offset + size > ctx->buffer_size) {
    return PARQUET_ERROR_BUFFER_OVERFLOW;
  };

  // move tail
  ctx->buffer_tail = offset + size;

  // allocated space
  return (i64)(ctx->buffer + offset);
}

static void parquet_metadata_release(struct parquet_parse_context *ctx, u64 size) {
  ctx->buffer_tail -= size;
}

static i64 parquet_read_i32_positive(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  i32 value;
  i64 result;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I32) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read i32 as the parquet version
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value <= 0) return PARQUET_ERROR_INVALID_VALUE;

  // value is OK
  *(i32 *)ctx->ptrs[field_id] = value;

  // success
  return result;
}

static i64 parquet_read_i64_positive(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  i64 value, result;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_I64) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read i64 as the number of rows
  result = thrift_read_i64(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value is within the valid range
  if (value < 0) return PARQUET_ERROR_INVALID_VALUE;

  // value is OK
  *(i64 *)ctx->ptrs[field_id] = value;

  // success
  return result;
}

static i64 parquet_read_string(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  char *value;
  i64 result, read;
  u32 size;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_BINARY) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read the size of the value string
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) goto cleanup;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // allocate the space for the copy + EOS
  result = parquet_metadata_acquire(ctx, size + 1);
  if (result < 0) goto cleanup;

  // remember allocated memory slice
  value = (char *)result;

  // copy binary content
  result = thrift_read_binary_content(value, size, buffer, buffer_size);
  if (result < 0) goto cleanup_buffer;

  // value is OK
  *(char **)ctx->ptrs[field_id] = value;

  // successs
  return read + result;

cleanup_buffer:
  parquet_metadata_release(ctx, size + 1);

cleanup:
  return result;
}

static i64 parquet_parse_schema_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_schema_element *schema;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 7;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive; // data_type
  fields[2] = (thrift_read_fn)thrift_ignore_field;       // ignored
  fields[3] = (thrift_read_fn)parquet_read_i32_positive; // repetition_type
  fields[4] = (thrift_read_fn)parquet_read_string;       // schema_name
  fields[5] = (thrift_read_fn)parquet_read_i32_positive; // num_children
  fields[6] = (thrift_read_fn)parquet_read_i32_positive; // converted_type

  // schema
  schema = (struct parquet_schema_element *)ctx->target;
  schema->data_type = PARQUET_DATA_TYPE_NONE;
  schema->type_length = -1;
  schema->repetition_type = -1;
  schema->name = NULL;
  schema->num_children = -1;
  schema->converted_type = -1;

  // context
  context.target = schema;
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

  // targets
  context.ptrs[1] = &schema->data_type;
  context.ptrs[3] = &schema->repetition_type;
  context.ptrs[4] = &schema->name;
  context.ptrs[5] = &schema->num_children;
  context.ptrs[6] = &schema->converted_type;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // restore the context
  ctx->buffer_tail = context.buffer_tail;

  // success
  return read;
}

static i64 parquet_parse_schema(
  struct parquet_parse_context *ctx, i16, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
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
  result = parquet_metadata_acquire(ctx, (1 + header.size) * sizeof(struct parquet_schema_element *));
  if (result < 0) return result;

  // remember allocated memory slice
  metadata = (struct parquet_metadata *)ctx->target;
  metadata->schemas = (struct parquet_schema_element **)result;

  // null-terminate the array
  metadata->schemas[header.size] = NULL;

  // allocate memory for the array
  result = parquet_metadata_acquire(ctx, header.size * sizeof(struct parquet_schema_element));
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
  struct parquet_metadata *metadata;

  const u32 FIELDS_SLOTS = 7;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive; // version
  fields[2] = (thrift_read_fn)parquet_parse_schema;      // schema
  fields[3] = (thrift_read_fn)parquet_read_i64_positive; // num_rows
  fields[4] = (thrift_read_fn)thrift_ignore_field;       // ignored
  fields[5] = (thrift_read_fn)thrift_ignore_field;       // ignored
  fields[6] = (thrift_read_fn)parquet_read_string;       // created_by

  // behind target we have metadata
  metadata = (struct parquet_metadata *)ctx->target;

  // targets
  ctx->ptrs[1] = (void *)&metadata->version;
  ctx->ptrs[3] = (void *)&metadata->num_rows;
  ctx->ptrs[6] = (void *)&metadata->created_by;

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

  // prepare the metadata lease
  file->metadata_lease.size = file->buffer_lease.size;

  // acquire memory for the metadata
  result = malloc_acquire(file->pool, &file->metadata_lease);
  if (result < 0) return -1;

  // initialize the context
  ctx.target = metadata;
  ctx.buffer = file->metadata_lease.ptr;
  ctx.buffer_size = file->metadata_lease.size;
  ctx.buffer_tail = 0;

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

static void can_read_i32_positive() {
  struct parquet_parse_context ctx;
  i32 value;

  i64 result;
  const char buffer[] = {0x02}; // zig-zag encoding of version 1

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i32_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 1, "should read value 1");
}

static void can_detect_i32_positive_invalid_type() {
  struct parquet_parse_context ctx;
  i32 value;

  i64 result;
  const char buffer[] = {0x02}; // zig-zag encoding of version 1

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i32_positive(&ctx, 1, THRIFT_TYPE_I16, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == 0, "should not change value");
}

static void can_detect_i32_positive_invalid_value() {
  struct parquet_parse_context ctx;
  i32 value;

  i64 result;
  const char buffer[] = {0x01}; // zig-zag encoding of version -1

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i32_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_VALUE, "should fail with PARQUET_ERROR_INVALID_VALUE");
  assert(value == 0, "should not change value");
}

static void can_propagate_i32_positive_buffer_overflow() {
  struct parquet_parse_context ctx;
  i32 value;

  i64 result;
  const char buffer[] = {};

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i32_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == 0, "should not change value");
}

static void can_read_i64_positive() {
  struct parquet_parse_context ctx;
  i64 value;

  i64 result;
  const char buffer[] = {0xf2, 0x94, 0x12};

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i64_positive(&ctx, 1, THRIFT_TYPE_I64, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 148793, "should read value 148793");
}

static void can_detect_i64_positive_invalid_type() {
  struct parquet_parse_context ctx;
  i64 value;

  i64 result;
  const char buffer[] = {0xf2, 0x94, 0x12};

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the version from the buffer
  result = parquet_read_i64_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == 0, "should not change value");
}

static void can_detect_i64_positive_invalid_value() {
  struct parquet_parse_context ctx;
  i64 value;

  i64 result;
  const char buffer[] = {0x01};

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i64_positive(&ctx, 1, THRIFT_TYPE_I64, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_VALUE, "should fail with PARQUET_ERROR_INVALID_VALUE");
  assert(value == 0, "should not change value");
}

static void can_propagate_i64_positive_buffer_overflow() {
  struct parquet_parse_context ctx;
  i64 value;

  i64 result;
  const char buffer[] = {0xf1};

  // defaults
  ctx.ptrs[1] = &value;
  value = 0;

  // read the value from the buffer
  result = parquet_read_i64_positive(&ctx, 1, THRIFT_TYPE_I64, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == 0, "should not change value");
}

static void can_read_string() {
  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x04, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &value;
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 1;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value != NULL, "should allocate value");
  assert((u64)value % 8 == 0, "should be aligned to 8 bytes");
  assert(ctx.buffer_tail == 13, "should update buffer tail to 13");
  assert_eq_str(value, "i13c", "should read value 'i13c'");
}

static void can_detect_string_invalid_type() {
  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x05, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &value;
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 1;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 1, "should not change buffer tail");
}

static void can_detect_string_buffer_overflow() {
  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  // defaults
  ctx.ptrs[1] = &value;
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 8;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_string_buffer_overflow_01() {
  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0xf0};

  // defaults
  ctx.ptrs[1] = &value;
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_string_buffer_overflow_02() {
  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x04, 'i', '1', '3'};

  // defaults
  ctx.ptrs[1] = &value;
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

void parquet_test_cases(struct runner_context *ctx) {
  // opening and closing cases
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);

  // reading footer cases
  test_case(ctx, "can read i32 positive", can_read_i32_positive);
  test_case(ctx, "can detect i32 positive invalid type", can_detect_i32_positive_invalid_type);
  test_case(ctx, "can detect i32 positive invalid value", can_detect_i32_positive_invalid_value);
  test_case(ctx, "can propagate i32 positive buffer overflow", can_propagate_i32_positive_buffer_overflow);

  test_case(ctx, "can read i64 positive", can_read_i64_positive);
  test_case(ctx, "can detect i64 positive invalid type", can_detect_i64_positive_invalid_type);
  test_case(ctx, "can detect i64 positive invalid value", can_detect_i64_positive_invalid_value);
  test_case(ctx, "can propagate i64 positive buffer overflow", can_propagate_i64_positive_buffer_overflow);

  test_case(ctx, "can read string", can_read_string);
  test_case(ctx, "can detect string invalid type", can_detect_string_invalid_type);
  test_case(ctx, "can detect string buffer overflow", can_detect_string_buffer_overflow);
  test_case(ctx, "can propagate string buffer overflow 1", can_propagate_string_buffer_overflow_01);
  test_case(ctx, "can propagate string buffer overflow 2", can_propagate_string_buffer_overflow_02);
}

#endif
