#include "parquet.parse.h"
#include "arena.h"
#include "malloc.h"
#include "parquet.base.h"
#include "runner.h"
#include "thrift.h"
#include "typing.h"

// forward declarations
struct parquet_parse_context;

/// @brief Function type for reading a target structure from the buffer
/// @param ctx Pointer to the parsing context
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*parquet_read_fn)(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size);

struct parquet_parse_context {
  struct arena_allocator *arena; // arena allocator for metadata

  void *target;              // pointer to the target structure to fill
  u64 target_size;           // size of the target structure
  parquet_read_fn target_fn; // function to read the target structure

  void *ptrs[16];         // pointers to the fields in the target structure
  thrift_read_fn *fields; // array of thrift read functions for the fields
  u64 fields_size;        // number of fields in the target structure
};

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
  if (value < 0) return PARQUET_ERROR_INVALID_VALUE;

  // value is OK, field will be found
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

  // value is OK, field will be found
  *(i64 *)ctx->ptrs[field_id] = value;

  // success
  return result;
}

static i64 parquet_read_string(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  char *value;
  i64 result, read;
  u32 size;
  u64 cursor;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_BINARY) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read the size of the value string
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // remember the cursor
  cursor = ctx->arena->cursor;

  // allocate the space for the copy + EOS
  result = arena_acquire(ctx->arena, size + 1, (void **)&value);
  if (result < 0) goto cleanup;

  // copy binary content, it also includes null terminator EOS
  result = thrift_read_binary_content(value, size, buffer, buffer_size);
  if (result < 0) goto cleanup;

  // value is OK, field will be found
  *(char **)ctx->ptrs[field_id] = value;

  // success
  return read + result;

cleanup:
  // revert the arena to the previous state
  arena_revert(ctx->arena, cursor);

  // failure
  return result;
}

static i64 parquet_read_list(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_parse_context context;
  struct thrift_list_header header;

  void *ptr;
  void **ptrs;
  i64 result, read;
  u32 index;
  u64 cursor;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_LIST) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read the size of the schemas list
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // remember the cursor
  cursor = ctx->arena->cursor;

  // allocate memory for the pointers
  result = arena_acquire(ctx->arena, 8 + header.size * 8, (void **)&ptrs);
  if (result < 0) return result;

  // null-terminate the array
  ptrs[header.size] = NULL;

  // allocate memory for the array
  if (ctx->target_size > 0) {
    result = arena_acquire(ctx->arena, header.size * ctx->target_size, &ptr);
    if (result < 0) goto cleanup;

    // fill out the array of schema elements
    for (index = 0; index < header.size; index++) {
      ptrs[index] = ptr;
      ptr += ctx->target_size;
    }
  }

  // initialize nested context
  context.arena = ctx->arena;

  // parse each schema element
  for (index = 0; index < header.size; index++) {
    // set the target for the nested context
    context.target = ctx->target_size > 0 ? ptrs[index] : &ptrs[index];

    // read the next schema element
    result = ctx->target_fn(&context, buffer, buffer_size);
    if (result < 0) goto cleanup;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // value is OK, field will be found
  *(void **)ctx->ptrs[field_id] = ptrs;

  // success
  return read;

cleanup:
  // revert the arena to the previous state
  arena_revert(ctx->arena, cursor);

  // failure
  return result;
}

static i64 parquet_read_struct(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_parse_context context;
  i64 result, read;
  void *data;
  u64 cursor;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_STRUCT) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // remember the cursor
  cursor = ctx->arena->cursor;

  // allocate memory for the struct
  result = arena_acquire(ctx->arena, ctx->target_size, &data);
  if (result < 0) return result;

  // context
  context.target = data;
  context.arena = ctx->arena;

  // read the next schema element
  result = ctx->target_fn(&context, buffer, buffer_size);
  if (result < 0) goto cleanup;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // value is OK, field will be found
  *(void **)ctx->ptrs[field_id] = data;

  // success
  return read;

cleanup:
  // revert the arena to the previous state
  arena_revert(ctx->arena, cursor);

  // failure
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
  fields[2] = (thrift_read_fn)parquet_read_i32_positive; // type_length
  fields[3] = (thrift_read_fn)parquet_read_i32_positive; // repetition_type
  fields[4] = (thrift_read_fn)parquet_read_string;       // schema_name
  fields[5] = (thrift_read_fn)parquet_read_i32_positive; // num_children
  fields[6] = (thrift_read_fn)parquet_read_i32_positive; // converted_type

  // schema
  schema = (struct parquet_schema_element *)ctx->target;
  schema->data_type = PARQUET_DATA_TYPE_NONE;
  schema->type_length = PARQUET_UNKNOWN_VALUE;
  schema->repetition_type = PARQUET_REPETITION_TYPE_NONE;
  schema->name = PARQUET_NULL_VALUE;
  schema->num_children = PARQUET_UNKNOWN_VALUE;
  schema->converted_type = PARQUET_UNKNOWN_VALUE;

  // context
  context.target = schema;
  context.arena = ctx->arena;

  // targets
  context.ptrs[1] = &schema->data_type;
  context.ptrs[2] = &schema->type_length;
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

  // success
  return read;
}

static i64 parquet_read_string_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {

  // ptr 0 is the target for the string
  ctx->ptrs[0] = ctx->target;

  // call the string reader at ptr 0
  return parquet_read_string(ctx, 0, THRIFT_TYPE_BINARY, buffer, buffer_size);
}

static i64 parquet_read_list_string(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = 0;
  ctx->target_fn = (parquet_read_fn)parquet_read_string_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_read_i32_positive_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {

  // ptr 0 is the target for the i32
  ctx->ptrs[0] = ctx->target;

  // call the i32 reader at ptr 0
  return parquet_read_i32_positive(ctx, 0, THRIFT_TYPE_I32, buffer, buffer_size);
}

static i64 parquet_read_list_i32_positive(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(i32);
  ctx->target_fn = (parquet_read_fn)parquet_read_i32_positive_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_parse_schemas(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(struct parquet_schema_element);
  ctx->target_fn = (parquet_read_fn)parquet_parse_schema_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64
parquet_parse_encoding_stats_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_page_encoding_stats *element;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 4;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive; // page_type
  fields[2] = (thrift_read_fn)parquet_read_i32_positive; // encoding
  fields[3] = (thrift_read_fn)parquet_read_i32_positive; // count

  // page_encoding_stats
  element = (struct parquet_page_encoding_stats *)ctx->target;
  element->page_type = PARQUET_PAGE_TYPE_NONE;
  element->encoding = PARQUET_ENCODING_NONE;
  element->count = PARQUET_UNKNOWN_VALUE;

  // context
  context.target = element;
  context.arena = ctx->arena;

  // targets
  context.ptrs[1] = &element->page_type;
  context.ptrs[2] = &element->encoding;
  context.ptrs[3] = &element->count;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

static i64 parquet_parse_encoding_stats(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(struct parquet_page_encoding_stats);
  ctx->target_fn = (parquet_read_fn)parquet_parse_encoding_stats_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_parse_column_meta_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_column_meta *meta;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 14;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive;      // data_type
  fields[2] = (thrift_read_fn)parquet_read_list_i32_positive; // encodings
  fields[3] = (thrift_read_fn)parquet_read_list_string;       // path_in_schema
  fields[4] = (thrift_read_fn)parquet_read_i32_positive;      // compression_codec
  fields[5] = (thrift_read_fn)parquet_read_i64_positive;      // num_values
  fields[6] = (thrift_read_fn)parquet_read_i64_positive;      // total_uncompressed_size
  fields[7] = (thrift_read_fn)parquet_read_i64_positive;      // total_compressed_size
  fields[8] = (thrift_read_fn)thrift_ignore_field;            // key_value_metadata
  fields[9] = (thrift_read_fn)parquet_read_i64_positive;      // data_page_offset
  fields[10] = (thrift_read_fn)parquet_read_i64_positive;     // index_page_offset
  fields[11] = (thrift_read_fn)parquet_read_i64_positive;     // dictionary_page_offset
  fields[12] = (thrift_read_fn)thrift_ignore_field;           // statistics
  fields[13] = (thrift_read_fn)parquet_parse_encoding_stats;  // encoding_stats

  // meta
  meta = (struct parquet_column_meta *)ctx->target;
  meta->data_type = PARQUET_DATA_TYPE_NONE;
  meta->encodings = PARQUET_NULL_VALUE;
  meta->path_in_schema = PARQUET_NULL_VALUE;
  meta->compression_codec = PARQUET_COMPRESSION_NONE;
  meta->num_values = PARQUET_UNKNOWN_VALUE;
  meta->total_uncompressed_size = PARQUET_UNKNOWN_VALUE;
  meta->total_compressed_size = PARQUET_UNKNOWN_VALUE;
  meta->data_page_offset = PARQUET_UNKNOWN_VALUE;
  meta->index_page_offset = PARQUET_UNKNOWN_VALUE;
  meta->dictionary_page_offset = PARQUET_UNKNOWN_VALUE;
  meta->statistics = PARQUET_NULL_VALUE;
  meta->encoding_stats = PARQUET_NULL_VALUE;

  // context
  context.target = meta;
  context.arena = ctx->arena;

  // targets
  context.ptrs[1] = &meta->data_type;
  context.ptrs[2] = &meta->encodings;
  context.ptrs[3] = &meta->path_in_schema;
  context.ptrs[4] = &meta->compression_codec;
  context.ptrs[5] = &meta->num_values;
  context.ptrs[6] = &meta->total_uncompressed_size;
  context.ptrs[7] = &meta->total_compressed_size;
  context.ptrs[9] = &meta->data_page_offset;
  context.ptrs[10] = &meta->index_page_offset;
  context.ptrs[11] = &meta->dictionary_page_offset;
  context.ptrs[12] = &meta->statistics;
  context.ptrs[13] = &meta->encoding_stats;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

static i64 parquet_parse_column_meta(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(struct parquet_column_meta);
  ctx->target_fn = (parquet_read_fn)parquet_parse_column_meta_element;

  // call generic struct reader
  return parquet_read_struct(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_parse_column_chunk_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_column_chunk *column_chunk;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 4;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_string;       // file_path
  fields[2] = (thrift_read_fn)parquet_read_i64_positive; // file_offset
  fields[3] = (thrift_read_fn)parquet_parse_column_meta; // meta

  // column_chunk
  column_chunk = (struct parquet_column_chunk *)ctx->target;
  column_chunk->file_path = PARQUET_NULL_VALUE;
  column_chunk->file_offset = PARQUET_UNKNOWN_VALUE;
  column_chunk->meta = PARQUET_NULL_VALUE;

  // context
  context.target = column_chunk;
  context.arena = ctx->arena;

  // targets
  context.ptrs[1] = &column_chunk->file_path;
  context.ptrs[2] = &column_chunk->file_offset;
  context.ptrs[3] = &column_chunk->meta;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

static i64 parquet_parse_column_chunks(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(struct parquet_column_chunk);
  ctx->target_fn = (parquet_read_fn)parquet_parse_column_chunk_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_parse_row_group_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_row_group *row_group;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 8;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_parse_column_chunks; // columns
  fields[2] = (thrift_read_fn)parquet_read_i64_positive;   // total_byte_size
  fields[3] = (thrift_read_fn)parquet_read_i64_positive;   // num_rows
  fields[4] = (thrift_read_fn)thrift_ignore_field;         // sorting_columns
  fields[5] = (thrift_read_fn)parquet_read_i64_positive;   // file_offset
  fields[6] = (thrift_read_fn)parquet_read_i64_positive;   // total_compressed_size
  fields[7] = (thrift_read_fn)thrift_ignore_field;         // ordinal

  // row_group
  row_group = (struct parquet_row_group *)ctx->target;
  row_group->columns = PARQUET_NULL_VALUE;
  row_group->total_byte_size = PARQUET_UNKNOWN_VALUE;
  row_group->num_rows = PARQUET_UNKNOWN_VALUE;
  row_group->file_offset = PARQUET_UNKNOWN_VALUE;
  row_group->total_compressed_size = PARQUET_UNKNOWN_VALUE;

  // context
  context.target = row_group;
  context.arena = ctx->arena;

  // targets
  context.ptrs[1] = &row_group->columns;
  context.ptrs[2] = &row_group->total_byte_size;
  context.ptrs[3] = &row_group->num_rows;
  context.ptrs[5] = &row_group->file_offset;
  context.ptrs[6] = &row_group->total_compressed_size;

  // default
  read = 0;

  // delegate content reading to the thrift function
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // success
  return read;
}

static i64 parquet_parse_row_groups(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {

  // fill up the context
  ctx->target_size = sizeof(struct parquet_row_group);
  ctx->target_fn = (parquet_read_fn)parquet_parse_row_group_element;

  // call generic list reader
  return parquet_read_list(ctx, field_id, field_type, buffer, buffer_size);
}

static i64 parquet_parse_footer(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_metadata *metadata;

  const u32 FIELDS_SLOTS = 7;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive; // version
  fields[2] = (thrift_read_fn)parquet_parse_schemas;     // schemas
  fields[3] = (thrift_read_fn)parquet_read_i64_positive; // num_rows
  fields[4] = (thrift_read_fn)parquet_parse_row_groups;  // row_groups
  fields[5] = (thrift_read_fn)thrift_ignore_field;       // ignored
  fields[6] = (thrift_read_fn)parquet_read_string;       // created_by

  // behind target we have metadata
  metadata = (struct parquet_metadata *)ctx->target;
  metadata->version = PARQUET_UNKNOWN_VALUE;
  metadata->schemas = PARQUET_NULL_VALUE;
  metadata->num_rows = PARQUET_UNKNOWN_VALUE;
  metadata->row_groups = PARQUET_NULL_VALUE;
  metadata->created_by = PARQUET_NULL_VALUE;

  // targets
  ctx->ptrs[1] = &metadata->version;
  ctx->ptrs[2] = &metadata->schemas;
  ctx->ptrs[3] = &metadata->num_rows;
  ctx->ptrs[4] = &metadata->row_groups;
  ctx->ptrs[6] = &metadata->created_by;

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

  // initialize the context
  ctx.target = metadata;
  ctx.arena = &file->arena;

  // initialize
  buffer = file->footer.start;
  buffer_size = file->footer.size;

  // parse the footer as the root structure
  result = parquet_parse_footer(&ctx, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  buffer += result;
  buffer_size -= result;

  return 0;
}

#if defined(I13C_TESTS)

static void can_read_i32_positive() {
  struct parquet_parse_context ctx;
  i32 value;

  i64 result;
  const char buffer[] = {0x02}; // zig-zag encoding of version 1

  // defaults
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

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
  value = 0;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_i64_positive(&ctx, 1, THRIFT_TYPE_I64, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == 0, "should not change value");
}

static void can_read_string() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  const char buffer[] = {0x04, 'i', '1', '3', 'c'};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value != NULL, "should allocate value");
  assert((u64)value % 8 == 0, "should be aligned to 8 bytes");

  assert_eq_str(value, "i13c", "should read value 'i13c'");
  assert(arena_occupied(&arena) == 8, "should occupy 8 bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_string_invalid_type() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  const char buffer[] = {0x05, 'i', '1', '3', 'c'};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_string_arena_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char *value;
  void *ptr;

  i64 result;
  u64 occupied;
  const char buffer[] = {0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 8, &ptr);
  occupied = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == occupied, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_string_buffer_overflow_01() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  const char buffer[] = {0xf0};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_string_buffer_overflow_02() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char *value;

  i64 result;
  const char buffer[] = {0x04, 'i', '1', '3'};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &value;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static i64 can_read_list_item(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  // hide the target pointer behind 0
  ctx->ptrs[0] = ctx->target;

  // delegate reading to known function
  return parquet_read_i64_positive(ctx, 0, THRIFT_TYPE_I64, buffer, buffer_size);
}

static void can_read_list() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 6, "should read six bytes");
  assert(values != NULL, "should allocate values");
  assert((u64)values % 8 == 0, "should be aligned to 8 bytes");
  assert((u64)values[0] % 8 == 0, "should be aligned to 8 bytes");
  assert(*values[0] == 1, "should read value 1");
  assert((u64)values[1] % 8 == 0, "should be aligned to 8 bytes");
  assert(*values[1] == 2, "should read value 2");
  assert((u64)values[2] % 8 == 0, "should be aligned to 8 bytes");
  assert(*values[2] == 3, "should read value 3");
  assert((u64)values[3] % 8 == 0, "should be aligned to 8 bytes");
  assert(*values[3] == 1337, "should read value 1337");
  assert(values[4] == 0, "should be null-terminated");
  assert(arena_occupied(&arena) == 40 + 32, "should occupy 72 bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_invalid_type() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_arena_overflow_01() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;
  void *ptr;

  i64 result;
  u64 cursor;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 8, &ptr);
  cursor = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == cursor, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_buffer_overflow_02() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;
  void *ptr;

  i64 result;
  u64 cursor;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 56, &ptr);
  cursor = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer) - 1);

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == cursor, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_list_buffer_overflow_01() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  const char buffer[] = {};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_list_buffer_overflow_02() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;
  ctx.target_size = 8;
  ctx.target_fn = (parquet_read_fn)can_read_list_item;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer) - 1);

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static i64 can_read_struct_item(struct parquet_parse_context *, const char *, u64 buffer_size) {
  return buffer_size ? 1 : THRIFT_ERROR_BUFFER_OVERFLOW;
}

static void can_read_struct() {
  struct sample {
    i32 len;
    char **name;
  };

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(struct sample);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value != NULL, "should allocate value");
  assert((u64)value % 8 == 0, "should be aligned to 8 bytes");
  assert(arena_occupied(&arena) == 16, "should occupy 16 bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_struct_invalid_type() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_struct_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  struct sample *value;

  void *ptr;
  i64 result;
  u64 cursor;
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 6, &ptr);
  cursor = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == cursor, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_struct_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  const char buffer[] = {};

  // defaults
  value = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_read_list_strings() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 10, "should read ten bytes");
  assert(values != NULL, "should allocate values");
  assert((u64)values % 8 == 0, "container should be aligned to 8 bytes");

  assert((u64)values[0] % 8 == 0, "entry should be aligned to 8 bytes");
  assert_eq_str(values[0], "abc", "value should be 'abc'");

  assert((u64)values[1] % 8 == 0, "entry should be aligned to 8 bytes");
  assert_eq_str(values[1], "i13c", "value should be 'i13c'");

  assert(values[2] == 0, "should be null-terminated");
  assert(arena_occupied(&arena) == 24 + 8 + 8, "should occupy 40 bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_strings_invalid_type() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_strings_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char **values;
  void *ptr;

  i64 result;
  u64 cursor;
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 8, &ptr);
  cursor = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == cursor, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_list_strings_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3'};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_read_list_i32_positive() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 6, "should read six bytes");
  assert(values != NULL, "should allocate values");
  assert((u64)values % 8 == 0, "container should be aligned to 8 bytes");

  assert((u64)values[0] % 4 == 0, "entry should be aligned to 4 bytes");
  assert(*values[0] == 1, "should read value 1");

  assert((u64)values[1] % 4 == 0, "entry should be aligned to 4 bytes");
  assert(*values[1] == 2, "should read value 2");

  assert((u64)values[2] % 4 == 0, "entry should be aligned to 4 bytes");
  assert(*values[2] == 3, "should read value 3");

  assert((u64)values[3] % 4 == 0, "entry should be aligned to 4 bytes");
  assert(*values[3] == 1337, "should read value 1337");

  assert(values[4] == NULL, "should be null-terminated");
  assert(arena_occupied(&arena) == 40 + 16, "should occupy 56 bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_i32_positive_invalid_type() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_detect_list_i32_positive_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i32 **values;
  void *ptr;

  i64 result;
  u64 cursor;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  arena_acquire(&arena, arena_available(&arena) - 48, &ptr);
  cursor = arena_occupied(&arena);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == ARENA_ERROR_OUT_OF_MEMORY, "should fail with ARENA_ERROR_OUT_OF_MEMORY");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == cursor, "shouldn't increase occupied size");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

static void can_propagate_list_i32_positive_buffer_overflow() {
  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2};

  // defaults
  values = NULL;

  // arena
  malloc_init(&pool);
  arena_init(&arena, &pool, 4096, 4096);

  // context
  ctx.arena = &arena;
  ctx.ptrs[1] = &values;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(arena_occupied(&arena) == 0, "shouldn't occupy any bytes");

  // release
  arena_destroy(&arena);
  malloc_destroy(&pool);
}

void parquet_test_cases_parse(struct runner_context *ctx) {
  // i32 cases
  test_case(ctx, "can read i32 positive", can_read_i32_positive);
  test_case(ctx, "can detect i32 positive invalid type", can_detect_i32_positive_invalid_type);
  test_case(ctx, "can detect i32 positive invalid value", can_detect_i32_positive_invalid_value);
  test_case(ctx, "can propagate i32 positive buffer overflow", can_propagate_i32_positive_buffer_overflow);

  // i64 cases
  test_case(ctx, "can read i64 positive", can_read_i64_positive);
  test_case(ctx, "can detect i64 positive invalid type", can_detect_i64_positive_invalid_type);
  test_case(ctx, "can detect i64 positive invalid value", can_detect_i64_positive_invalid_value);
  test_case(ctx, "can propagate i64 positive buffer overflow", can_propagate_i64_positive_buffer_overflow);

  // string cases
  test_case(ctx, "can read string", can_read_string);
  test_case(ctx, "can detect string invalid type", can_detect_string_invalid_type);
  test_case(ctx, "can detect string buffer overflow", can_detect_string_arena_overflow);
  test_case(ctx, "can propagate string buffer overflow 1", can_propagate_string_buffer_overflow_01);
  test_case(ctx, "can propagate string buffer overflow 2", can_propagate_string_buffer_overflow_02);

  // list cases
  test_case(ctx, "can read list", can_read_list);
  test_case(ctx, "can detect list invalid type", can_detect_list_invalid_type);
  test_case(ctx, "can detect list buffer overflow 1", can_detect_list_arena_overflow_01);
  test_case(ctx, "can detect list buffer overflow 2", can_detect_list_buffer_overflow_02);
  test_case(ctx, "can propagate list buffer overflow 1", can_propagate_list_buffer_overflow_01);
  test_case(ctx, "can propagate list buffer overflow 2", can_propagate_list_buffer_overflow_02);

  // struct cases
  test_case(ctx, "can read struct", can_read_struct);
  test_case(ctx, "can detect struct invalid type", can_detect_struct_invalid_type);
  test_case(ctx, "can detect struct buffer overflow", can_detect_struct_buffer_overflow);
  test_case(ctx, "can propagate struct buffer overflow", can_propagate_struct_buffer_overflow);

  // list of strings cases
  test_case(ctx, "can read list strings", can_read_list_strings);
  test_case(ctx, "can detect list strings invalid type", can_detect_list_strings_invalid_type);
  test_case(ctx, "can detect list strings buffer overflow", can_detect_list_strings_buffer_overflow);
  test_case(ctx, "can propagate list strings buffer overflow", can_propagate_list_strings_buffer_overflow);

  // list of i32 cases
  test_case(ctx, "can read list i32 positive", can_read_list_i32_positive);
  test_case(ctx, "can detect list i32 positive invalid type", can_detect_list_i32_positive_invalid_type);
  test_case(ctx, "can detect list i32 positive buffer overflow", can_detect_list_i32_positive_buffer_overflow);
  test_case(ctx, "can propagate list i32 positive buffer overflow", can_propagate_list_i32_positive_buffer_overflow);
}

#endif
