#include "parquet.h"
#include "arena.h"
#include "dom.h"
#include "malloc.h"
#include "runner.h"
#include "stdout.h"
#include "sys.h"
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

void parquet_init(struct parquet_file *file, struct malloc_pool *pool) {
  file->fd = 0;
  file->pool = pool;

  file->footer.lease.ptr = NULL;
  file->footer.lease.size = 0;

  file->footer.start = NULL;
  file->footer.end = NULL;

  arena_init(&file->arena, pool, 4096, 32 * 4096);
}

i64 parquet_open(struct parquet_file *file, const char *path) {
  const u64 DEFAULT_BUFFER_SIZE = 4096;

  i64 result;
  file_stat stat;
  u64 offset, completed, remaining;

  // check if the path can be opened
  result = sys_open(path, O_RDONLY, 0);
  if (result < 0) goto cleanup;
  else file->fd = result;

  // check the size of the file behind the file descriptor
  result = sys_fstat(file->fd, &stat);
  if (result < 0) goto cleanup_file;

  // check if the file is too small to be a parquet file
  if (stat.st_size < 8) {
    result = PARQUET_ERROR_INVALID_FILE;
    goto cleanup_file;
  }

  // set default parameters
  file->footer.lease.size = DEFAULT_BUFFER_SIZE;

alloc:
  // allocate a buffer for the file content
  result = malloc_acquire(file->pool, &file->footer.lease);
  if (result < 0) goto cleanup_file;

  // adjust buffer pointers
  if (stat.st_size < (i64)file->footer.lease.size) {
    file->footer.start = file->footer.lease.ptr + file->footer.lease.size - stat.st_size;
    file->footer.end = file->footer.lease.ptr + file->footer.lease.size;
  } else {
    file->footer.start = file->footer.lease.ptr;
    file->footer.end = file->footer.lease.ptr + file->footer.lease.size;
  }

  completed = 0;
  remaining = file->footer.end - file->footer.start;
  offset = stat.st_size - remaining;

  // fill the buffer
  while (remaining > 0) {
    // read next chunk of the footer
    result = sys_pread(file->fd, file->footer.start + completed, remaining, offset);
    if (result < 0) goto cleanup_buffer;

    // check if the read was as expected
    if (result == 0) {
      result = PARQUET_ERROR_INVALID_FILE;
      goto cleanup_buffer;
    }

    // move the offset and completed bytes
    offset += result;
    completed += result;
    remaining -= result;
  }

  // recompute buffer boundaries, next op is properly aligned
  file->footer.size = *(u32 *)(file->footer.end - 8);
  file->footer.end = file->footer.end - 8;
  file->footer.start = file->footer.end - file->footer.size;

  // check if the buffer is too small to handle the footer
  if (file->footer.start < (char *)file->footer.lease.ptr) {
    if (file->footer.lease.size > DEFAULT_BUFFER_SIZE) {
      result = PARQUET_ERROR_INVALID_FILE;
      goto cleanup_buffer;
    }

    // there is a hope for better buffer size
    malloc_release(file->pool, &file->footer.lease);
    file->footer.start = NULL;
    file->footer.end = NULL;

    // set the expected value aligned to the next power of 2
    file->footer.lease.size = 1 << (64 - __builtin_clzll(file->footer.size + 7));
    goto alloc;
  }

  // success
  return 0;

cleanup_buffer:
  // release the buffer and clear the pointers
  malloc_release(file->pool, &file->footer.lease);
  file->footer.start = NULL;
  file->footer.end = NULL;

cleanup_file:
  // close the file descriptor and clear it
  sys_close(file->fd);
  file->fd = 0;

cleanup:
  return result;
}

void parquet_close(struct parquet_file *file) {
  // release the buffer lease using the pool
  malloc_release(file->pool, &file->footer.lease);
  file->footer.start = NULL;
  file->footer.end = NULL;

  // close the file descriptor
  sys_close(file->fd);
  file->fd = 0;

  // release arena
  arena_destroy(&file->arena);
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

#endif

#if defined(I13C_PARQUET) || defined(I13C_TESTS)

#define PARQUET_METADATA_TOKENS_SIZE 256
#define PARQUET_METADATA_QUEUE_SIZE 256

struct parquet_metadata_iterator;

typedef void *parquet_metadata_iterator_ctx;
typedef const char *parquet_metadata_iterator_name;
typedef i64 (*parquet_metadata_iterator_fn)(struct parquet_metadata_iterator *iterator, u32 index);

union parquet_metadata_iterator_args {
  u64 value;        // the row value
  const char *name; // the static item name
};

struct parquet_metadata_iterator_item {
  void *ctx; // context of the item

  union parquet_metadata_iterator_args ctx_args; // the context args
  parquet_metadata_iterator_fn ctx_fn;           // the context handler

  union parquet_metadata_iterator_args item_args; // the item args
  parquet_metadata_iterator_fn item_fn;           // the item handler
};

struct parquet_metadata_iterator_tokens {
  u32 count;    // the number of currently occupied tokens
  u32 capacity; // the total number of all available tokens

  // it actually represents all tokens in the iterator
  struct dom_token items[PARQUET_METADATA_TOKENS_SIZE];
};

struct parquet_metadata_iterator_queue {
  u32 count;    // the number of currently occupied slots
  u32 capacity; // the total number of all slots in the queue

  // it actually represents all slots in the queue
  struct parquet_metadata_iterator_item items[PARQUET_METADATA_QUEUE_SIZE];
};

struct parquet_metadata_iterator {
  struct parquet_metadata *metadata;
  struct parquet_metadata_iterator_queue queue;
  struct parquet_metadata_iterator_tokens tokens;
};

static const char *const PARQUET_COMPRESSION_NAMES[PARQUET_COMPRESSION_SIZE] = {
  [PARQUET_COMPRESSION_UNCOMPRESSED] = "UNCOMPRESSED",
  [PARQUET_COMPRESSION_SNAPPY] = "SNAPPY",
  [PARQUET_COMPRESSION_GZIP] = "GZIP",
  [PARQUET_COMPRESSION_LZO] = "LZO",
  [PARQUET_COMPRESSION_BROTLI] = "BROTLI",
  [PARQUET_COMPRESSION_LZ4] = "LZ4",
  [PARQUET_COMPRESSION_ZSTD] = "ZSTD",
  [PARQUET_COMPRESSION_LZ4_RAW] = "LZ4_RAW",
};

static const char *const PARQUET_CONVERTED_TYPE_NAMES[PARQUET_CONVERTED_TYPE_SIZE] = {
  [PARQUET_CONVERTED_TYPE_UTF8] = "UTF8",
  [PARQUET_CONVERTED_TYPE_MAP] = "MAP",
  [PARQUET_CONVERTED_TYPE_MAP_KEY_VALUE] = "MAP_KEY_VALUE",
  [PARQUET_CONVERTED_TYPE_LIST] = "LIST",
  [PARQUET_CONVERTED_TYPE_ENUM] = "ENUM",
  [PARQUET_CONVERTED_TYPE_DECIMAL] = "DECIMAL",
  [PARQUET_CONVERTED_TYPE_DATE] = "DATE",
  [PARQUET_CONVERTED_TYPE_TIME_MILLIS] = "TIME_MILLIS",
  [PARQUET_CONVERTED_TYPE_TIME_MICROS] = "TIME_MICROS",
  [PARQUET_CONVERTED_TYPE_TIMESTAMP_MILLIS] = "TIMESTAMP_MILLIS",
  [PARQUET_CONVERTED_TYPE_TIMESTAMP_MICROS] = "TIMESTAMP_MICROS",
  [PARQUET_CONVERTED_TYPE_UINT8] = "UINT8",
  [PARQUET_CONVERTED_TYPE_UINT16] = "UINT16",
  [PARQUET_CONVERTED_TYPE_UINT32] = "UINT32",
  [PARQUET_CONVERTED_TYPE_UINT64] = "UINT64",
  [PARQUET_CONVERTED_TYPE_INT8] = "INT8",
  [PARQUET_CONVERTED_TYPE_INT16] = "INT16",
  [PARQUET_CONVERTED_TYPE_INT32] = "INT32",
  [PARQUET_CONVERTED_TYPE_INT64] = "INT64",
  [PARQUET_CONVERTED_TYPE_JSON] = "JSON",
  [PARQUET_CONVERTED_TYPE_BSON] = "BSON",
  [PARQUET_CONVERTED_TYPE_INTERVAL] = "INTERVAL",
};

static const char *const PARQUET_DATA_TYPE_NAMES[PARQUET_DATA_TYPE_SIZE] = {
  [PARQUET_DATA_TYPE_BOOLEAN] = "BOOLEAN",
  [PARQUET_DATA_TYPE_INT32] = "INT32",
  [PARQUET_DATA_TYPE_INT64] = "INT64",
  [PARQUET_DATA_TYPE_INT96] = "INT96",
  [PARQUET_DATA_TYPE_FLOAT] = "FLOAT",
  [PARQUET_DATA_TYPE_DOUBLE] = "DOUBLE",
  [PARQUET_DATA_TYPE_BYTE_ARRAY] = "BYTE_ARRAY",
  [PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED] = "PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED",
};

static const char *const PARQUET_ENCODING_NAMES[PARQUET_ENCODING_SIZE] = {
  [PARQUET_ENCODING_PLAIN] = "PLAIN",
  [PARQUET_ENCODING_GROUP_VAR_INT] = "GROUP_VAR_INT",
  [PARQUET_ENCODING_PLAIN_DICTIONARY] = "PLAIN_DICTIONARY",
  [PARQUET_ENCODING_RLE] = "RLE",
  [PARQUET_ENCODING_BIT_PACKED] = "BIT_PACKED",
  [PARQUET_ENCODING_DELTA_BINARY_PACKED] = "DELTA_BINARY_PACKED",
  [PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY] = "DELTA_LENGTH_BYTE_ARRAY",
  [PARQUET_ENCODING_DELTA_BYTE_ARRAY] = "DELTA_BYTE_ARRAY",
  [PARQUET_ENCODING_RLE_DICTIONARY] = "RLE_DICTIONARY",
  [PARQUET_ENCODING_BYTE_STREAM_SPLIT] = "BYTE_STREAM_SPLIT",
};

static const char *const PARQUET_PAGE_TYPE_NAMES[PARQUET_PAGE_TYPE_SIZE] = {
  [PARQUET_PAGE_TYPE_DATA_PAGE] = "DATA_PAGE",
  [PARQUET_PAGE_TYPE_INDEX_PAGE] = "INDEX_PAGE",
  [PARQUET_PAGE_TYPE_DICTIONARY_PAGE] = "DICTIONARY_PAGE",
  [PARQUET_PAGE_TYPE_DATA_PAGE_V2] = "DATA_PAGE_V2",
};

static const char *const PARQUET_REPETITION_TYPE_NAMES[PARQUET_REPETITION_TYPE_SIZE] = {
  [PARQUET_REPETITION_TYPE_REQUIRED] = "REQUIRED",
  [PARQUET_REPETITION_TYPE_OPTIONAL] = "OPTIONAL",
  [PARQUET_REPETITION_TYPE_REPEATED] = "REPEATED",
};

static i64
parquet_dump_enum(struct parquet_metadata_iterator *iterator, u32 index, i32 size, const char *const *names) {
  i32 *value;
  const char *name;

  // check for the capacity, we need only 1 slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // extract the value and the name
  name = NULL;
  value = (i32 *)iterator->queue.items[index].ctx;

  // find mapping if available
  if (*value < size) {
    name = names[*value];
  }

  // output either raw i32 or mapped name
  if (name == NULL) {
    iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
    iterator->tokens.items[iterator->tokens.count].data = (u64)*value;
    iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_I32;
  } else {
    iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
    iterator->tokens.items[iterator->tokens.count].data = (u64)name;
    iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;
  }

  // success
  return 0;
}

static i64 parquet_dump_compression_codec(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_COMPRESSION_SIZE, PARQUET_COMPRESSION_NAMES);
}

static i64 parquet_dump_converted_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_CONVERTED_TYPE_SIZE, PARQUET_CONVERTED_TYPE_NAMES);
}

static i64 parquet_dump_data_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);
}

static i64 parquet_dump_encoding(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_ENCODING_SIZE, PARQUET_ENCODING_NAMES);
}

static i64 parquet_dump_page_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_PAGE_TYPE_SIZE, PARQUET_PAGE_TYPE_NAMES);
}

static i64 parquet_dump_repetition_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_REPETITION_TYPE_SIZE, PARQUET_REPETITION_TYPE_NAMES);
}

static i64 parquet_dump_literal(struct parquet_metadata_iterator *iterator, u32 index, u8 type) {
  u64 value;

  // check for the capacity, we need only 1 slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // extract the value
  switch (type) {
    case DOM_TYPE_I32:
      value = (u64) * (i32 *)iterator->queue.items[index].ctx;
      break;
    case DOM_TYPE_I64:
      value = (u64) * (i64 *)iterator->queue.items[index].ctx;
      break;
    case DOM_TYPE_TEXT:
      value = (u64)(const char *)iterator->queue.items[index].ctx;
      break;
  }

  // value literal
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = value;
  iterator->tokens.items[iterator->tokens.count++].type = type;

  // success
  return 0;
}

static i64 parquet_dump_i32(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_I32);
}

static i64 parquet_dump_i64(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_I64);
}

static i64 parquet_dump_text(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_TEXT);
}

static i64 parquet_dump_struct_open(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_STRUCT_START;
  iterator->tokens.items[iterator->tokens.count++].data = iterator->queue.items[index].ctx_args.value;

  return 0;
}

static i64 parquet_dump_struct_close(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct end
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_STRUCT_END;
  iterator->tokens.items[iterator->tokens.count++].data = iterator->queue.items[index].ctx_args.value;

  return 0;
}

static i64 parquet_dump_array_open(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_ARRAY_START;
  iterator->tokens.items[iterator->tokens.count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_array_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array end
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_ARRAY_END;
  iterator->tokens.items[iterator->tokens.count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_value_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // value end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_VALUE_END;

  // success
  return 0;
}

static i64 parquet_dump_index_open(struct parquet_metadata_iterator *iterator, u32 index) {
  u64 ctx_args;

  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // get args
  ctx_args = iterator->queue.items[index].ctx_args.value;

  // index start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_INDEX_START;
  iterator->tokens.items[iterator->tokens.count++].data = ctx_args;

  // success
  return 0;
}

static i64 parquet_dump_index_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // index end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_INDEX_END;

  // success
  return 0;
}

static i64 parquet_dump_field(struct parquet_metadata_iterator *iterator, u32 index) {
  i32 *ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // check for the capacity, we need 4 slots
  if (iterator->tokens.count > iterator->tokens.capacity - 4) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // check for the capacity, we need 2 slots
  if (iterator->queue.count > iterator->queue.capacity - 2) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // extract
  ctx = iterator->queue.items[index].ctx;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;
  item_fn = iterator->queue.items[index].item_fn;

  // key-start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_KEY_START;
  iterator->tokens.items[iterator->tokens.count].type = DOM_TYPE_TEXT;
  iterator->tokens.items[iterator->tokens.count++].data = (u64) "text";

  // key-content
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = ctx_args;
  iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;

  // key-end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_KEY_END;

  // value-start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_VALUE_START;
  iterator->tokens.items[iterator->tokens.count++].data = item_args;

  // value-close
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_value_close;

  // value-content
  iterator->queue.items[iterator->queue.count].ctx = ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = item_fn;

  // success
  return 0;
}

static i64 parquet_dump_index(struct parquet_metadata_iterator *iterator, u32 index) {
  void **ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // get the value
  ctx = (void **)iterator->queue.items[index].ctx;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;
  item_fn = iterator->queue.items[index].item_fn;

  // check for null-terminated
  if (*ctx == NULL) {
    return 0;
  }

  // check for the capacity, we need 4 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 4) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // next index
  iterator->queue.items[iterator->queue.count].ctx = ctx + 1;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_index;
  iterator->queue.items[iterator->queue.count].item_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].item_fn = item_fn;

  // close-index
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_index_close;

  // index content
  iterator->queue.items[iterator->queue.count].ctx = *ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = item_fn;

  // open-index
  iterator->queue.items[iterator->queue.count].ctx_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_index_open;

  // success
  return 0;
}

static i64 parquet_dump_array(struct parquet_metadata_iterator *iterator, u32 index) {
  void *ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // check for the capacity, we need 4 slots
  if (iterator->tokens.count > iterator->tokens.capacity - 4) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // check for the capacity, we need 4 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 4) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // copy values
  ctx = iterator->queue.items[index].ctx;
  item_fn = iterator->queue.items[index].item_fn;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;

  // key start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_KEY_START;
  iterator->tokens.items[iterator->tokens.count].type = DOM_TYPE_TEXT;
  iterator->tokens.items[iterator->tokens.count++].data = (u64) "text";

  // extract the name
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = ctx_args;
  iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_VALUE_START;
  iterator->tokens.items[iterator->tokens.count++].data = item_args;

  // close-value
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_value_close;

  // close-array
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_array_close;

  // open-index with the first item or a null-termination
  iterator->queue.items[iterator->queue.count].ctx = ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_index;
  iterator->queue.items[iterator->queue.count].item_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].item_fn = item_fn;

  // open-array
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_array_open;

  // success
  return 0;
}

static i64 parquet_dump_encoding_stats(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_page_encoding_stats *encoding_stats;

  // check for the capacity, we need 5 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 5) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  encoding_stats = (struct parquet_page_encoding_stats *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = encoding_stats;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding-stats";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // count
  if (encoding_stats->count != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->count;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "count";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // encoding
  if (encoding_stats->encoding != PARQUET_ENCODING_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->encoding;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding;
  }

  // page_type
  if (encoding_stats->page_type != PARQUET_PAGE_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->page_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "page_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_page_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = encoding_stats;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding-stats";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_column_meta(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_meta *column_meta;

  // check for the capacity, we need 13 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 13) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  column_meta = (struct parquet_column_meta *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = column_meta;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-meta";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // encoding_stats
  if (column_meta->encoding_stats != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->encoding_stats;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding_stats";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding_stats;
  }

  // dictionary_page_offset
  if (column_meta->dictionary_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->dictionary_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "dictionary_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // index_page_offset
  if (column_meta->index_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->index_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "index_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // data_page_offset
  if (column_meta->data_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->data_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_compressed_size
  if (column_meta->total_compressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->total_compressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_compressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_uncompressed_size
  if (column_meta->total_uncompressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->total_uncompressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_uncompressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // num_values
  if (column_meta->num_values != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->num_values;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_values";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // compression_codec
  if (column_meta->compression_codec != PARQUET_COMPRESSION_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->compression_codec;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "compression_codec";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_compression_codec;
  }

  // path_in_schema
  if (column_meta->path_in_schema != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->path_in_schema;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "path_in_schema";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "str";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // encodings
  if (column_meta->encodings != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->encodings;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encodings";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding;
  }

  // data_type
  if (column_meta->data_type != PARQUET_DATA_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->data_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_data_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = column_meta;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-meta";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_column_chunk(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_chunk *column_chunk;

  // check for the capacity, we need 5 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 5) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  column_chunk = (struct parquet_column_chunk *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = column_chunk;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-chunk";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // meta
  if (column_chunk->meta != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_chunk->meta;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "meta";
    iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_column_meta;
  }

  // file_path
  if (column_chunk->file_path != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_chunk->file_path;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_path";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // file_offset, not -1
  if (column_chunk->file_offset > 0) {
    iterator->queue.items[iterator->queue.count].ctx = &column_chunk->file_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = column_chunk;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-chunk";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_row_group(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_row_group *row_group;

  // check for the capacity, we need 7 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 7) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  row_group = (struct parquet_row_group *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = row_group;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "row_group";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // total_compressed_size
  if (row_group->total_compressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->total_compressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_compressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // file_offset, not -1
  if (row_group->file_offset > 0) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->file_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // num_rows
  if (row_group->num_rows != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->num_rows;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_rows";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_byte_size
  if (row_group->total_byte_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->total_byte_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_byte_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // columns
  if (row_group->columns != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = row_group->columns;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "columns";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_column_chunk;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = row_group;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "row_group";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  return 0;
}

static i64 parquet_dump_schema_element(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_schema_element *schema_element;

  // check for the capacity, we need 8 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 8) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  schema_element = (struct parquet_schema_element *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = schema_element;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "schema_element";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // converted_type
  if (schema_element->converted_type != PARQUET_CONVERTED_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->converted_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "converted_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_converted_type;
  }

  // num_children
  if (schema_element->num_children != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->num_children;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_children";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // name
  if (schema_element->name != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = schema_element->name;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "name";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // repetition_type
  if (schema_element->repetition_type != PARQUET_REPETITION_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->repetition_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "repetition_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_repetition_type;
  }

  // type_length
  if (schema_element->type_length != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->type_length;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "type_length";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // data_type
  if (schema_element->data_type != PARQUET_DATA_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->data_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_data_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = schema_element;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "schema_element";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_metadata(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_metadata *metadata;

  // check for the capacity, we need 7 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 7) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  metadata = (struct parquet_metadata *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = metadata;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // created_by
  if (metadata->created_by != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->created_by;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "created_by";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // row_groups
  if (metadata->row_groups != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->row_groups;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "row_groups";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_row_group;
  }

  // num_rows
  if (metadata->num_rows != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &metadata->num_rows;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_rows";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // schemas
  if (metadata->schemas != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->schemas;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "schemas";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_schema_element;
  }

  // version
  if (metadata->version != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &metadata->version;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "version";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = metadata;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static void parquet_metadata_iter(struct parquet_metadata_iterator *iterator, struct parquet_metadata *metadata) {
  iterator->metadata = metadata;

  iterator->tokens.count = 0;
  iterator->tokens.capacity = PARQUET_METADATA_TOKENS_SIZE;

  iterator->queue.count = 0;
  iterator->queue.capacity = PARQUET_METADATA_QUEUE_SIZE;

  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_metadata;
  iterator->queue.items[iterator->queue.count++].ctx = metadata;
}

static i64 parquet_metadata_next(struct parquet_metadata_iterator *iterator) {
  u32 index;
  i64 result;
  parquet_metadata_iterator_fn fn;

  // reset the tokens counter
  iterator->tokens.count = 0;

  // iterator over the LIFO stack
  while (iterator->queue.count > 0) {
    index = --iterator->queue.count;
    fn = iterator->queue.items[index].ctx_fn;

    // call next function
    result = fn(iterator, index);

    // handle buffer too small error
    if (result == PARQUET_ERROR_BUFFER_TOO_SMALL) {
      iterator->queue.count++;
      break;
    };

    // propagate unhandled errors
    if (result < 0) return result;
  }

  // success
  return 0;
}

#endif

#if defined(I13C_PARQUET)

i32 parquet_main(u32 argc, const char **argv) {
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

  // default
  result = 0;

  // check for required arguments
  if (argc < 2) goto cleanup;

  // initialize memory and parquet file
  malloc_init(&pool);
  parquet_init(&file, &pool);

  // try to open parquet file
  result = parquet_open(&file, argv[1]);
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
  if (result == 0) return 0;

  writef("Something wrong happened; error=%r\n", result);
  return result;
}

#endif

#if defined(I13C_TESTS)

static void can_iterate_through_metadata() {
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = 1;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = 43;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = "test_user";

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // iterate one batch
  result = parquet_metadata_next(&iterator);
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count > 0, "should have some tokens");
}

static void can_dump_enum_with_known_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_TEXT, "expected text");
  assert_eq_str((const char *)iterator.tokens.items[0].data, "INT96", "expected correct value");
}

static void can_dump_enum_with_unknown_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 27;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I32, "expected integer");
  assert((u64)iterator.tokens.items[0].data == 27, "expected correct value");
}

static void can_detect_buffer_too_small_with_enum() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity;

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_dump_literal_with_i32_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_I32);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I32, "expected i32");
  assert((i32)iterator.tokens.items[0].data == 3, "expected correct value");
}

static void can_dump_literal_with_i64_value() {
  i64 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_I64);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I64, "expected i64");
  assert((i64)iterator.tokens.items[0].data == 3, "expected correct value");
}

static void can_dump_literal_with_text_value() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = "abc";
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_TEXT);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_TEXT, "expected text");
  assert_eq_str((const char *)iterator.tokens.items[0].data, "abc", "expected correct value");
}

static void can_detect_buffer_too_small_with_literal() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = "abc";
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_TEXT);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_dump_index_with_null_terminator() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = PARQUET_NULL_VALUE;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 0, "should generate 0 tokens");
  assert(iterator.queue.count == 0, "should have 0 items in the queue");
}

static void can_dump_index_with_next_item() {
  i64 result;
  const char *value[3];

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value[0] = "abc";
  value[1] = "cde";
  value[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 0, "should generate 0 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_index_open, "expected index open");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_index_close, "expected index close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_index, "expected index");
  assert(iterator.queue.items[2].ctx_fn == iterator.queue.items[0].item_fn, "expected callback");

  assert(iterator.queue.items[2].ctx == value[0], "expected first item");
  assert(iterator.queue.items[0].ctx == &value[1], "expected second item");
}

static void can_detect_capacity_overflow_with_index() {
  i64 result;
  const char *value[3];

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value[0] = "abc";
  value[1] = "cde";
  value[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = iterator.queue.capacity - 3;
  iterator.queue.items[0].ctx = (void *)value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
  assert(iterator.tokens.count == 0, "should not generate any tokens");
  assert(iterator.queue.count == iterator.queue.capacity - 3, "should not change the queue");
}

static void can_dump_array_with_no_items() {
  i64 *array[1];
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_array_open, "expected array open");
  assert(iterator.queue.items[2].ctx_fn == parquet_dump_index, "expected array index");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_array_close, "expected array close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");

  assert(iterator.queue.items[2].ctx == array, "expected first item");
  assert((u64)iterator.queue.items[2].item_fn == 0x12345678, "expected callback");
  assert_eq_str(iterator.queue.items[2].item_args.name, "item-x", "expected correct name");
}

static void can_dump_array_with_two_items() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_array_open, "expected array open");
  assert(iterator.queue.items[2].ctx_fn == parquet_dump_index, "expected array index");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_array_close, "expected array close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");

  assert(iterator.queue.items[2].ctx == array, "expected first item");
  assert((u64)iterator.queue.items[2].item_fn == 0x12345678, "expected callback");
  assert_eq_str(iterator.queue.items[2].item_args.name, "item-x", "expected correct name");
}

static void can_detect_buffer_too_small_with_array() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity - 3;

  // process array
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_detect_capacity_overflow_with_array() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.queue.count = iterator.queue.capacity - 3;

  // process array
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
}

static void can_dump_field_with_type_name() {
  i64 result, value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 2, "should have 2 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");
  assert(iterator.queue.items[1].ctx_fn == (parquet_metadata_iterator_fn)0x12345678, "expected callback");

  assert(iterator.queue.items[1].ctx == &value, "expected value");
  assert_eq_str(iterator.queue.items[1].ctx_args.name, "item-x", "expected correct name");
}

static void can_dump_field_with_type_id() {
  i64 result, value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 2, "should have 2 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");
  assert(iterator.queue.items[1].ctx_fn == (parquet_metadata_iterator_fn)0x12345678, "expected callback");

  assert(iterator.queue.items[1].ctx == &value, "expected value");
  assert(iterator.queue.items[1].ctx_args.value == DOM_TYPE_U16, "expected correct name");
}

static void can_detect_buffer_too_small_with_field() {
  i64 result;
  u64 value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity - 3;

  // process field
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_detect_capacity_overflow_with_field() {
  i64 result;
  u64 value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.queue.count = iterator.queue.capacity - 1;

  // process field
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
}

void parquet_test_cases(struct runner_context *ctx) {
  // opening and closing cases
  test_case(ctx, "can open and close parquet file", can_open_and_close_parquet_file);
  test_case(ctx, "can detect non-existing parquet file", can_detect_non_existing_parquet_file);

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

  // dump cases
  test_case(ctx, "can iterate through metadata", can_iterate_through_metadata);
  test_case(ctx, "can dump enum with known value", can_dump_enum_with_known_value);
  test_case(ctx, "can dump enum with unknown value", can_dump_enum_with_unknown_value);
  test_case(ctx, "can detect buffer too small with enum", can_detect_buffer_too_small_with_enum);

  test_case(ctx, "can dump literal with i32 value", can_dump_literal_with_i32_value);
  test_case(ctx, "can dump literal with i64 value", can_dump_literal_with_i64_value);
  test_case(ctx, "can dump literal with text value", can_dump_literal_with_text_value);
  test_case(ctx, "can detect buffer to small with literal", can_detect_buffer_too_small_with_literal);

  test_case(ctx, "can dump index with null-terminator", can_dump_index_with_null_terminator);
  test_case(ctx, "can dump index with next item", can_dump_index_with_next_item);
  test_case(ctx, "can detect buffer too small with index", can_detect_capacity_overflow_with_index);

  test_case(ctx, "can dump array with no items", can_dump_array_with_no_items);
  test_case(ctx, "can dump array with two items", can_dump_array_with_two_items);
  test_case(ctx, "can detect buffer too small with array", can_detect_buffer_too_small_with_array);
  test_case(ctx, "can detect capacity overflow with array", can_detect_capacity_overflow_with_array);

  test_case(ctx, "can dump field with type name", can_dump_field_with_type_name);
  test_case(ctx, "can dumo field with type id", can_dump_field_with_type_id);
  test_case(ctx, "can detect buffer too small with field", can_detect_buffer_too_small_with_field);
  test_case(ctx, "can detect capacity overflow with field", can_detect_capacity_overflow_with_field);
}

#endif
