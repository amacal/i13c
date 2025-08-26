#include "parquet.h"
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
  void *target;              // pointer to the target structure to fill
  u64 target_size;           // size of the target structure
  parquet_read_fn target_fn; // function to read the target structure

  char *buffer;    // the buffer behind metadata storage
  u64 buffer_size; // the size of the buffer
  u64 buffer_tail; // the tail of the buffer

  void *ptrs[16];         // pointers to the fields in the target structure
  thrift_read_fn *fields; // array of thrift read functions for the fields
  u64 fields_size;        // number of fields in the target structure
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
  u32 offset;

  // check if there is enough space
  if (ctx->buffer_tail + size > ctx->buffer_size) {
    return PARQUET_ERROR_BUFFER_OVERFLOW;
  }

  // move tail aligned to 8 bytes
  offset = ctx->buffer_tail;
  ctx->buffer_tail = (offset + size + 7) & ~7;

  // allocated space
  return (i64)(ctx->buffer + offset);
}

static void parquet_metadata_release(struct parquet_parse_context *ctx, u64 size) {
  // move tail back aligned to 8 bytes
  ctx->buffer_tail -= (size + 7) & ~7;
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

  // copy binary content, it also includes null terminator EOS
  result = thrift_read_binary_content(value, size, buffer, buffer_size);
  if (result < 0) goto cleanup_buffer;

  // value is OK, field will be found
  *(char **)ctx->ptrs[field_id] = value;

  // successs
  return read + result;

cleanup_buffer:
  parquet_metadata_release(ctx, size + 1);

cleanup:
  return result;
}

static i64 parquet_read_list(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_parse_context context;
  struct thrift_list_header header;

  void **ptrs;
  i64 result, read;
  u32 index;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_LIST) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // read the size of the schemas list
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) goto cleanup;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // allocate memory for the pointers
  result = parquet_metadata_acquire(ctx, 8 + header.size * 8);
  if (result < 0) goto cleanup;

  // remember allocated array
  ptrs = (void **)result;

  // null-terminate the array
  ptrs[header.size] = NULL;

  // allocate memory for the array
  if (ctx->target_size > 0) {
    result = parquet_metadata_acquire(ctx, header.size * ctx->target_size);
    if (result < 0) goto cleanup_ptrs;

    // fill out the array of schema elements
    for (index = 0; index < header.size; index++) {
      ptrs[index] = (void *)(result);
      result += ctx->target_size;
    }
  }

  // initialize nested context
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

  // parse each schema element
  for (index = 0; index < header.size; index++) {
    // set the target for the nested context
    context.target = ctx->target_size > 0 ? ptrs[index] : &ptrs[index];

    // read the next schema element
    result = ctx->target_fn(&context, buffer, buffer_size);
    if (result < 0) goto cleanup_data;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // apply buffer tail
  ctx->buffer_tail = context.buffer_tail;

  // value is OK, field will be found
  *(void **)ctx->ptrs[field_id] = ptrs;

  // success
  return read;

cleanup_data:
  parquet_metadata_release(ctx, header.size * ctx->target_size);

cleanup_ptrs:
  parquet_metadata_release(ctx, 8 + header.size * 8);

cleanup:
  return result;
}

static i64 parquet_read_struct(
  struct parquet_parse_context *ctx, i16 field_id, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  struct parquet_parse_context context;
  i64 result, read;
  void *data;

  // check if the field type is correct
  if (field_type != THRIFT_TYPE_STRUCT) {
    return PARQUET_ERROR_INVALID_TYPE;
  }

  // allocate memory for the struct
  result = parquet_metadata_acquire(ctx, ctx->target_size);
  if (result < 0) goto cleanup;

  // remember allocated array
  data = (void *)result;

  // context
  context.target = data;
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

  // read the next schema element
  result = ctx->target_fn(&context, buffer, buffer_size);
  if (result < 0) goto cleanup_data;

  // move the buffer pointer and size
  read = result;
  buffer += result;
  buffer_size -= result;

  // restore the context
  ctx->buffer_tail = context.buffer_tail;

  // value is OK, field will be found
  *(void **)ctx->ptrs[field_id] = data;

  // success
  return read;

cleanup_data:
  parquet_metadata_release(ctx, ctx->target_size);

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
  fields[2] = (thrift_read_fn)parquet_read_i32_positive; // type_length
  fields[3] = (thrift_read_fn)parquet_read_i32_positive; // repetition_type
  fields[4] = (thrift_read_fn)parquet_read_string;       // schema_name
  fields[5] = (thrift_read_fn)parquet_read_i32_positive; // num_children
  fields[6] = (thrift_read_fn)parquet_read_i32_positive; // converted_type

  // schema
  schema = (struct parquet_schema_element *)ctx->target;
  schema->data_type = PARQUET_DATA_TYPE_NONE;
  schema->type_length = -1;
  schema->repetition_type = PARQUET_REPETITION_TYPE_NONE;
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

  // restore the context
  ctx->buffer_tail = context.buffer_tail;

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
  ctx->target_size = sizeof(char **);
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

static i64 parquet_parse_column_meta_element(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct parquet_column_meta *meta;
  struct parquet_parse_context context;

  const u32 FIELDS_SLOTS = 11;
  thrift_read_fn fields[FIELDS_SLOTS];

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)parquet_read_i32_positive;      // data_type
  fields[2] = (thrift_read_fn)parquet_read_list_i32_positive; // encodings
  fields[3] = (thrift_read_fn)parquet_read_list_string;       // path_in_schema
  fields[4] = (thrift_read_fn)parquet_read_i32_positive;      // compression_codec
  fields[5] = (thrift_read_fn)parquet_read_i64_positive;      // num_values
  fields[6] = (thrift_read_fn)parquet_read_i64_positive;      // total_uncompressed_size
  fields[7] = (thrift_read_fn)parquet_read_i64_positive;      // total_compressed_size
  fields[8] = (thrift_read_fn)parquet_read_i64_positive;      // data_page_offset
  fields[9] = (thrift_read_fn)parquet_read_i64_positive;      // index_page_offset
  fields[10] = (thrift_read_fn)parquet_read_i64_positive;     // dictionary_page_offset

  // meta
  meta = (struct parquet_column_meta *)ctx->target;
  meta->data_type = PARQUET_DATA_TYPE_NONE;
  meta->encodings = NULL;
  meta->path_in_schema = NULL;
  meta->compression_codec = PARQUET_COMPRESSION_NONE;
  meta->num_values = -1;
  meta->total_uncompressed_size = -1;
  meta->total_compressed_size = -1;
  meta->data_page_offset = -1;
  meta->index_page_offset = -1;
  meta->dictionary_page_offset = -1;
  meta->statistics = NULL;
  meta->encoding_stats = NULL;

  // context
  context.target = meta;
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

  // targets
  context.ptrs[1] = &meta->data_type;
  context.ptrs[2] = &meta->encodings;
  context.ptrs[3] = &meta->path_in_schema;
  context.ptrs[4] = &meta->compression_codec;
  context.ptrs[5] = &meta->num_values;
  context.ptrs[6] = &meta->total_uncompressed_size;
  context.ptrs[7] = &meta->total_compressed_size;
  context.ptrs[8] = &meta->data_page_offset;
  context.ptrs[9] = &meta->index_page_offset;
  context.ptrs[10] = &meta->dictionary_page_offset;

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
  column_chunk->file_path = NULL;
  column_chunk->file_offset = -1;
  column_chunk->meta = NULL;

  // context
  context.target = column_chunk;
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

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

  // restore the context
  ctx->buffer_tail = context.buffer_tail;

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
  fields[4] = (thrift_read_fn)parquet_read_string;         // sorting_columns
  fields[5] = (thrift_read_fn)parquet_read_i64_positive;   // file_offset
  fields[6] = (thrift_read_fn)parquet_read_i64_positive;   // total_compressed_size
  fields[7] = (thrift_read_fn)thrift_ignore_field;         // ordinal

  // row_group
  row_group = (struct parquet_row_group *)ctx->target;
  row_group->columns = NULL;
  row_group->total_byte_size = -1;
  row_group->num_rows = -1;
  row_group->file_offset = -1;
  row_group->total_compressed_size = -1;

  // context
  context.target = row_group;
  context.buffer = ctx->buffer;
  context.buffer_size = ctx->buffer_size;
  context.buffer_tail = ctx->buffer_tail;

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

  // restore the context
  ctx->buffer_tail = context.buffer_tail;

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

  // targets
  ctx->ptrs[1] = (void *)&metadata->version;
  ctx->ptrs[2] = (void *)&metadata->schemas;
  ctx->ptrs[3] = (void *)&metadata->num_rows;
  ctx->ptrs[4] = (void *)&metadata->row_groups;
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
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value != NULL, "should allocate value");
  assert((u64)value % 8 == 0, "should be aligned to 8 bytes");
  assert_eq_str(value, "i13c", "should read value 'i13c'");

  assert(ctx.buffer_tail == 8, "should update buffer tail to 8");
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
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
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

static i64 can_read_list_item(struct parquet_parse_context *ctx, const char *buffer, u64 buffer_size) {
  // hide the target pointer behind 0
  ctx->ptrs[0] = ctx->target;

  // delegate reading to known function
  return parquet_read_i64_positive(ctx, 0, THRIFT_TYPE_I64, buffer, buffer_size);
}

static void can_read_list() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // context
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
  assert(ctx.buffer_tail == 40 + 32, "should update buffer tail to 72");
}

static void can_detect_list_invalid_type() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_detect_list_buffer_overflow_01() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 8;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_detect_list_buffer_overflow_02() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 56;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer) - 1);

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_list_buffer_overflow_01() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_list_buffer_overflow_02() {
  struct parquet_parse_context ctx;
  i64 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer) - 1);

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static i64 can_read_struct_item(struct parquet_parse_context *, const char *, u64 buffer_size) {
  return buffer_size ? 1 : THRIFT_ERROR_BUFFER_OVERFLOW;
}

static void can_read_struct() {
  struct sample {
    i32 len;
    char **name;
  };

  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // context
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value != NULL, "should allocate value");
  assert(ctx.buffer_tail == 8, "should update buffer tail to 8");
}

static void can_detect_struct_invalid_type() {
  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // context
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_detect_struct_buffer_overflow() {
  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x01};

  // defaults
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 6;
  ctx.buffer_tail = 0;

  // context
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_struct_buffer_overflow() {
  struct parquet_parse_context ctx;
  struct sample *value;

  i64 result;
  u64 output[32];
  const char buffer[] = {};

  // defaults
  value = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // context
  ctx.ptrs[0] = &value;
  ctx.target_size = sizeof(value);
  ctx.target_fn = (parquet_read_fn)can_read_struct_item;

  // read the value from the buffer
  result = parquet_read_struct(&ctx, 0, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(value == NULL, "should not allocate value");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_read_list_strings() {
  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

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
  assert(ctx.buffer_tail == 24 + 8 + 8, "should update buffer tail to 40");
}

static void can_detect_list_strings_invalid_type() {
  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_detect_list_strings_buffer_overflow() {
  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3', 'c'};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 8;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_list_strings_buffer_overflow() {
  struct parquet_parse_context ctx;
  char **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x28, 0x03, 'a', 'b', 'c', 0x04, 'i', '1', '3'};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_string(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_read_list_i32_positive() {
  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 6, "should read six bytes");
  assert(values != NULL, "should allocate values");
  assert((u64)values % 8 == 0, "container should be aligned to 8 bytes");

  assert((u64)values[0] % 8 == 0, "entry should be aligned to 8 bytes");
  assert(*values[0] == 1, "should read value 1");

  assert((u64)values[1] % 8 == 0, "entry should be aligned to 8 bytes");
  assert(*values[1] == 2, "should read value 2");

  assert((u64)values[2] % 8 == 0, "entry should be aligned to 8 bytes");
  assert(*values[2] == 3, "should read value 3");

  assert((u64)values[3] % 8 == 0, "entry should be aligned to 8 bytes");
  assert(*values[3] == 1337, "should read value 1337");

  assert(values[4] == NULL, "should be null-terminated");
  assert(ctx.buffer_tail == 40 + 32, "should update buffer tail to 72");
}

static void can_detect_list_i32_positive_invalid_type() {
  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_INVALID_TYPE, "should fail with PARQUET_ERROR_INVALID_TYPE");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_detect_list_i32_positive_buffer_overflow() {
  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2, 0x14};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 64;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == PARQUET_ERROR_BUFFER_OVERFLOW, "should fail with PARQUET_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

static void can_propagate_list_i32_positive_buffer_overflow() {
  struct parquet_parse_context ctx;
  i32 **values;

  i64 result;
  u64 output[32];
  const char buffer[] = {0x46, 0x02, 0x04, 0x06, 0xf2};

  // defaults
  ctx.ptrs[1] = &values;
  values = NULL;

  // buffer
  ctx.buffer = (char *)output;
  ctx.buffer_size = 256;
  ctx.buffer_tail = 0;

  // read the value from the buffer
  result = parquet_read_list_i32_positive(&ctx, 1, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
  assert(values == NULL, "should not allocate values");
  assert(ctx.buffer_tail == 0, "should not change buffer tail");
}

#endif

#if defined(I13C_PARQUET) || defined(I13C_TESTS)

#define PARQUET_METADATA_TOKENS_SIZE 32
#define PARQUET_METADATA_ITERATOR_SIZE 32

struct parquet_metadata_iterator;

typedef void *parquet_metadata_iterator_ctx;
typedef const char *parquet_metadata_iterator_name;
typedef i64 (*parquet_metadata_iterator_fn)(struct parquet_metadata_iterator *iterator, u32 index);

struct parquet_metadata_iterator {
  u32 tokens_count;
  u32 context_count;

  struct parquet_metadata *metadata;
  struct dom_token tokens[PARQUET_METADATA_TOKENS_SIZE];

  parquet_metadata_iterator_fn fns[PARQUET_METADATA_ITERATOR_SIZE];
  parquet_metadata_iterator_ctx ctxs[PARQUET_METADATA_ITERATOR_SIZE];
  parquet_metadata_iterator_name names[PARQUET_METADATA_ITERATOR_SIZE];
  parquet_metadata_iterator_fn items[PARQUET_METADATA_ITERATOR_SIZE];
};

static const char *PARQUET_COMPRESSION_NAMES[PARQUET_COMPRESSION_SIZE] = {
  [PARQUET_COMPRESSION_UNCOMPRESSED] = "UNCOMPRESSED",
  [PARQUET_COMPRESSION_SNAPPY] = "SNAPPY",
  [PARQUET_COMPRESSION_GZIP] = "GZIP",
  [PARQUET_COMPRESSION_LZO] = "LZO",
  [PARQUET_COMPRESSION_BROTLI] = "BROTLI",
  [PARQUET_COMPRESSION_LZ4] = "LZ4",
  [PARQUET_COMPRESSION_ZSTD] = "ZSTD",
  [PARQUET_COMPRESSION_LZ4_RAW] = "LZ4_RAW",
};

static const char *PARQUET_CONVERTED_TYPE_NAMES[PARQUET_CONVERTED_TYPE_SIZE] = {
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

static const char *PARQUET_DATA_TYPE_NAMES[PARQUET_DATA_TYPE_SIZE] = {
  [PARQUET_DATA_TYPE_BOOLEAN] = "BOOLEAN",
  [PARQUET_DATA_TYPE_INT32] = "INT32",
  [PARQUET_DATA_TYPE_INT64] = "INT64",
  [PARQUET_DATA_TYPE_INT96] = "INT96",
  [PARQUET_DATA_TYPE_FLOAT] = "FLOAT",
  [PARQUET_DATA_TYPE_DOUBLE] = "DOUBLE",
  [PARQUET_DATA_TYPE_BYTE_ARRAY] = "BYTE_ARRAY",
  [PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED] = "PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED",
};

static const char *PARQUET_ENCODING_NAMES[PARQUET_ENCODING_SIZE] = {
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

static const char *PARQUET_REPETITION_TYPE_NAMES[PARQUET_REPETITION_TYPE_SIZE] = {
  [PARQUET_REPETITION_TYPE_REQUIRED] = "REQUIRED",
  [PARQUET_REPETITION_TYPE_OPTIONAL] = "OPTIONAL",
  [PARQUET_REPETITION_TYPE_REPEATED] = "REPEATED",
};

static i64 parquet_dump_enum(struct parquet_metadata_iterator *iterator, u32 index, i32 size, const char **names) {
  i32 *value;
  const char *name;

  // check for available space, we need 6 tokens
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // key start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_KEY_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_TEXT;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the name
  name = (const char *)iterator->names[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)name;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_VALUE_START;
  iterator->tokens[iterator->tokens_count++].data = (u64) "i32";

  // extract the value and the name
  value = (i32 *)iterator->ctxs[index];
  name = NULL;

  // find mapping if available
  if (*value < size) {
    name = names[*value];
  }

  // output either raw i32 or mapped name
  if (name == NULL) {
    iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
    iterator->tokens[iterator->tokens_count].data = (u64)*value;
    iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_I32;
  } else {
    iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
    iterator->tokens[iterator->tokens_count].data = (u64)name;
    iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;
  }

  // value end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_VALUE_END;

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

static i64 parquet_dump_repetition_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_REPETITION_TYPE_SIZE, PARQUET_REPETITION_TYPE_NAMES);
}

static i64 parquet_dump_i32(struct parquet_metadata_iterator *iterator, u32 index) {
  i32 *value;
  const char *name;

  // check for available space, we need 6 tokens
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // key start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_KEY_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_TEXT;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the name
  name = (const char *)iterator->names[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)name;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_VALUE_START;
  iterator->tokens[iterator->tokens_count++].data = (u64) "i32";

  // extract the value
  value = (i32 *)iterator->ctxs[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)*value;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_I32;

  // value end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_VALUE_END;

  // success
  return 0;
}

static i64 parquet_dump_i64(struct parquet_metadata_iterator *iterator, u32 index) {
  i64 *value;
  const char *name;

  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // key start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_KEY_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_TEXT;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the name
  name = (const char *)iterator->names[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)name;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_VALUE_START;
  iterator->tokens[iterator->tokens_count++].data = (u64) "i64";

  // extract the value
  value = (i64 *)iterator->ctxs[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)*value;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_I64;

  // value end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_VALUE_END;

  return 0;
}

static i64 parquet_dump_text(struct parquet_metadata_iterator *iterator, u32 index) {
  const char *text;
  const char *name;

  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // key start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_KEY_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_TEXT;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the name
  name = (const char *)iterator->names[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)name;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_VALUE_START;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the value
  text = (const char *)iterator->ctxs[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)text;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // value end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_VALUE_END;

  // success
  return 0;
}

static i64 parquet_dump_struct_open(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_STRUCT_START;
  iterator->tokens[iterator->tokens_count++].data = (u64)iterator->names[index];

  return 0;
}

static i64 parquet_dump_struct_close(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct end
  iterator->tokens[iterator->tokens_count].op = DOM_OP_STRUCT_END;
  iterator->tokens[iterator->tokens_count++].data = (u64)iterator->names[index];

  return 0;
}

static i64 parquet_dump_array_open(struct parquet_metadata_iterator *iterator, u32) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_ARRAY_START;
  iterator->tokens[iterator->tokens_count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_array_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array end
  iterator->tokens[iterator->tokens_count].op = DOM_OP_ARRAY_END;
  iterator->tokens[iterator->tokens_count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_value_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // value end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_VALUE_END;

  // success
  return 0;
}

static i64 parquet_dump_index_open(struct parquet_metadata_iterator *iterator, u32 index) {
  const char *name;

  // get the name
  name = (const char *)iterator->names[index];

  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // index start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_INDEX_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_STRUCT;
  iterator->tokens[iterator->tokens_count++].data = (u64)name;

  // success
  return 0;
}

static i64 parquet_dump_index_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // index end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_INDEX_END;

  // success
  return 0;
}

static i64 parquet_dump_index(struct parquet_metadata_iterator *iterator, u32 index) {
  void **value;
  const char *name;

  // get the value
  value = iterator->ctxs[index];
  name = (const char *)iterator->names[index];

  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // check for null-terminated
  if (*value == NULL) {
    return 0;
  }

  // next index
  iterator->fns[iterator->context_count] = iterator->fns[index];
  iterator->items[iterator->context_count] = iterator->items[index];
  iterator->ctxs[iterator->context_count] = value + 1;
  iterator->names[iterator->context_count++] = name;

  // close-index
  iterator->fns[iterator->context_count++] = parquet_dump_index_close;

  // index content
  iterator->fns[iterator->context_count] = iterator->items[index];
  iterator->ctxs[iterator->context_count] = *value;
  iterator->names[iterator->context_count++] = name;

  // open-index
  iterator->fns[iterator->context_count] = parquet_dump_index_open;
  iterator->names[iterator->context_count++] = name;

  // success
  return 0;
}

static i64 parquet_dump_array(struct parquet_metadata_iterator *iterator, u32 index) {
  void *value;
  const char *name;
  parquet_metadata_iterator_fn item;

  // get the value and item
  value = iterator->ctxs[index];
  item = iterator->items[index];

  // check for available space
  if (iterator->tokens_count >= PARQUET_METADATA_TOKENS_SIZE - 6) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // key start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_KEY_START;
  iterator->tokens[iterator->tokens_count].type = DOM_TYPE_TEXT;
  iterator->tokens[iterator->tokens_count++].data = (u64) "text";

  // extract the name
  name = (const char *)iterator->names[index];
  iterator->tokens[iterator->tokens_count].op = DOM_OP_LITERAL;
  iterator->tokens[iterator->tokens_count].data = (u64)name;
  iterator->tokens[iterator->tokens_count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens[iterator->tokens_count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens[iterator->tokens_count].op = DOM_OP_VALUE_START;
  iterator->tokens[iterator->tokens_count++].data = (u64) "abc";

  // close-value
  iterator->fns[iterator->context_count++] = parquet_dump_value_close;

  // close-array
  iterator->fns[iterator->context_count++] = parquet_dump_array_close;

  // open-index with the first item or a null-termination
  iterator->fns[iterator->context_count] = parquet_dump_index;
  iterator->ctxs[iterator->context_count] = value;
  iterator->names[iterator->context_count] = "cde";
  iterator->items[iterator->context_count++] = item;

  // open-array
  iterator->fns[iterator->context_count++] = parquet_dump_array_open;

  // success
  return 0;
}

static i64 parquet_dump_column_meta(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_meta *column_meta;

  // get the value
  column_meta = (struct parquet_column_meta *)iterator->ctxs[index];

  // close-struct
  iterator->names[iterator->context_count] = "column-meta";
  iterator->fns[iterator->context_count] = parquet_dump_struct_close;
  iterator->ctxs[iterator->context_count++] = column_meta;

  // dictionary_page_offset
  if (column_meta->dictionary_page_offset > -1) {
    iterator->names[iterator->context_count] = "dictionary_page_offset";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->dictionary_page_offset;
  }

  // index_page_offset
  if (column_meta->index_page_offset > -1) {
    iterator->names[iterator->context_count] = "index_page_offset";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->index_page_offset;
  }

  // data_page_offset
  if (column_meta->data_page_offset > -1) {
    iterator->names[iterator->context_count] = "data_page_offset";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->data_page_offset;
  }

  // total_compressed_size
  if (column_meta->total_compressed_size > -1) {
    iterator->names[iterator->context_count] = "total_compressed_size";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->total_compressed_size;
  }

  // total_uncompressed_size
  if (column_meta->total_uncompressed_size > -1) {
    iterator->names[iterator->context_count] = "total_uncompressed_size";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->total_uncompressed_size;
  }

  // num_values
  if (column_meta->num_values > -1) {
    iterator->names[iterator->context_count] = "num_values";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_meta->num_values;
  }

  // compression_codec
  if (column_meta->compression_codec > PARQUET_COMPRESSION_NONE) {
    iterator->names[iterator->context_count] = "compression_codec";
    iterator->fns[iterator->context_count] = parquet_dump_compression_codec;
    iterator->ctxs[iterator->context_count++] = &column_meta->compression_codec;
  }

  // path_in_schema
  if (column_meta->path_in_schema) {
    iterator->names[iterator->context_count] = "path_in_schema";
    iterator->fns[iterator->context_count] = parquet_dump_array;
    iterator->items[iterator->context_count] = parquet_dump_text;
    iterator->ctxs[iterator->context_count++] = column_meta->path_in_schema;
  }

  // encodings
  if (column_meta->encodings) {
    iterator->names[iterator->context_count] = "encodings";
    iterator->fns[iterator->context_count] = parquet_dump_array;
    iterator->items[iterator->context_count] = parquet_dump_encoding;
    iterator->ctxs[iterator->context_count++] = column_meta->encodings;
  }

  // data_type
  if (column_meta->data_type > PARQUET_DATA_TYPE_NONE) {
    iterator->names[iterator->context_count] = "data_type";
    iterator->fns[iterator->context_count] = parquet_dump_data_type;
    iterator->ctxs[iterator->context_count++] = &column_meta->data_type;
  }

  // open-struct
  iterator->names[iterator->context_count] = "column-meta";
  iterator->fns[iterator->context_count] = parquet_dump_struct_open;
  iterator->ctxs[iterator->context_count++] = column_meta;

  // success
  return 0;
}

static i64 parquet_dump_column_chunk(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_chunk *column_chunk;

  // get the value
  column_chunk = (struct parquet_column_chunk *)iterator->ctxs[index];

  // close-struct
  iterator->names[iterator->context_count] = "column-chunk";
  iterator->fns[iterator->context_count] = parquet_dump_struct_close;
  iterator->ctxs[iterator->context_count++] = column_chunk;

  // meta
  if (column_chunk->meta) {
    iterator->names[iterator->context_count] = "meta";
    iterator->fns[iterator->context_count] = parquet_dump_column_meta;
    iterator->ctxs[iterator->context_count++] = column_chunk->meta;
  }

  // file_path
  if (column_chunk->file_path) {
    iterator->names[iterator->context_count] = "file_path";
    iterator->fns[iterator->context_count] = parquet_dump_text;
    iterator->ctxs[iterator->context_count++] = column_chunk->file_path;
  }

  // file_offset, not -1
  if (column_chunk->file_offset > 0) {
    iterator->names[iterator->context_count] = "file_offset";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &column_chunk->file_offset;
  }

  // open-struct
  iterator->names[iterator->context_count] = "column-chunk";
  iterator->fns[iterator->context_count] = parquet_dump_struct_open;
  iterator->ctxs[iterator->context_count++] = column_chunk;

  // success
  return 0;
}

static i64 parquet_dump_row_group(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_row_group *row_group;

  // get the value
  row_group = (struct parquet_row_group *)iterator->ctxs[index];

  // close-struct
  iterator->names[iterator->context_count] = "row_group";
  iterator->fns[iterator->context_count] = parquet_dump_struct_close;
  iterator->ctxs[iterator->context_count++] = row_group;

  // total_compressed_size
  if (row_group->total_compressed_size > -1) {
    iterator->names[iterator->context_count] = "total_compressed_size";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &row_group->total_compressed_size;
  }

  // file_offset, not -1
  if (row_group->file_offset > 0) {
    iterator->names[iterator->context_count] = "file_offset";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &row_group->file_offset;
  }

  // num_rows
  if (row_group->num_rows > -1) {
    iterator->names[iterator->context_count] = "num_rows";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &row_group->num_rows;
  }

  // total_byte_size
  if (row_group->total_byte_size > -1) {
    iterator->names[iterator->context_count] = "total_byte_size";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &row_group->total_byte_size;
  }

  // columns
  if (row_group->columns) {
    iterator->names[iterator->context_count] = "columns";
    iterator->fns[iterator->context_count] = parquet_dump_array;
    iterator->items[iterator->context_count] = parquet_dump_column_chunk;
    iterator->ctxs[iterator->context_count++] = row_group->columns;
  }

  // open-struct
  iterator->names[iterator->context_count] = "row_group";
  iterator->fns[iterator->context_count] = parquet_dump_struct_open;
  iterator->ctxs[iterator->context_count++] = row_group;

  return 0;
}

static i64 parquet_dump_schema_element(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_schema_element *schema_element;

  // get the value
  schema_element = (struct parquet_schema_element *)iterator->ctxs[index];

  // close-struct
  iterator->names[iterator->context_count] = "schema_element";
  iterator->fns[iterator->context_count] = parquet_dump_struct_close;
  iterator->ctxs[iterator->context_count++] = schema_element;

  // converted_type
  if (schema_element->converted_type > PARQUET_CONVERTED_TYPE_NONE) {
    iterator->names[iterator->context_count] = "converted_type";
    iterator->fns[iterator->context_count] = parquet_dump_converted_type;
    iterator->ctxs[iterator->context_count++] = &schema_element->converted_type;
  }

  // num_children
  if (schema_element->num_children > -1) {
    iterator->names[iterator->context_count] = "num_children";
    iterator->fns[iterator->context_count] = parquet_dump_i32;
    iterator->ctxs[iterator->context_count++] = &schema_element->num_children;
  }

  // name
  if (schema_element->name) {
    iterator->names[iterator->context_count] = "name";
    iterator->fns[iterator->context_count] = parquet_dump_text;
    iterator->ctxs[iterator->context_count++] = schema_element->name;
  }

  // repetition_type
  if (schema_element->repetition_type > PARQUET_REPETITION_TYPE_NONE) {
    iterator->names[iterator->context_count] = "repetition_type";
    iterator->fns[iterator->context_count] = parquet_dump_repetition_type;
    iterator->ctxs[iterator->context_count++] = &schema_element->repetition_type;
  }

  // type_length
  if (schema_element->type_length > -1) {
    iterator->names[iterator->context_count] = "type_length";
    iterator->fns[iterator->context_count] = parquet_dump_i32;
    iterator->ctxs[iterator->context_count++] = &schema_element->type_length;
  }

  // data_type
  if (schema_element->data_type > PARQUET_DATA_TYPE_NONE) {
    iterator->names[iterator->context_count] = "data_type";
    iterator->fns[iterator->context_count] = parquet_dump_data_type;
    iterator->ctxs[iterator->context_count++] = &schema_element->data_type;
  }

  // open-struct
  iterator->names[iterator->context_count] = "schema_element";
  iterator->fns[iterator->context_count] = parquet_dump_struct_open;
  iterator->ctxs[iterator->context_count++] = schema_element;

  return 0;
}

static i64 parquet_dump_metadata(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_metadata *metadata;

  // get the value
  metadata = (struct parquet_metadata *)iterator->ctxs[index];

  // close-struct
  iterator->names[iterator->context_count] = "metadata";
  iterator->fns[iterator->context_count] = parquet_dump_struct_close;
  iterator->ctxs[iterator->context_count++] = metadata;

  // created-by
  if (metadata->created_by) {
    iterator->names[iterator->context_count] = "created_by";
    iterator->fns[iterator->context_count] = parquet_dump_text;
    iterator->ctxs[iterator->context_count++] = metadata->created_by;
  }

  // row_groups
  if (metadata->row_groups) {
    iterator->names[iterator->context_count] = "row_groups";
    iterator->fns[iterator->context_count] = parquet_dump_array;
    iterator->items[iterator->context_count] = parquet_dump_row_group;
    iterator->ctxs[iterator->context_count++] = metadata->row_groups;
  }

  // num_rows
  if (metadata->num_rows > -1) {
    iterator->names[iterator->context_count] = "num_rows";
    iterator->fns[iterator->context_count] = parquet_dump_i64;
    iterator->ctxs[iterator->context_count++] = &metadata->num_rows;
  }

  // schemas
  if (metadata->schemas) {
    iterator->names[iterator->context_count] = "schemas";
    iterator->fns[iterator->context_count] = parquet_dump_array;
    iterator->items[iterator->context_count] = parquet_dump_schema_element;
    iterator->ctxs[iterator->context_count++] = metadata->schemas;
  }

  // version
  if (metadata->version > -1) {
    iterator->names[iterator->context_count] = "version";
    iterator->fns[iterator->context_count] = parquet_dump_i32;
    iterator->ctxs[iterator->context_count++] = &metadata->version;
  }

  // open-struct
  iterator->names[iterator->context_count] = "metadata";
  iterator->fns[iterator->context_count] = parquet_dump_struct_open;
  iterator->ctxs[iterator->context_count++] = metadata;

  return 0;
}

static void parquet_metadata_iter(struct parquet_metadata_iterator *iterator, struct parquet_metadata *metadata) {
  iterator->tokens_count = 0;
  iterator->context_count = 0;
  iterator->metadata = metadata;

  iterator->names[iterator->context_count] = "metadata";
  iterator->fns[iterator->context_count] = parquet_dump_metadata;
  iterator->ctxs[iterator->context_count++] = metadata;
}

static i64 parquet_metadata_next(struct parquet_metadata_iterator *iterator) {
  u32 index;
  i64 result;
  parquet_metadata_iterator_fn fn;

  // reset the tokens counter
  iterator->tokens_count = 0;

  // iterator over the LIFO stack
  while (iterator->context_count > 0) {
    index = --iterator->context_count;
    fn = iterator->fns[index];

    // call next function
    result = fn(iterator, index);

    // handle buffer too small error
    if (result == PARQUET_ERROR_BUFFER_TOO_SMALL) {
      iterator->context_count++;
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

i32 parquet_main() {
  i64 result;
  u32 tokens;

  struct parquet_file file;
  struct malloc_pool pool;
  struct malloc_lease output;
  struct dom_state dom;
  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  malloc_init(&pool);
  parquet_init(&file, &pool);

  result = parquet_open(&file, "data/test01.parquet");
  if (result < 0) return result;

  result = parquet_parse(&file, &metadata);
  if (result < 0) return result;

  output.size = 4096;
  result = malloc_acquire(&pool, &output);
  if (result < 0) return result;

  dom_init(&dom, &output);
  parquet_metadata_iter(&iterator, &metadata);

  do {
    result = parquet_metadata_next(&iterator);
    if (result < 0) return result;

    tokens = iterator.tokens_count;
    result = dom_write(&dom, iterator.tokens, &tokens);
    if (result < 0) return result;

    stdout_flush(&dom.format);
  } while (iterator.tokens_count > 0);

  // success
  return 0;
}

#endif

#if defined(I13C_TESTS)

static void can_iterate_through_metadata() {
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = 1;
  metadata.schemas = NULL;
  metadata.num_rows = 43;
  metadata.row_groups = NULL;
  metadata.created_by = "test_user";

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // iterate one batch
  result = parquet_metadata_next(&iterator);
  assert(result == 0, "should succeed");
  assert(iterator.tokens_count > 0, "should have some tokens");
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
  test_case(ctx, "can detect string buffer overflow", can_detect_string_buffer_overflow);
  test_case(ctx, "can propagate string buffer overflow 1", can_propagate_string_buffer_overflow_01);
  test_case(ctx, "can propagate string buffer overflow 2", can_propagate_string_buffer_overflow_02);

  // list cases
  test_case(ctx, "can read list", can_read_list);
  test_case(ctx, "can detect list invalid type", can_detect_list_invalid_type);
  test_case(ctx, "can detect list buffer overflow 1", can_detect_list_buffer_overflow_01);
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
}

#endif
