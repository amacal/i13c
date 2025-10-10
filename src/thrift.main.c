#include "malloc.h"
#include "stderr.h"
#include "stdin.h"
#include "stdout.h"
#include "thrift.base.h"
#include "typing.h"

#if defined(I13C_THRIFT) || defined(I13C_TESTS)

struct thrift_dump_context {
  u32 indent; // current indentation level
};

/// @brief Thrift dump field function type.
/// @param ctx The dump context.
/// @param buffer The buffer containing the field data.
/// @param buffer_size The size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_dump_field_fn)(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);

// forward declarations
static i64 thrift_dump_bool_true(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_bool_false(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_bool(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_i8(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_i16(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_i32(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_i64(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_binary(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_struct(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);
static i64 thrift_dump_list(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);

static const thrift_dump_field_fn THRIFT_DUMP_STRUCT_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_dump_bool_true,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_dump_bool_false,
  [THRIFT_TYPE_I8] = thrift_dump_i8,
  [THRIFT_TYPE_I16] = thrift_dump_i16,
  [THRIFT_TYPE_I32] = thrift_dump_i32,
  [THRIFT_TYPE_I64] = thrift_dump_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_dump_binary,
  [THRIFT_TYPE_LIST] = thrift_dump_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_dump_struct,
  [THRIFT_TYPE_UUID] = NULL,
};

static const thrift_dump_field_fn THRIFT_DUMP_LIST_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_dump_bool,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_dump_bool,
  [THRIFT_TYPE_I8] = thrift_dump_i8,
  [THRIFT_TYPE_I16] = thrift_dump_i16,
  [THRIFT_TYPE_I32] = thrift_dump_i32,
  [THRIFT_TYPE_I64] = thrift_dump_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_dump_binary,
  [THRIFT_TYPE_LIST] = thrift_dump_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_dump_struct,
  [THRIFT_TYPE_UUID] = NULL,
};

static const char *const THRIFT_TYPE_TO_NAME[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = "stop",     [THRIFT_TYPE_BOOL_TRUE] = "bool", [THRIFT_TYPE_BOOL_FALSE] = "bool",
  [THRIFT_TYPE_I8] = "i8",         [THRIFT_TYPE_I16] = "i16",        [THRIFT_TYPE_I32] = "i32",
  [THRIFT_TYPE_I64] = "i64",       [THRIFT_TYPE_DOUBLE] = "double",  [THRIFT_TYPE_BINARY] = "binary",
  [THRIFT_TYPE_LIST] = "list",     [THRIFT_TYPE_SET] = "set",        [THRIFT_TYPE_MAP] = "map",
  [THRIFT_TYPE_STRUCT] = "struct", [THRIFT_TYPE_UUID] = "uuid",
};

static i64 thrift_dump_bool_true(struct thrift_dump_context *, const char *, u64) {
  writef(", value=true");
  return 0;
}

static i64 thrift_dump_bool_false(struct thrift_dump_context *, const char *, u64) {
  writef(", value=false");
  return 0;
}

static i64 thrift_dump_bool(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  bool value;
  i64 result;

  // read the bool value from the buffer
  result = thrift_read_bool(&value, buffer, buffer_size);
  if (result < 0) return result;

  // print the value
  writef(", value=%s", value ? "true" : "false");

  // success
  return result;
}

static i64 thrift_dump_i8(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  i8 value;
  i64 result;

  // read the i8 value from the buffer
  result = thrift_read_i8(&value, buffer, buffer_size);
  if (result < 0) return result;

  // print the value
  writef(", value=%d", (i64)value);

  // success
  return result;
}

static i64 thrift_dump_i16(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  i16 value;
  i64 result;

  // read the i16 value from the buffer
  result = thrift_read_i16(&value, buffer, buffer_size);
  if (result < 0) return result;

  // print the value
  writef(", value=%d", (i64)value);

  // success
  return result;
}

static i64 thrift_dump_i32(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  i32 value;
  i64 result;

  // read the i32 value from the buffer
  result = thrift_read_i32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // print the value
  writef(", value=%d", (i64)value);

  // success
  return result;
}

static i64 thrift_dump_i64(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  i64 value;
  i64 result;

  // read the i64 value from the buffer
  result = thrift_read_i64(&value, buffer, buffer_size);
  if (result < 0) return result;

  // print the value
  writef(", value=%d", value);

  // success
  return result;
}

static i64 thrift_dump_binary(struct thrift_dump_context *, const char *buffer, u64 buffer_size) {
  u32 size;
  i64 result;

  // read the size of the binary data
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  buffer += result;
  buffer_size -= result;

  // check if the buffer is large enough
  if (buffer_size < size) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // print the binary content
  writef(", size=%d, ascii=%a", size, buffer, size);

  // success
  return result + size;
}

static const char *thrift_type_to_string(enum thrift_type type) {
  if (type < THRIFT_TYPE_SIZE) {
    return THRIFT_TYPE_TO_NAME[type];
  }

  return "unknown";
}

static i64 thrift_dump_list(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size) {
  u32 index;
  i64 result, read;
  struct thrift_list_header header;

  // default
  read = 0;
  index = 0;

  // read the list header containing size and type
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // check if the dump function is out of range
  if (header.type >= THRIFT_TYPE_SIZE) return THRIFT_ERROR_INVALID_VALUE;

  // check if the dump function is available
  if (THRIFT_DUMP_LIST_FN[header.type] == NULL) return THRIFT_ERROR_INVALID_VALUE;

  ctx->indent++;
  writef(", size=%d, item-type=%s\n%ilist-start", header.size, thrift_type_to_string(header.type), ctx->indent);
  ctx->indent++;

  for (index = 0; index < header.size; index++) {
    writef("\n%iindex=%d, type=%s", ctx->indent, index, thrift_type_to_string(header.type));

    // read the list element content and print it
    result = THRIFT_DUMP_LIST_FN[header.type](ctx, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  ctx->indent--;
  writef("\n%ilist-end", ctx->indent);
  ctx->indent--;

  return read;
}

static i64 thrift_dump_struct(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct thrift_struct_header header;

  // default
  read = 0;
  header.field = 0;

  if (ctx->indent > 0) {
    writef("\n");
  }

  ctx->indent++;
  writef("%istruct-start\n", ctx->indent);
  ctx->indent++;

  while (TRUE) {
    // read the next struct header of the footer
    result = thrift_read_struct_header(&header, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;

    // check if we reached the end of the struct
    if (header.type == THRIFT_TYPE_STOP) {
      writef("%ifield=%d, type=%s\n", ctx->indent, header.field, thrift_type_to_string(header.type));
      break;
    }

    writef("%ifield=%d, type=%s", ctx->indent, header.field, thrift_type_to_string(header.type));

    // check if the field type is out of range
    if (header.type >= THRIFT_TYPE_SIZE) return THRIFT_ERROR_INVALID_VALUE;

    // check if the dump function is available
    if (THRIFT_DUMP_STRUCT_FN[header.type] == NULL) return THRIFT_ERROR_INVALID_VALUE;

    // call the dump function for the field type
    result = THRIFT_DUMP_STRUCT_FN[header.type](ctx, buffer, buffer_size);
    if (result < 0) return result;

    writef("\n");

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  ctx->indent--;
  writef("%istruct-end", ctx->indent);
  ctx->indent--;

  return read;
}

i32 thrift_main() {
  i64 result, read;
  const u64 SIZE = 4 * 4096;

  char *buffer;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dump_context ctx;

  // new memory pool
  malloc_init(&pool);

  // prepare the lease
  lease.size = SIZE;

  // acquire memory for the input
  result = malloc_acquire(&pool, &lease);
  if (result < 0) goto clear_memory_init;

  read = 0;
  buffer = (char *)lease.ptr;
  buffer_size = lease.size;

  // initialize the context
  ctx.indent = 0;

  do {
    // read data from standard input
    result = stdin_read(buffer, buffer_size);
    if (result < 0) goto clean_memory_alloc;

    // advance the read pointer
    read += result;
    buffer += result;
    buffer_size -= result;
  } while (result > 0 && buffer_size > 0);

  if (buffer_size == 0) {
    result = THRIFT_ERROR_BUFFER_OVERFLOW;
    goto clean_memory_alloc;
  }

  // rewind the buffer
  buffer -= read;
  buffer_size = read;
  read = 0;

  // dump the thrift struct
  result = thrift_dump_struct(&ctx, buffer, buffer_size);
  if (result < 0) goto clean_memory_alloc;

  // success
  writef("\n");
  result = 0;

clean_memory_alloc:
  malloc_release(&pool, &lease);

clear_memory_init:
  malloc_destroy(&pool);
  if (result == 0) return 0;

  errorf("Something wrong happened; error=%r\n", result);
  return result;
}

#endif
