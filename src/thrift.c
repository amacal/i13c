#include "thrift.h"
#include "malloc.h"
#include "runner.h"
#include "stdin.h"
#include "stdout.h"
#include "typing.h"

/// @brief Thrift ignore field function type.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_ignore_field_fn)(const char *buffer, u64 buffer_size);

// forward declarations of ignore functions
static i64 thrift_ignore_field_bool_true(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_bool_false(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_bool(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_i8(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_i16(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_i32(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_i64(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_binary(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_list(const char *buffer, u64 buffer_size);
static i64 thrift_ignore_field_struct(const char *buffer, u64 buffer_size);

// represents a mapping between types and ignore functions used for struct fields
static const thrift_ignore_field_fn THRIFT_IGNORE_STRUCT_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_ignore_field_bool_true,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_ignore_field_bool_false,
  [THRIFT_TYPE_I8] = thrift_ignore_field_i8,
  [THRIFT_TYPE_I16] = thrift_ignore_field_i16,
  [THRIFT_TYPE_I32] = thrift_ignore_field_i32,
  [THRIFT_TYPE_I64] = thrift_ignore_field_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_ignore_field_binary,
  [THRIFT_TYPE_LIST] = thrift_ignore_field_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_ignore_field_struct,
  [THRIFT_TYPE_UUID] = NULL};

// represents a mapping between types and ignore functions used for list elements
static const thrift_ignore_field_fn THRIFT_IGNORE_LIST_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_ignore_field_bool,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_ignore_field_bool,
  [THRIFT_TYPE_I8] = thrift_ignore_field_i8,
  [THRIFT_TYPE_I16] = thrift_ignore_field_i16,
  [THRIFT_TYPE_I32] = thrift_ignore_field_i32,
  [THRIFT_TYPE_I64] = thrift_ignore_field_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_ignore_field_binary,
  [THRIFT_TYPE_LIST] = thrift_ignore_field_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_ignore_field_struct,
  [THRIFT_TYPE_UUID] = NULL};

static i64 thrift_ignore_field_bool_true(const char *, u64) {
  return 0;
}

static i64 thrift_ignore_field_bool_false(const char *, u64) {
  return 0;
}

static i64 thrift_ignore_field_bool(const char *buffer, u64 buffer_size) {
  return thrift_read_bool(NULL, buffer, buffer_size);
}

static i64 thrift_ignore_field_i8(const char *buffer, u64 buffer_size) {
  return thrift_read_i8(NULL, buffer, buffer_size);
}

static i64 thrift_ignore_field_i16(const char *buffer, u64 buffer_size) {
  return thrift_read_i16(NULL, buffer, buffer_size);
}

static i64 thrift_ignore_field_i32(const char *buffer, u64 buffer_size) {
  return thrift_read_i32(NULL, buffer, buffer_size);
}

static i64 thrift_ignore_field_i64(const char *buffer, u64 buffer_size) {
  return thrift_read_i64(NULL, buffer, buffer_size);
}

static i64 thrift_ignore_field_binary(const char *buffer, u64 buffer_size) {
  i64 result, read;
  u32 size;

  // default
  read = 0;

  // read the size of the binary data to learn how many bytes to read
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // read the binary content, it will be ignored
  result = thrift_read_binary_content(NULL, size, buffer, buffer_size);
  if (result < 0) return result;

  // success
  return read + result;
}

static i64 thrift_ignore_field_struct(const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct thrift_struct_header header;

  // initialize
  read = 0;
  header.field = 0;

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
      break;
    }

    // call the ignore function
    result = thrift_ignore_field(NULL, header.type, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // success
  return read;
}

static i64 thrift_ignore_field_list(const char *buffer, u64 buffer_size) {
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

  // check if the ignore function is out of range
  if (header.type >= THRIFT_TYPE_SIZE) return THRIFT_ERROR_INVALID_VALUE;

  // check if the ignore function is available
  if (THRIFT_IGNORE_LIST_FN[header.type] == NULL) return THRIFT_ERROR_INVALID_VALUE;

  for (index = 0; index < header.size; index++) {
    // read the list element content, it will be ignored
    result = THRIFT_IGNORE_LIST_FN[header.type](buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // success
  return read;
}

i64 thrift_ignore_field(void *, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  // check if the field type is out of range
  if (field_type >= THRIFT_TYPE_SIZE) return THRIFT_ERROR_INVALID_VALUE;

  // check if the ignore function is available
  if (THRIFT_IGNORE_STRUCT_FN[field_type] == NULL) return THRIFT_ERROR_INVALID_VALUE;

  // success
  return THRIFT_IGNORE_STRUCT_FN[field_type](buffer, buffer_size);
}

i64 thrift_read_struct_header(struct thrift_struct_header *target, const char *buffer, u64 buffer_size) {
  i64 result, read;
  u16 delta, type;

  // check if the buffer is large enough
  if (buffer_size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // extract values from the short notation
  type = *buffer & 0x0f;
  delta = (*buffer & 0xf0) >> 4;

  // move the buffer pointer and size
  read = 1;
  buffer += 1;
  buffer_size -= 1;

  // check for the last field
  if (type == THRIFT_TYPE_STOP) {
    target->field = 0;
    target->type = type;
    return read;
  }

  // if delta is zero, follow long notation
  if (delta == 0) {
    result = thrift_read_u16(&delta, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // if delta is zero, it is invalid
  if (delta == 0) return THRIFT_ERROR_INVALID_VALUE;

  // update the target struct header
  target->field += delta;
  target->type = type;

  // if the field index is too large (32767), return an error
  if (target->field > 0x7fff) return THRIFT_ERROR_BITS_OVERFLOW;

  // success
  return read;
}

i64 thrift_read_struct_content(
  void *target, thrift_read_fn *fields, u32 field_size, const char *buffer, u64 buffer_size) {

  i64 result, read;
  struct thrift_struct_header header;

  // default
  read = 0;
  header.field = 0;

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
      break;
    }

    // call the field callback or ignore function
    if (header.field >= field_size) {
      result = thrift_ignore_field(NULL, header.type, buffer, buffer_size);
    } else {
      result = fields[header.field](target, header.type, buffer, buffer_size);
    }

    // perhaps callback failed
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // success
  return read;
}

i64 thrift_read_binary_header(u32 *target, const char *buffer, u64 buffer_size) {
  i64 result;

  // read the size of the binary data
  result = thrift_read_u32(target, buffer, buffer_size);
  if (result < 0) return result;

  // success
  return result;
}

i64 thrift_read_binary_content(char *target, u32 size, const char *buffer, u64 buffer_size) {
  u32 index;

  // check if the buffer is large enough
  if (buffer_size < size) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // copy byte by byte
  if (target) {
    for (index = 0; index < size; index++) {
      target[index] = buffer[index];
    }

    // terminate the buffer
    target[size] = 0;
  }

  // success
  return size;
}

i64 thrift_read_list_header(struct thrift_list_header *target, const char *buffer, u64 buffer_size) {
  i64 result, read;
  u32 size, type;

  // check if the buffer is large enough
  if (buffer_size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // extract values from the short notation
  type = *buffer & 0x0f;
  size = (*buffer & 0xf0) >> 4;

  // move the buffer pointer and size
  read = 1;
  buffer += 1;
  buffer_size -= 1;

  // if the size is 0x0f, follow long notation
  if (size == 0x0f) {
    result = thrift_read_u32(&size, buffer, buffer_size);
    if (result < 0) return result;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  // copy the values to the target
  target->type = type;
  target->size = size;

  // success
  return read;
}

i64 thrift_read_bool(bool *target, const char *buffer, u64 buffer_size) {
  bool value;

  // check if the buffer is large enough
  if (buffer_size < 1) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // read the bool
  switch (*buffer) {
      case THRIFT_TYPE_BOOL_TRUE:
        value = TRUE;
        break;
      case THRIFT_TYPE_BOOL_FALSE:
        value = FALSE;
        break;
      default:
        return THRIFT_ERROR_INVALID_VALUE;
    }

  // set the target
  if (target) *target = value;

  // success
  return 1;
}

i64 thrift_read_i8(i8 *target, const char *buffer, u64 buffer_size) {
  // check if the buffer is large enough
  if (buffer_size < 1) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // read the byte
  if (target) *target = (i8)*buffer;

  // success
  return 1;
}

i64 thrift_read_u16(u16 *target, const char *buffer, u64 buffer_size) {
  i64 result;
  u32 value;

  // read the unsigned 32-bit value
  result = thrift_read_u32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // check if the value fits into u16
  if (value > 0xffff) return THRIFT_ERROR_BITS_OVERFLOW;

  // convert to u16
  if (target) *target = (u16)value;

  // success
  return result;
}

i64 thrift_read_i16(i16 *target, const char *buffer, u64 buffer_size) {
  i64 result;
  u16 value;

  // read the unsigned 16-bit value
  result = thrift_read_u16(&value, buffer, buffer_size);
  if (result < 0) return result;

  // zigzag to i16
  if (target) *target = (i16)((value >> 1) ^ -(value & 1));

  // success
  return result;
}

i64 thrift_read_u32(u32 *target, const char *buffer, u64 buffer_size) {
  u8 next, shift;
  u32 value;

  // default to zero
  value = 0;
  shift = 0;

  // set the continuation bit
  next = 0x80;

  // loop until varint ends or buffer is exhausted
  while (buffer_size && (next & 0x80) && shift <= 28) {
    next = *buffer++;
    buffer_size--;

    // accumulate the value (LEB128 encoding)
    value |= (next & 0x7f) << shift;
    shift += 7;
  }

  // check if the buffer is too small
  if (next & 0x80) {
    return THRIFT_ERROR_BUFFER_OVERFLOW;
  }

  // check for the last byte overflow
  if (shift >= 28 && (next & 0xf0)) {
    return THRIFT_ERROR_BITS_OVERFLOW;
  }

  // success
  *target = value;
  return shift / 7;
}

i64 thrift_read_i32(i32 *target, const char *buffer, u64 buffer_size) {
  i64 result;
  u32 value;

  // read the unsigned 32-bit value
  result = thrift_read_u32(&value, buffer, buffer_size);
  if (result < 0) return result;

  // zigzag to i32
  if (target) *target = (i32)((value >> 1) ^ -(value & 1));

  // success
  return result;
}

i64 thrift_read_i64(i64 *target, const char *buffer, u64 buffer_size) {
  u8 next, shift;
  u64 value;

  // default to zero
  value = 0;
  shift = 0;

  // set the continuation bit
  next = 0x80;

  // loop until varint ends or buffer is exhausted
  while (buffer_size && (next & 0x80) && shift <= 56) {
    next = *buffer++;
    buffer_size--;

    // accumulate the value (LEB128 encoding)
    value |= (next & 0x7f) << shift;
    shift += 7;
  }

  // check if the buffer is too small
  if (next & 0x80) {
    return THRIFT_ERROR_BUFFER_OVERFLOW;
  }

  // check for the last byte overflow
  if (shift >= 56 && (next & 0xf0)) {
    return THRIFT_ERROR_BITS_OVERFLOW;
  }

  // zigzag to i64
  if (target) *target = (i64)((value >> 1) ^ -(value & 1));

  // success
  return shift / 7;
}

#if defined(I13C_TESTS)

static void can_read_struct_header_short_version() {
  struct thrift_struct_header header;
  const char buffer[] = {0x35, 0x44, 0x00};
  i64 result;

  // initialize
  header.field = 0;

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.field == 3, "should read field 3");
  assert(header.type == THRIFT_TYPE_I32, "should read type THRIFT_TYPE_I32");

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer + 1, sizeof(buffer) - 1);

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.field == 7, "should read field 7");
  assert(header.type == THRIFT_TYPE_I16, "should read type THRIFT_TYPE_I16");

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer + 2, sizeof(buffer) - 2);

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.field == 0, "should read stop field");
  assert(header.type == THRIFT_TYPE_STOP, "should read type THRIFT_TYPE_STOP");
}

static void can_detect_struct_header_short_buffer_overflow() {
  struct thrift_struct_header header;
  const char buffer[] = {};

  // read the struct header from the buffer
  i64 result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_struct_header_long_version() {
  struct thrift_struct_header header;
  const char buffer[] = {0x05, 0x10, 0x04, 0x11, 0x00};
  i64 result;

  // initialize
  header.field = 0;

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(header.field == 16, "should read field 16");
  assert(header.type == THRIFT_TYPE_I32, "should read type THRIFT_TYPE_I32");

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer + 2, sizeof(buffer) - 2);

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(header.field == 33, "should read field 33");
  assert(header.type == THRIFT_TYPE_I16, "should read type THRIFT_TYPE_I16");

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer + 4, sizeof(buffer) - 4);

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.field == 0, "should read stop field");
  assert(header.type == THRIFT_TYPE_STOP, "should read type THRIFT_TYPE_STOP");
}

static void can_detect_struct_header_long_buffer_overflow() {
  struct thrift_struct_header header;
  const char buffer[] = {0x05};

  // read the struct header from the buffer
  i64 result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_detect_struct_header_long_zero_delta() {
  struct thrift_struct_header header;
  const char buffer[] = {0x05, 0x00};

  // read the struct header from the buffer
  i64 result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_INVALID_VALUE, "should fail with THRIFT_ERROR_INVALID_VALUE for zero delta");
}

static void can_detect_struct_header_long_bit_overflow_01() {
  struct thrift_struct_header header;
  const char buffer[] = {0x05, 0xff, 0xff, 0x02};

  // default
  header.field = 0;

  // read the struct header from the buffer
  i64 result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_struct_header_long_bit_overflow_02() {
  struct thrift_struct_header header;
  const char buffer[] = {0x05, 0xff, 0xff, 0x01, 0x05, 0xff, 0x7f};

  // default
  header.field = 0;

  // read the struct header from the buffer
  i64 result = thrift_read_struct_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 4, "should read four bytes");

  // read the struct header from the buffer
  result = thrift_read_struct_header(&header, buffer + 4, sizeof(buffer) - 4);

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_read_struct_content_empty() {
  const char buffer[] = {0x00};
  i64 result, context;

  const u32 FIELDS_SLOTS = 3;
  thrift_read_fn fields[FIELDS_SLOTS];

  // default
  context = 0;

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)thrift_ignore_field; // ignored
  fields[2] = (thrift_read_fn)thrift_ignore_field; // ignored

  // read the struct header from the buffer
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(context == 0, "should not change context");
}

static void can_read_struct_content_one_field() {
  i64 result, context;
  const char buffer[] = {0x15, 0x13, 0x00};

  const u32 FIELDS_SLOTS = 3;
  thrift_read_fn fields[FIELDS_SLOTS];

  // default
  context = 0;

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)thrift_ignore_field; // ignored
  fields[2] = (thrift_read_fn)thrift_ignore_field; // ignored

  // read the struct header from the buffer
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(context == 0, "should not change context");
}

static void can_read_struct_content_two_fields() {
  i64 result, context;
  const char buffer[] = {0x15, 0x13, 0x15, 0x14, 0x00};

  const u32 FIELDS_SLOTS = 3;
  thrift_read_fn fields[FIELDS_SLOTS];

  // default
  context = 0;

  // prepare the mapping of fields
  fields[1] = (thrift_read_fn)thrift_ignore_field; // ignored
  fields[2] = (thrift_read_fn)thrift_ignore_field; // ignored

  // read the struct header from the buffer
  result = thrift_read_struct_content(&context, fields, FIELDS_SLOTS, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(context == 0, "should not change context");
}

static void can_ignore_struct_content() {
  i64 result;
  const char buffer[] = {0x35, 0x13, 0x44, 0x14, 0x00};

  // read the struct header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_STRUCT, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
}

static void can_read_binary_header() {
  u32 size;
  const char buffer[] = {0x85, 0x01};

  // read the binary header from the buffer
  i64 result = thrift_read_binary_header(&size, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(size == 133, "should read size 133");
}

static void can_detect_binary_header_buffer_overflow() {
  u32 size;
  const char buffer[] = {};

  // read the binary header from the buffer
  i64 result = thrift_read_binary_header(&size, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_binary_content() {
  char content[] = {0xff, 0xff, 0xff};
  const char buffer[] = {0x01, 0x02, 0x03};

  // read the binary content
  i64 result = thrift_read_binary_content(content, 2, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(content[0] == 0x01, "should read first byte as 0x01");
  assert(content[1] == 0x02, "should read second byte as 0x02");
  assert(content[2] == 0x00, "should terminate the buffer with 0x00");
}

static void can_read_binary_content_buffer_overflow() {
  char content[] = {0xff, 0xff, 0xff};
  const char buffer[] = {0x01};

  // read the binary content
  i64 result = thrift_read_binary_content(content, 2, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_ignore_binary_content() {
  const char buffer[] = {0x03, 0x01, 0x02, 0x03};

  // read the binary content from the buffer into nothing
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == 4, "should read four bytes");
}

static void can_detect_binary_ignore_buffer_overflow_01() {
  const char buffer[] = {};

  // read the binary content from the buffer into nothing
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_detect_binary_ignore_buffer_overflow_02() {
  const char buffer[] = {0x03, 0x01, 0x02};

  // read the binary content from the buffer into nothing
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_BINARY, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_bool() {
  bool value;
  const char buffer[] = {0x01, 0x02};

  // read the boolean value from the buffer
  i64 result = thrift_read_bool(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == TRUE, "should read value TRUE");

  // read the boolean value from the buffer
  result = thrift_read_bool(&value, buffer + 1, sizeof(buffer) - 1);

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == FALSE, "should read value FALSE");
}

static void can_ignore_bool_in_list() {
  i64 result;
  const char buffer[] = {0x21, 0x01, 0x02, 0x22, 0x01, 0x02};

  // read the boolean value from the buffer
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");

  // read the boolean value from the buffer
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer + 3, sizeof(buffer) - 3);

  // assert the result
  assert(result == 3, "should read three bytes");
}

static void can_ignore_bool_true() {
  const char buffer[] = {};

  // read the boolean value from the buffer into nothing
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_BOOL_TRUE, buffer, sizeof(buffer));

  // assert the result
  assert(result == 0, "shouldn't read any bytes");
}

static void can_ignore_bool_false() {
  const char buffer[] = {};

  // read the boolean value from the buffer into nothing
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_BOOL_FALSE, buffer, sizeof(buffer));

  // assert the result
  assert(result == 0, "shouldn't read any bytes");
}

static void can_detect_bool_buffer_overflow() {
  bool value;
  const char buffer[] = {};

  // read the boolean value from the buffer
  i64 result = thrift_read_bool(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_detect_bool_invalid_value() {
  bool value;
  const char buffer[] = {0x03};

  // read the boolean value from the buffer
  i64 result = thrift_read_bool(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_INVALID_VALUE, "should fail with THRIFT_ERROR_INVALID_VALUE");
}

static void can_detect_bool_invalid_value_in_list() {
  i64 result;
  const char buffer[] = {0x11, 0x03};

  // read the boolean value from the buffer
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_INVALID_VALUE, "should fail with THRIFT_ERROR_INVALID_VALUE");
}

static void can_read_list_header_short_version() {
  struct thrift_list_header header;
  const char buffer[] = {0x35};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.size == 3, "should read size 3");
  assert(header.type == THRIFT_TYPE_I32, "should read type THRIFT_TYPE_I32");
}

static void can_detect_list_header_short_buffer_overflow() {
  struct thrift_list_header header;
  const char buffer[] = {};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_list_header_long_version() {
  struct thrift_list_header header;
  const char buffer[] = {0xf5, 0x0f};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(header.size == 15, "should read size 15");
  assert(header.type == THRIFT_TYPE_I32, "should read type THRIFT_TYPE_I32");
}

static void can_detect_list_header_long_buffer_overflow() {
  struct thrift_list_header header;
  const char buffer[] = {0xf5};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_ignore_list_content() {
  i64 result;
  const char buffer[] = {0x35, 0x13, 0x44, 0x14};

  // read the list header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == 4, "should read four bytes");
}

static void can_detect_list_ignore_buffer_overflow_01() {
  i64 result;
  const char buffer[] = {};

  // read the list header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_detect_list_ignore_buffer_overflow_02() {
  i64 result;
  const char buffer[] = {0x35, 0x13, 0x44};

  // read the list header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_detect_list_ignore_invalid_type_01() {
  i64 result;
  const char buffer[] = {0x3f};

  // read the list header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_INVALID_VALUE, "should fail with THRIFT_ERROR_INVALID_VALUE");
}

static void can_detect_list_ignore_invalid_type_02() {
  i64 result;
  const char buffer[] = {0x30};

  // read the list header from the buffer into nothing
  result = thrift_ignore_field(NULL, THRIFT_TYPE_LIST, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_INVALID_VALUE, "should fail with THRIFT_ERROR_INVALID_VALUE");
}

static void can_read_i8_positive() {
  i8 value;
  const char buffer[] = {0x14};

  // read the i8 value from the buffer
  i64 result = thrift_read_i8(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 20, "should read value 20");
}

static void can_read_i8_negative() {
  i8 value;
  const char buffer[] = {0xe4};

  // read the i8 value from the buffer
  i64 result = thrift_read_i8(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == -28, "should read value -28");
}

static void can_ignore_i8_value() {
  const char buffer[] = {0xfe, 0xff, 0x01};

  // read the i8 value from the buffer
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_I8, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
}

static void can_detect_i8_buffer_overflow() {
  i8 value;
  const char buffer[] = {};

  // read the i8 value from the buffer
  i64 result = thrift_read_i8(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_single_byte_i16_positive() {
  i16 value;
  const char buffer[] = {0x14};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 10, "should read value 10");
}

static void can_read_single_byte_i16_negative() {
  i16 value;
  const char buffer[] = {0x13};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == -10, "should read value -10");
}

static void can_read_multiple_bytes_i16_positive() {
  i16 value;
  const char buffer[] = {0xf2, 0x14};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(value == 1337, "should read value 1337");
}

static void can_read_multiple_bytes_i16_negative() {
  i16 value;
  const char buffer[] = {0xf1, 0x14};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(value == -1337, "should read value -1337");
}

static void can_handle_min_i16_value() {
  i16 value;
  const char buffer[] = {0xff, 0xff, 0x03};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read five bytes");
  assert(value == -32768, "should read value -32768");
}

static void can_handle_max_i16_value() {
  i16 value;
  const char buffer[] = {0xfe, 0xff, 0x03};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 32767, "should read value 32767");
}

static void can_ignore_i16_value() {
  const char buffer[] = {0xfe, 0xff, 0x01};

  // read the i16 value from the buffer
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_I16, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
}

static void can_detect_i16_bits_overflow() {
  i16 value;
  const char buffer[] = {0xff, 0xff, 0x04};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_i16_buffer_overflow() {
  i16 value;
  const char buffer[] = {0xff, 0xff};

  // read the i16 value from the buffer
  i64 result = thrift_read_i16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_single_byte_u16_positive() {
  u16 value;
  const char buffer[] = {0x14};

  // read the u16 value from the buffer
  i64 result = thrift_read_u16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 20, "should read value 20");
}

static void can_read_multiple_bytes_u16_positive() {
  u16 value;
  const char buffer[] = {0xf2, 0x14};

  // read the u16 value from the buffer
  i64 result = thrift_read_u16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(value == 2674, "should read value 2674");
}

static void can_handle_max_u16_value() {
  u16 value;
  const char buffer[] = {0xff, 0xff, 0x03};

  // read the u16 value from the buffer
  i64 result = thrift_read_u16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 65535, "should read value 65535");
}

static void can_detect_u16_bits_overflow() {
  u16 value;
  const char buffer[] = {0xff, 0xff, 0x07};

  // read the u16 value from the buffer
  i64 result = thrift_read_u16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_u16_buffer_overflow() {
  u16 value;
  const char buffer[] = {0xff, 0xff};

  // read the u16 value from the buffer
  i64 result = thrift_read_u16(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_single_byte_i32_positive() {
  i32 value;
  const char buffer[] = {0x14};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 10, "should read value 10");
}

static void can_read_single_byte_i32_negative() {
  i32 value;
  const char buffer[] = {0x13};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == -10, "should read value -10");
}

static void can_read_multiple_bytes_i32_positive() {
  i32 value;
  const char buffer[] = {0xf2, 0x94, 0x12};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 148793, "should read value 148793");
}

static void can_read_multiple_bytes_i32_negative() {
  i32 value;
  const char buffer[] = {0xf1, 0x94, 0x12};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == -148793, "should read value -148793");
}

static void can_handle_min_i32_value() {
  i32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0x0f};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value == -2147483648, "should read value -2147483648");
}

static void can_handle_max_i32_value() {
  i32 value;
  const char buffer[] = {0xfe, 0xff, 0xff, 0xff, 0x0f};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value == 2147483647, "should read value 2147483647");
}

static void can_ignore_i32_value() {
  const char buffer[] = {0xfe, 0xff, 0x0f};

  // read the i32 value from the buffer
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_I32, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
}

static void can_detect_i32_bits_overflow() {
  i32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0x10};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_i32_buffer_overflow() {
  i32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_single_byte_u32_positive() {
  u32 value;
  const char buffer[] = {0x14};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 20, "should read value 20");
}

static void can_read_multiple_bytes_u32_positive() {
  u32 value;
  const char buffer[] = {0xf2, 0x94, 0x12};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 297586, "should read value 297586");
}

static void can_handle_max_u32_value() {
  u32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0x0f};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 5, "should read five bytes");
  assert(value == 4294967295, "should read value 4294967295");
}

static void can_detect_u32_bits_overflow() {
  u32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0x10};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_u32_buffer_overflow() {
  u32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

static void can_read_single_byte_i64_positive() {
  i64 value;
  const char buffer[] = {0x14};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == 10, "should read value 10");
}

static void can_read_single_byte_i64_negative() {
  i64 value;
  const char buffer[] = {0x13};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(value == -10, "should read value -10");
}

static void can_read_multiple_bytes_i64_positive() {
  i64 value;
  const char buffer[] = {0xf2, 0x94, 0x12};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == 148793, "should read value 148793");
}

static void can_read_multiple_bytes_i64_negative() {
  i64 value;
  const char buffer[] = {0xf1, 0x94, 0x12};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
  assert(value == -148793, "should read value -148793");
}

static void can_handle_min_i64_value() {
  i64 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 8, "should read eight bytes");
  assert(value == -9223372036854775807ll - 1, "should read value -9223372036854775808");
}

static void can_handle_max_i64_value() {
  i64 value;
  const char buffer[] = {0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == 8, "should read eight bytes");
  assert(value == 9223372036854775807, "should read value 9223372036854775807");
}

static void can_ignore_i64_value() {
  const char buffer[] = {0xfe, 0xff, 0x0f};

  // read the i64 value from the buffer
  i64 result = thrift_ignore_field(NULL, THRIFT_TYPE_I64, buffer, sizeof(buffer));

  // assert the result
  assert(result == 3, "should read three bytes");
}

static void can_detect_i64_bits_overflow() {
  i64 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BITS_OVERFLOW, "should fail with THRIFT_ERROR_BITS_OVERFLOW");
}

static void can_detect_i64_buffer_overflow() {
  i64 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == THRIFT_ERROR_BUFFER_OVERFLOW, "should fail with THRIFT_ERROR_BUFFER_OVERFLOW");
}

void thrift_test_cases(struct runner_context *ctx) {
  // list cases
  test_case(ctx, "can read list header short version", can_read_list_header_short_version);
  test_case(ctx, "can detect list header short buffer overflow", can_detect_list_header_short_buffer_overflow);
  test_case(ctx, "can read list header long version", can_read_list_header_long_version);
  test_case(ctx, "can detect list header long buffer overflow", can_detect_list_header_long_buffer_overflow);

  // struct header cases
  test_case(ctx, "can read struct header short version", can_read_struct_header_short_version);
  test_case(ctx, "can detect struct header short buffer overflow", can_detect_struct_header_short_buffer_overflow);
  test_case(ctx, "can read struct header long version", can_read_struct_header_long_version);
  test_case(ctx, "can detect struct header long buffer overflow", can_detect_struct_header_long_buffer_overflow);
  test_case(ctx, "can detect struct header long zero delta", can_detect_struct_header_long_zero_delta);
  test_case(ctx, "can detect struct header long bit overflow 1", can_detect_struct_header_long_bit_overflow_01);
  test_case(ctx, "can detect struct header long bit overflow 2", can_detect_struct_header_long_bit_overflow_02);

  // struct content cases
  test_case(ctx, "can read struct content empty", can_read_struct_content_empty);
  test_case(ctx, "can read struct content one field", can_read_struct_content_one_field);
  test_case(ctx, "can read struct content two fields", can_read_struct_content_two_fields);

  // binary cases
  test_case(ctx, "can read binary header", can_read_binary_header);
  test_case(ctx, "can detect binary header buffer overflow", can_detect_binary_header_buffer_overflow);
  test_case(ctx, "can read binary content", can_read_binary_content);
  test_case(ctx, "can read binary content buffer overflow", can_read_binary_content_buffer_overflow);

  // bool cases
  test_case(ctx, "can read bool", can_read_bool);
  test_case(ctx, "can detect bool buffer overflow", can_detect_bool_buffer_overflow);
  test_case(ctx, "can detect bool invalid value", can_detect_bool_invalid_value);
  test_case(ctx, "can detect bool invalid value in list", can_detect_bool_invalid_value_in_list);

  // i8 cases
  test_case(ctx, "can read i8 positive", can_read_i8_positive);
  test_case(ctx, "can read i8 negative", can_read_i8_negative);
  test_case(ctx, "can detect i8 buffer overflow", can_detect_i8_buffer_overflow);

  // i16 cases
  test_case(ctx, "can read single-byte i16 positive", can_read_single_byte_i16_positive);
  test_case(ctx, "can read single-byte i16 negative", can_read_single_byte_i16_negative);
  test_case(ctx, "can read multiple bytes i16 positive", can_read_multiple_bytes_i16_positive);
  test_case(ctx, "can read multiple bytes i16 negative", can_read_multiple_bytes_i16_negative);
  test_case(ctx, "can handle min i16 value", can_handle_min_i16_value);
  test_case(ctx, "can handle max i16 value", can_handle_max_i16_value);
  test_case(ctx, "can detected i16 bits overflow", can_detect_i16_bits_overflow);
  test_case(ctx, "can detected i16 buffer overflow", can_detect_i16_buffer_overflow);

  // u16 cases
  test_case(ctx, "can read single-byte u16 positive", can_read_single_byte_u16_positive);
  test_case(ctx, "can read multiple bytes u16 positive", can_read_multiple_bytes_u16_positive);
  test_case(ctx, "can handle max u16 value", can_handle_max_u16_value);
  test_case(ctx, "can detected u16 bits overflow", can_detect_u16_bits_overflow);
  test_case(ctx, "can detected u16 buffer overflow", can_detect_u16_buffer_overflow);

  // i32 cases
  test_case(ctx, "can read single-byte i32 positive", can_read_single_byte_i32_positive);
  test_case(ctx, "can read single-byte i32 negative", can_read_single_byte_i32_negative);
  test_case(ctx, "can read multiple bytes i32 positive", can_read_multiple_bytes_i32_positive);
  test_case(ctx, "can read multiple bytes i32 negative", can_read_multiple_bytes_i32_negative);
  test_case(ctx, "can handle min i32 value", can_handle_min_i32_value);
  test_case(ctx, "can handle max i32 value", can_handle_max_i32_value);
  test_case(ctx, "can detected i32 bits overflow", can_detect_i32_bits_overflow);
  test_case(ctx, "can detected i32 buffer overflow", can_detect_i32_buffer_overflow);

  // u32 cases
  test_case(ctx, "can read single-byte u32 positive", can_read_single_byte_u32_positive);
  test_case(ctx, "can read multiple bytes u32 positive", can_read_multiple_bytes_u32_positive);
  test_case(ctx, "can handle max u32 value", can_handle_max_u32_value);
  test_case(ctx, "can detected u32 bits overflow", can_detect_u32_bits_overflow);
  test_case(ctx, "can detected u32 buffer overflow", can_detect_u32_buffer_overflow);

  // i64 cases
  test_case(ctx, "can read single-byte i64 positive", can_read_single_byte_i64_positive);
  test_case(ctx, "can read single-byte i64 negative", can_read_single_byte_i64_negative);
  test_case(ctx, "can read multiple bytes i64 positive", can_read_multiple_bytes_i64_positive);
  test_case(ctx, "can read multiple bytes i64 negative", can_read_multiple_bytes_i64_negative);
  test_case(ctx, "can handle min i64 value", can_handle_min_i64_value);
  test_case(ctx, "can handle max i64 value", can_handle_max_i64_value);
  test_case(ctx, "can detected i64 bits overflow", can_detect_i64_bits_overflow);
  test_case(ctx, "can detected i64 buffer overflow", can_detect_i64_buffer_overflow);

  // ignore cases
  test_case(ctx, "can ignore list content", can_ignore_list_content);
  test_case(ctx, "can detect list ignore buffer overflow 1", can_detect_list_ignore_buffer_overflow_01);
  test_case(ctx, "can detect list ignore buffer overflow 2", can_detect_list_ignore_buffer_overflow_02);
  test_case(ctx, "can detect list ignore invalid type 1", can_detect_list_ignore_invalid_type_01);
  test_case(ctx, "can detect list ignore invalid type 2", can_detect_list_ignore_invalid_type_02);
  test_case(ctx, "can ignore struct content", can_ignore_struct_content);
  test_case(ctx, "can ignore binary content", can_ignore_binary_content);
  test_case(ctx, "can detect binary ignore buffer overflow 1", can_detect_binary_ignore_buffer_overflow_01);
  test_case(ctx, "can detect binary ignore buffer overflow 2", can_detect_binary_ignore_buffer_overflow_02);
  test_case(ctx, "can ignore bool in list", can_ignore_bool_in_list);
  test_case(ctx, "can ignore bool true", can_ignore_bool_true);
  test_case(ctx, "can ignore bool false", can_ignore_bool_false);
  test_case(ctx, "can ignore i8 value", can_ignore_i8_value);
  test_case(ctx, "can ignore i16 value", can_ignore_i16_value);
  test_case(ctx, "can ignore i32 value", can_ignore_i32_value);
  test_case(ctx, "can ignore i64 value", can_ignore_i64_value);
}

#endif

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

static const char *THRIFT_TYPE_TO_NAME[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = "stop",
  [THRIFT_TYPE_BOOL_TRUE] = "bool-true",
  [THRIFT_TYPE_BOOL_FALSE] = "bool-false",
  [THRIFT_TYPE_I8] = "i8",
  [THRIFT_TYPE_I16] = "i16",
  [THRIFT_TYPE_I32] = "i32",
  [THRIFT_TYPE_I64] = "i64",
  [THRIFT_TYPE_DOUBLE] = "double",
  [THRIFT_TYPE_BINARY] = "binary",
  [THRIFT_TYPE_LIST] = "list",
  [THRIFT_TYPE_SET] = "set",
  [THRIFT_TYPE_MAP] = "map",
  [THRIFT_TYPE_STRUCT] = "struct",
  [THRIFT_TYPE_UUID] = "uuid",
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
  writef(", value=%d", value);

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
  writef(", value=%d", value);

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
  writef(", value=%d", value);

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

int thrift_main() {
  i64 result, read;
  const u64 SIZE = 4 * 4096;

  char *buffer;
  u64 buffer_size;

  struct malloc_pool pool;
  struct thrift_dump_context ctx;

  // new memory pool
  malloc_init(&pool);

  // acquire memory for the input
  result = malloc_acquire(&pool, SIZE);
  if (result <= 0) {
    writef("Error: Could not acquire memory for input buffer: %x.\n", result);
    goto clear_memory_init;
  }

  read = 0;
  buffer = (char *)result;
  buffer_size = SIZE;

  // initialize the context
  ctx.indent = 0;

  do {
    // read data from standard input
    result = stdin_read(buffer, buffer_size);
    if (result < 0) {
      writef("Error: Could not read data from standard input: %d\n", result);
      goto clean_memory_alloc;
    }

    // advance the read pointer
    read += result;
    buffer += result;
    buffer_size -= result;
  } while (result > 0 && buffer_size > 0);

  if (buffer_size == 0) {
    writef("Error: Buffer would overflow.\n");
    goto clean_memory_alloc;
  }

  // rewind the buffer
  buffer -= read;
  buffer_size = read;
  read = 0;

  // dump the thrift struct
  result = thrift_dump_struct(&ctx, buffer, buffer_size);
  if (result < 0) {
    writef("Error: Could not dump thrift struct: %d\n", result);
    goto clean_memory_alloc;
  }

  // success
  writef("\n");
  result = 0;

clean_memory_alloc:
  malloc_release(&pool, buffer, SIZE);

clear_memory_init:
  malloc_destroy(&pool);

  return result;
}

#endif
