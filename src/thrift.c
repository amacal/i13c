#include "thrift.h"
#include "malloc.h"
#include "runner.h"
#include "stdin.h"
#include "stdout.h"
#include "typing.h"

struct thrift_dump_context {
  u32 indent;
};

typedef i64 (*thrift_ignore_field_fn)(const char *buffer, u64 buffer_size);
typedef i64 (*thrift_dump_field_fn)(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);

static i64 thrift_ignore_field_bool_true(const char *, u64) {
  return 0;
}

static i64 thrift_ignore_field_bool_false(const char *, u64) {
  return 0;
}

static i64 thrift_ignore_field_bool(const char *, u64) {
  return 1;
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
  if (result < 0) return -1;

  read += result;
  buffer += result;
  buffer_size -= result;

  // read the binary content, it will be ignored
  result = thrift_read_binary_content(NULL, size, buffer, buffer_size);
  if (result < 0) return -1;

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

  return read;
}

static i64 thrift_ignore_field_list(const char *buffer, u64 buffer_size) {
  u32 index;
  i64 result, read;

  struct thrift_list_header header;
  thrift_ignore_field_fn ignore_fn[THRIFT_TYPE_SIZE];

  // default
  read = 0;
  index = 0;

  // initialize the ignore functions
  ignore_fn[THRIFT_TYPE_STOP] = NULL;
  ignore_fn[THRIFT_TYPE_BOOL_TRUE] = thrift_ignore_field_bool;
  ignore_fn[THRIFT_TYPE_BOOL_FALSE] = thrift_ignore_field_bool;
  ignore_fn[THRIFT_TYPE_I8] = thrift_ignore_field_i8;
  ignore_fn[THRIFT_TYPE_I16] = thrift_ignore_field_i16;
  ignore_fn[THRIFT_TYPE_I32] = thrift_ignore_field_i32;
  ignore_fn[THRIFT_TYPE_I64] = thrift_ignore_field_i64;
  ignore_fn[THRIFT_TYPE_DOUBLE] = NULL;
  ignore_fn[THRIFT_TYPE_BINARY] = thrift_ignore_field_binary;
  ignore_fn[THRIFT_TYPE_LIST] = thrift_ignore_field_list;
  ignore_fn[THRIFT_TYPE_SET] = NULL;
  ignore_fn[THRIFT_TYPE_MAP] = NULL;
  ignore_fn[THRIFT_TYPE_STRUCT] = thrift_ignore_field_struct;
  ignore_fn[THRIFT_TYPE_UUID] = NULL;

  // read the list header containing size and type
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return -1;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // check if the ignore function is out of range
  if (header.type >= THRIFT_TYPE_SIZE) return -1;

  // check if the ignore function is available
  if (ignore_fn[header.type] == NULL) return -1;

  for (index = 0; index < header.size; index++) {
    // read the list element content, it will be ignored
    result = ignore_fn[header.type](buffer, buffer_size);
    if (result < 0) return -1;

    // move the buffer pointer and size
    read += result;
    buffer += result;
    buffer_size -= result;
  }

  return read;
}

i64 thrift_ignore_field(void *, enum thrift_type field_type, const char *buffer, u64 buffer_size) {
  thrift_ignore_field_fn ignore_fn[THRIFT_TYPE_SIZE];

  ignore_fn[THRIFT_TYPE_STOP] = NULL;
  ignore_fn[THRIFT_TYPE_BOOL_TRUE] = thrift_ignore_field_bool_true;
  ignore_fn[THRIFT_TYPE_BOOL_FALSE] = thrift_ignore_field_bool_false;
  ignore_fn[THRIFT_TYPE_I8] = thrift_ignore_field_i8;
  ignore_fn[THRIFT_TYPE_I16] = thrift_ignore_field_i16;
  ignore_fn[THRIFT_TYPE_I32] = thrift_ignore_field_i32;
  ignore_fn[THRIFT_TYPE_I64] = thrift_ignore_field_i64;
  ignore_fn[THRIFT_TYPE_DOUBLE] = NULL;
  ignore_fn[THRIFT_TYPE_BINARY] = thrift_ignore_field_binary;
  ignore_fn[THRIFT_TYPE_LIST] = thrift_ignore_field_list;
  ignore_fn[THRIFT_TYPE_SET] = NULL;
  ignore_fn[THRIFT_TYPE_MAP] = NULL;
  ignore_fn[THRIFT_TYPE_STRUCT] = thrift_ignore_field_struct;

  // check if the field type is out of range
  if (field_type >= THRIFT_TYPE_SIZE) return -1;

  // check if the ignore function is available
  if (ignore_fn[field_type] == NULL) return -1;

  return ignore_fn[field_type](buffer, buffer_size);
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
  // check if the buffer is large enough
  if (buffer_size < 1) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // read the bool
  if (target) switch (*buffer) {
      case THRIFT_TYPE_BOOL_TRUE:
        *target = TRUE;
        break;
      case THRIFT_TYPE_BOOL_FALSE:
        *target = FALSE;
        break;
      default:
        return THRIFT_ERROR_INVALID_VALUE;
    }

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

static void can_detect_i8_buffer_overflow() {
  i8 value;
  const char buffer[] = {};

  // read the i8 value from the buffer
  i64 result = thrift_read_i8(&value, buffer, sizeof(buffer));

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

  // struct cases
  test_case(ctx, "can read struct header short version", can_read_struct_header_short_version);
  test_case(ctx, "can detect struct header short buffer overflow", can_detect_struct_header_short_buffer_overflow);
  test_case(ctx, "can read struct header long version", can_read_struct_header_long_version);
  test_case(ctx, "can detect struct header long buffer overflow", can_detect_struct_header_long_buffer_overflow);
  test_case(ctx, "can detect struct header long zero delta", can_detect_struct_header_long_zero_delta);
  test_case(ctx, "can ignore struct content", can_ignore_struct_content);

  // binary cases
  test_case(ctx, "can read binary header", can_read_binary_header);
  test_case(ctx, "can detect binary header buffer overflow", can_detect_binary_header_buffer_overflow);
  test_case(ctx, "can read binary content", can_read_binary_content);
  test_case(ctx, "can read binary content buffer overflow", can_read_binary_content_buffer_overflow);

  // bool cases
  test_case(ctx, "can read bool", can_read_bool);
  test_case(ctx, "can detect bool buffer overflow", can_detect_bool_buffer_overflow);
  test_case(ctx, "can detect bool invalid value", can_detect_bool_invalid_value);

  // i8 cases
  test_case(ctx, "can read i8 positive", can_read_i8_positive);
  test_case(ctx, "can read i8 negative", can_read_i8_negative);
  test_case(ctx, "can detect i8 buffer overflow", can_detect_i8_buffer_overflow);

  // i32 cases
  test_case(ctx, "can read single-byte i32 positive", can_read_single_byte_i32_positive);
  test_case(ctx, "can read single-byte i32 negative", can_read_single_byte_i32_negative);
  test_case(ctx, "can read multiple bytes i32 positive", can_read_multiple_bytes_i32_positive);
  test_case(ctx, "can read multiple bytes i32 negative", can_read_multiple_bytes_i32_negative);
  test_case(ctx, "can handle min i32 value", can_handle_min_i32_value);
  test_case(ctx, "can handle max i32 value", can_handle_max_i32_value);
  test_case(ctx, "can ignore i32 value", can_ignore_i32_value);
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
}

static i64 thrift_dump_struct(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size);

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
  if (result < 0) return -1;

  // move the buffer pointer and size
  buffer += result;
  buffer_size -= result;

  // check if the buffer is large enough
  if (buffer_size < size) return -1;

  // print the binary content
  writef(", size=%d, ascii=%a", size, buffer, size);

  // success
  return result + size;
}

static const char *thrift_type_to_string(enum thrift_type type) {
  switch (type) {
    case THRIFT_TYPE_STOP:
      return "stop";
    case THRIFT_TYPE_BOOL_TRUE:
      return "bool-true";
    case THRIFT_TYPE_BOOL_FALSE:
      return "bool-false";
    case THRIFT_TYPE_I8:
      return "i8";
    case THRIFT_TYPE_I16:
      return "i16";
    case THRIFT_TYPE_I32:
      return "i32";
    case THRIFT_TYPE_I64:
      return "i64";
    case THRIFT_TYPE_DOUBLE:
      return "double";
    case THRIFT_TYPE_BINARY:
      return "binary";
    case THRIFT_TYPE_LIST:
      return "list";
    case THRIFT_TYPE_SET:
      return "set";
    case THRIFT_TYPE_MAP:
      return "map";
    case THRIFT_TYPE_STRUCT:
      return "struct";
    case THRIFT_TYPE_UUID:
      return "uuid";
    default:
      return "unknown";
  }
}

static i64 thrift_dump_list(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size) {
  u32 index;
  i64 result, read;

  struct thrift_list_header header;
  thrift_dump_field_fn dump_fn[THRIFT_TYPE_SIZE];

  // default
  read = 0;
  index = 0;

  // initialize the dump functions
  dump_fn[THRIFT_TYPE_STOP] = NULL;
  dump_fn[THRIFT_TYPE_BOOL_TRUE] = thrift_dump_bool;
  dump_fn[THRIFT_TYPE_BOOL_FALSE] = thrift_dump_bool;
  dump_fn[THRIFT_TYPE_I8] = thrift_dump_i8;
  dump_fn[THRIFT_TYPE_I16] = thrift_dump_i16;
  dump_fn[THRIFT_TYPE_I32] = thrift_dump_i32;
  dump_fn[THRIFT_TYPE_I64] = thrift_dump_i64;
  dump_fn[THRIFT_TYPE_DOUBLE] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_BINARY] = thrift_dump_binary;
  dump_fn[THRIFT_TYPE_LIST] = thrift_dump_list;
  dump_fn[THRIFT_TYPE_SET] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_MAP] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_STRUCT] = thrift_dump_struct;
  dump_fn[THRIFT_TYPE_UUID] = NULL; // not implemented

  // read the list header containing size and type
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return -1;

  // move the buffer pointer and size
  read += result;
  buffer += result;
  buffer_size -= result;

  // check if the dump function is out of range
  if (header.type >= THRIFT_TYPE_SIZE) return -1;

  // check if the dump function is available
  if (dump_fn[header.type] == NULL) return -1;

  ctx->indent++;
  writef(", size=%d, item-type=%s\n%ilist-start", header.size, thrift_type_to_string(header.type), ctx->indent);
  ctx->indent++;

  for (index = 0; index < header.size; index++) {
    writef("\n%iindex=%d, type=%s", ctx->indent, index, thrift_type_to_string(header.type));

    // read the list element content and print it
    result = dump_fn[header.type](ctx, buffer, buffer_size);
    if (result < 0) return -1;

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

static i64 thrift_dump_field(struct thrift_dump_context *ctx, enum thrift_type field_type, const char *buffer,
                             u64 buffer_size) {
  thrift_dump_field_fn dump_fn[THRIFT_TYPE_SIZE];

  dump_fn[THRIFT_TYPE_STOP] = NULL;
  dump_fn[THRIFT_TYPE_BOOL_TRUE] = thrift_dump_bool_true;
  dump_fn[THRIFT_TYPE_BOOL_FALSE] = thrift_dump_bool_false;
  dump_fn[THRIFT_TYPE_I8] = thrift_dump_i8;
  dump_fn[THRIFT_TYPE_I16] = thrift_dump_i16;
  dump_fn[THRIFT_TYPE_I32] = thrift_dump_i32;
  dump_fn[THRIFT_TYPE_I64] = thrift_dump_i64;
  dump_fn[THRIFT_TYPE_DOUBLE] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_BINARY] = thrift_dump_binary;
  dump_fn[THRIFT_TYPE_LIST] = thrift_dump_list;
  dump_fn[THRIFT_TYPE_SET] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_MAP] = NULL; // not implemented
  dump_fn[THRIFT_TYPE_STRUCT] = thrift_dump_struct;
  dump_fn[THRIFT_TYPE_UUID] = NULL; // not implemented

  // check if the field type is out of range
  if (field_type >= THRIFT_TYPE_SIZE) return -1;

  // check if the dump function is available
  if (dump_fn[field_type] == NULL) return -1;

  return dump_fn[field_type](ctx, buffer, buffer_size);
}

static i64 thrift_dump_struct(struct thrift_dump_context *ctx, const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct thrift_struct_header header;

  // default
  read = 0;

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

    result = thrift_dump_field(ctx, header.type, buffer, buffer_size);
    if (result < 0) return -1;

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

  // allocate memory for the input
  result = malloc(&pool, SIZE);
  if (result <= 0) return -1;

  buffer = (char *)result;
  buffer_size = SIZE;

  // initialize the context
  ctx.indent = 0;

  do {
    // read data from standard input
    result = stdin_read(buffer, buffer_size);
    if (result < 0) return -1;

    // advance the read pointer
    read += result;
    buffer += result;
    buffer_size -= result;
  } while (result > 0 && buffer_size > 0);

  if (buffer_size == 0) {
    writef("Error: Input buffer overflow.\n");
    return -1;
  }

  // rewind the buffer
  buffer -= read;
  buffer_size = read;
  read = 0;

  // dump the thrift struct
  result = thrift_dump_struct(&ctx, buffer, buffer_size);
  if (result < 0) return -1;

  writef("\n");

  // free the buffer
  free(&pool, buffer, SIZE);

  // clean up the memory pool
  malloc_destroy(&pool);

  // success
  return 0;
}
