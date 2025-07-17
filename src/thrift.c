#include "thrift.h"
#include "runner.h"
#include "stdout.h"
#include "typing.h"

#define STRUCT_FIELD_TYPE_SIZE (STRUCT_FIELD_UUID + 1)
#define LIST_FIELD_TYPE_SIZE (LIST_FIELD_UUID + 1)

typedef i64 (*thrift_ignore_field_fn)(const char *buffer, u64 buffer_size);

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

static i64 thrift_ignore_field_list(const char *buffer, u64 buffer_size) {
  i64 result, read;
  struct thrift_list_header header;

  // default
  read = 0;

  // read the list header containing size and type
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return -1;
  read += result;

  // read the list content, it will be ignored
  result = thrift_read_list_content(NULL, &header, buffer + read, buffer_size - read);
  if (result < 0) return -1;
  read += result;

  printf("Ignoring list content, read: %x\n", read); // Debug output
  return read;
}

static i64 thrift_ignore_field_struct(const char *buffer, u64 buffer_size) {
  return thrift_read_struct(NULL, NULL, 0, buffer, buffer_size);
}

i64 thrift_ignore_field(void *target, enum thrift_struct_type field_type, const char *buffer, u64 buffer_size) {
  thrift_ignore_field_fn ignore_fn[STRUCT_FIELD_TYPE_SIZE];

  ignore_fn[STRUCT_FIELD_STOP] = NULL;
  ignore_fn[STRUCT_FIELD_BOOL_TRUE] = NULL;
  ignore_fn[STRUCT_FIELD_BOOL_FALSE] = NULL;
  ignore_fn[STRUCT_FIELD_I8] = thrift_ignore_field_i8;
  ignore_fn[STRUCT_FIELD_I16] = thrift_ignore_field_i16;
  ignore_fn[STRUCT_FIELD_I32] = thrift_ignore_field_i32;
  ignore_fn[STRUCT_FIELD_I64] = thrift_ignore_field_i64;
  ignore_fn[STRUCT_FIELD_DOUBLE] = NULL;
  ignore_fn[STRUCT_FIELD_BINARY] = thrift_ignore_field_binary;
  ignore_fn[STRUCT_FIELD_LIST] = thrift_ignore_field_list;
  ignore_fn[STRUCT_FIELD_SET] = NULL;
  ignore_fn[STRUCT_FIELD_MAP] = NULL;
  ignore_fn[STRUCT_FIELD_STRUCT] = thrift_ignore_field_struct;

  if (ignore_fn[field_type]) {
    return ignore_fn[field_type](buffer, buffer_size);
  }

  printf("Unknown field type %x, cannot ignore\n", field_type); // Debug output
  return -1;
}

i64 thrift_read_struct(void *target, thrift_field *fields, u64 fields_size, const char *buffer, u64 buffer_size) {
  u64 index;
  i64 result, read;
  i16 delta, type;
  thrift_field field;

  read = 0;
  type = 1;
  index = 0;
  field = NULL;

  // iterate through the buffer until we reach the end
  while (buffer_size > 0) {
    type = *buffer & 0x0f;
    delta = (*buffer & 0xf0) >> 4;

    read += 1;
    buffer++;
    buffer_size--;

    printf("type: %x\n", type); // Debug output

    if (type == STRUCT_FIELD_STOP) {
      printf("Stopping at field type %x\n", buffer_size); // Debug output
      break;
    }

    if (type > STRUCT_FIELD_STRUCT) {
      printf("invalid field type %x\n", type);
      return -1;
    }

    if (delta == 0) {
      result = thrift_read_i16(&delta, buffer, buffer_size);
      if (result < 0) return -1;
      printf("delta: %x, index: %x\n", delta, index); // Debug output

      read += result;
      buffer += result;
      buffer_size -= result;
    }

    if (delta == 0) {
      printf("invalid delta %x\n", delta);
      return -1;
    }

    // check if the field index is zero
    index += delta;
    if (index <= 0) {
      printf("invalid field index %x\n", index);
      return -1;
    }

    printf("type: %x, field: %x, left: %x\n", type, index, buffer_size); // Debug output

    // check if the index is within bounds
    if (index < fields_size) {
      field = fields[index];
    }

    // if the field is not defined, use the ignore field function
    if (field == NULL) {
      field = thrift_ignore_field;
    }

    // check if the callback function is valid
    if (field == NULL) {
      return -1;
    }

    // call the field callback function
    result = field(target, type, buffer, buffer_size);
    if (result < 0) return -1;

    // move the buffer pointer and size
    buffer += result;
    buffer_size -= result;
    read += result;
  }

  return read;
}

i64 thrift_read_binary_header(u32 *target, const char *buffer, u64 buffer_size) {
  i64 result;

  // read the size of the binary data
  result = thrift_read_u32(target, buffer, buffer_size);
  if (result < 0) return -1;

  // success
  return result;
}

i64 thrift_read_binary_content(char **target, u32 size, const char *buffer, u64 buffer_size) {
  // check if the buffer is large enough
  if (buffer_size < size) return -1;

  // reference the buffer as the target
  if (target) *target = buffer;

  // success
  return size;
}

i64 thrift_read_list_header(struct thrift_list_header *target, const char *buffer, u64 buffer_size) {
  i64 result, read;
  u32 size, type;

  read = 0;

  if (buffer_size > 0) {
    read += 1;
    type = *buffer & 0x0f;
    size = (*buffer & 0xf0) >> 4;

    if (size == 0x0f) {
      result = thrift_read_u32(&size, buffer + 1, buffer_size - 1);
      if (result < 0) return -1;

      read += result;
    }

    target->type = type;
    target->size = size;

    return read;
  }

  return -1;
}

i64 thrift_read_list_content(void *target, struct thrift_list_header *header, const char *buffer, u64 buffer_size) {
  u32 index;
  i64 result;
  u64 read;

  thrift_ignore_field_fn ignore_fn[LIST_FIELD_TYPE_SIZE];

  ignore_fn[LIST_FIELD_I8] = thrift_ignore_field_i8;
  ignore_fn[LIST_FIELD_I16] = thrift_ignore_field_i16;
  ignore_fn[LIST_FIELD_I32] = thrift_ignore_field_i32;
  ignore_fn[LIST_FIELD_I64] = thrift_ignore_field_i64;
  ignore_fn[LIST_FIELD_BINARY] = thrift_ignore_field_binary;
  ignore_fn[LIST_FIELD_STRUCT] = thrift_ignore_field_struct;

  read = 0;
  printf("Reading %x elements of type %x, left: %x\n", header->size, header->type, buffer_size);

  for (index = 0; index < header->size; index++) {
    if (header->type >= sizeof(ignore_fn)) return -1;
    if (ignore_fn[header->type] == NULL) return -1;

    printf("Reading %x at index %x, left: %x\n", header->type, index, buffer_size);
    result = ignore_fn[header->type](buffer, buffer_size);
    if (result < 0) return -1;

    buffer += result;
    buffer_size -= result;
    read += result;
  }

  return read;
}

i64 thrift_read_i8(i8 *target, const char *buffer, u64 buffer_size) {
  // check if the buffer is large enough
  if (buffer_size < 1) return -1;

  // read the byte
  if (target) *target = (i8)*buffer;

  // success
  return 1;
}

i64 thrift_read_i16(i16 *target, const char *buffer, u64 buffer_size) {
  i64 result;
  u32 value;

  // read the unsigned 32-bit value
  result = thrift_read_u32(&value, buffer, buffer_size);
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
    return -1;
  }

  // check for the last byte overflow
  if (shift >= 28 && (next & 0xf0)) {
    return -1;
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
    return -1;
  }

  // check for the last byte overflow
  if (shift >= 56 && (next & 0xf0)) {
    return -1;
  }

  // zigzag to i64
  if (target) *target = (i64)((value >> 1) ^ -(value & 1));

  // success
  return shift / 7;
}

static void can_read_list_header_in_short_version() {
  struct thrift_list_header header;
  const char buffer[] = {0x38};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 1, "should read one byte");
  assert(header.size == 3, "should read size 3");
  assert(header.type == LIST_FIELD_I32, "should read type LIST_FIELD_I32");
}

static void can_read_list_header_in_long_version() {
  struct thrift_list_header header;
  const char buffer[] = {0xf8, 0x0f};

  // read the list header from the buffer
  i64 result = thrift_read_list_header(&header, buffer, sizeof(buffer));

  // assert the result
  assert(result == 2, "should read two bytes");
  assert(header.size == 15, "should read size 15");
  assert(header.type == LIST_FIELD_I32, "should read type LIST_FIELD_I32");
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

static void can_detect_i32_bits_overflow() {
  i32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0x10};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == -1, "should fail with -1");
}

static void can_detect_i32_buffer_overflow() {
  i32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff};

  // read the i32 value from the buffer
  i64 result = thrift_read_i32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == -1, "should fail with -1");
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
  assert(result == -1, "should fail with -1");
}

static void can_detect_u32_buffer_overflow() {
  u32 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff};

  // read the u32 value from the buffer
  i64 result = thrift_read_u32(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == -1, "should fail with -1");
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
  assert(result == -1, "should fail with -1");
}

static void can_detect_i64_buffer_overflow() {
  i64 value;
  const char buffer[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

  // read the i64 value from the buffer
  i64 result = thrift_read_i64(&value, buffer, sizeof(buffer));

  // assert the result
  assert(result == -1, "should fail with -1");
}

void thrift_test_cases(struct runner_context *ctx) {
  // list cases
  test_case(ctx, "can read list header in short version", can_read_list_header_in_short_version);
  test_case(ctx, "can read list header in long version", can_read_list_header_in_long_version);

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
}
