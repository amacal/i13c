#include "thrift.h"
#include "runner.h"
#include "stdout.h"
#include "typing.h"

i64 thrift_read_struct(void *target, thrift_field *fields, u64 fields_size, const char *buffer, u64 buffer_size) {
  i64 result, read;
  u8 delta, type;
  u64 field_index;

  read = 0;
  field_index = 0;
  type = 1;

  while (buffer_size > 0 && type != FIELD_TYPE_STOP) {
    read += 1;
    type = buffer[0] & 0x0f;
    delta = (buffer[0] & 0xf0) >> 4;

    printf("type: %x, delta: %x\n", type, delta); // Debug output

    if (delta > 0) {
      field_index += delta;
    } else {
      printf("invalid delta %x\n", delta);
      return -1;
    }

    // check if the type is valid
    if (field_index >= fields_size) {
      printf("invalid field index %x, max %x\n", field_index, fields_size);
      return -1;
    }

    // call the field callback function
    result = fields[field_index](target, type, buffer + 1, buffer_size - 1);
    if (result < 0) return -1;

    // move the buffer pointer and size
    buffer = buffer + result + 1;
    buffer_size -= result + 1;
    read += result;

    printf("read %x bytes, field index %x, type %x\n", read, field_index, type); // Debug output
  }

  return read;
}

i64 thrift_read_i32(i32 *target, const char *buffer, u64 buffer_size) {
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

  // zigzag to i32
  *target = (i32)((value >> 1) ^ -(value & 1));

  // success
  return shift / 7;
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
  *target = (i64)((value >> 1) ^ -(value & 1));

  // success
  return shift / 7;
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
  // i32 cases
  test_case(ctx, "can read single-byte i32 positive", can_read_single_byte_i32_positive);
  test_case(ctx, "can read single-byte i32 negative", can_read_single_byte_i32_negative);
  test_case(ctx, "can read multiple bytes i32 positive", can_read_multiple_bytes_i32_positive);
  test_case(ctx, "can read multiple bytes i32 negative", can_read_multiple_bytes_i32_negative);
  test_case(ctx, "can handle min i32 value", can_handle_min_i32_value);
  test_case(ctx, "can handle max i32 value", can_handle_max_i32_value);
  test_case(ctx, "can detected i32 bits overflow", can_detect_i32_bits_overflow);
  test_case(ctx, "can detected i32 buffer overflow", can_detect_i32_buffer_overflow);

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
