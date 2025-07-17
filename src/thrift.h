#pragma once

#include "runner.h"
#include "typing.h"

enum field_type {
  FIELD_TYPE_STOP = 0,
  FIELD_TYPE_BOOL_TRUE = 1,
  FIELD_TYPE_BOOL_FALSE = 2,
  FIELD_TYPE_BYTE = 3,
  FIELD_TYPE_I16 = 4,
  FIELD_TYPE_I32 = 5,
  FIELD_TYPE_I64 = 6,
  FIELD_TYPE_DOUBLE = 7,
  FIELD_TYPE_BINARY = 8,
  FIELD_TYPE_LIST = 9,
  FIELD_TYPE_SET = 10,
  FIELD_TYPE_MAP = 11,
  FIELD_TYPE_STRUCT = 12
};

/// @brief Thrift field type callback function type.
/// @param target Pointer to the target struct.
/// @param field_type Type of the field to read.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_field)(void *target, enum field_type field_type, const char *buffer, u64 buffer_size);

/// @brief Reads a struct from the buffer.
/// @param target Pointer to the target struct.
/// @param field Pointer to the thrift field array.
/// @param field_size Size of the fields array.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_struct(void *target, thrift_field *field, u64 field_size, const char *buffer, u64 buffer_size);

/// @brief Reads an i32 value from the buffer.
/// @param target Pointer to the target i32 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_i32(i32 *target, const char *buffer, u64 buffer_size);

/// @brief Reads an i64 value from the buffer.
/// @param target Pointer to the target i64 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_i64(i64 *target, const char *buffer, u64 buffer_size);

/// @brief Registers thrift test cases.
/// @param ctx Pointer to the runner_context structure.
extern void thrift_test_cases(struct runner_context *ctx);
