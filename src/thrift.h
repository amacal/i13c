#pragma once

#include "runner.h"
#include "typing.h"

enum thrift_struct_type {
  STRUCT_FIELD_STOP = 0,
  STRUCT_FIELD_BOOL_TRUE = 1,
  STRUCT_FIELD_BOOL_FALSE = 2,
  STRUCT_FIELD_I8 = 3,
  STRUCT_FIELD_I16 = 4,
  STRUCT_FIELD_I32 = 5,
  STRUCT_FIELD_I64 = 6,
  STRUCT_FIELD_DOUBLE = 7,
  STRUCT_FIELD_BINARY = 8,
  STRUCT_FIELD_LIST = 9,
  STRUCT_FIELD_SET = 10,
  STRUCT_FIELD_MAP = 11,
  STRUCT_FIELD_STRUCT = 12,
  STRUCT_FIELD_UUID = 13,
};

enum thrift_list_type {
  LIST_FIELD_STOP = 0,
  LIST_FIELD_BOOL_TRUE = 1,
  LIST_FIELD_BOOL_FALSE = 2,
  LIST_FIELD_I8 = 3,
  LIST_FIELD_I16 = 4,
  LIST_FIELD_I32 = 5,
  LIST_FIELD_I64 = 6,
  LIST_FIELD_DOUBLE = 7,
  LIST_FIELD_BINARY = 8,
  LIST_FIELD_LIST = 9,
  LIST_FIELD_SET = 10,
  LIST_FIELD_MAP = 11,
  LIST_FIELD_STRUCT = 12,
  LIST_FIELD_UUID = 13,

};

struct thrift_list_header {
  u32 size;                   // number of elements in the list
  enum thrift_list_type type; // type of the elements in the list
};

struct thrift_struct_header {
  u32 field;                    // index of the field in the struct
  enum thrift_struct_type type; // type of the struct
};

/// @brief Thrift field type callback function type.
/// @param target Pointer to the target struct.
/// @param field_type Type of the field to read.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_read_fn)(void *target, enum thrift_struct_type field_type, const char *buffer, u64 buffer_size);

/// @brief Reads a struct header from the buffer.
/// @param target Pointer to the target struct header.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_struct_header(struct thrift_struct_header *target, const char *buffer, u64 buffer_size);

/// @brief Ignores a field in the struct.
/// @param target Pointer to the target struct (unused).
/// @param field_type Type of the field to ignore.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_ignore_field(void *target, enum thrift_struct_type field_type, const char *buffer, u64 buffer_size);

/// @brief Reads a binary header from the buffer.
/// @param target Pointer to the target u32 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_binary_header(u32 *target, const char *buffer, u64 buffer_size);

/// @brief Reads binary content from the buffer.
/// @param target Pointer to the target buffer where the binary content will be copied.
/// @param size Size of the binary content to read.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_binary_content(char *target, u32 size, const char *buffer, u64 buffer_size);

/// @brief Reads a list header from the buffer.
/// @param target Pointer to the target list header structure.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_list_header(struct thrift_list_header *target, const char *buffer, u64 buffer_size);

/// @brief Reads an i8 value from the buffer.
/// @param target Pointer to the target i8 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_i8(i8 *target, const char *buffer, u64 buffer_size);

/// @brief Reads an u16 value from the buffer.
/// @param target Pointer to the target u16 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_u16(u16 *target, const char *buffer, u64 buffer_size);

/// @brief Reads an i16 value from the buffer.
/// @param target Pointer to the target i16 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_i16(i16 *target, const char *buffer, u64 buffer_size);

/// @brief Reads an u32 value from the buffer.
/// @param target Pointer to the target u32 variable.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size Size of the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
extern i64 thrift_read_u32(u32 *target, const char *buffer, u64 buffer_size);

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
