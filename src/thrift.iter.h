#pragma once

#include "malloc.h"
#include "runner.h"
#include "thrift.base.h"
#include "typing.h"

enum thrift_iter_token {
  THRIFT_ITER_TOKEN_BOOL = 1,
  THRIFT_ITER_TOKEN_I8 = 2,
  THRIFT_ITER_TOKEN_I16 = 3,
  THRIFT_ITER_TOKEN_I32 = 4,
  THRIFT_ITER_TOKEN_I64 = 5,
  THRIFT_ITER_TOKEN_BINARY_CHUNK = 6,
  THRIFT_ITER_TOKEN_BINARY_CONTENT = 7,
  THRIFT_ITER_TOKEN_LIST_HEADER = 8,
  THRIFT_ITER_TOKEN_STRUCT_FIELD = 9,
  THRIFT_ITER_TOKEN_SIZE,
};

struct thrift_iter_literal {
  union {
    bool v_bool;
    i8 v_i8;
    i16 v_i16;
    i32 v_i32;
    i64 v_i64;
  } value;
};

struct thrift_iter_chunk {
  u32 size;   // the size of the chunk, can be zero
  u32 offset; // the offset of the chunk in the binary data
};

struct thrift_iter_content {
  const char *ptr;
};

struct thrift_iter_field {
  u32 id;
  u32 type;
};

struct thrift_iter_list {
  u32 size;
  u32 type;
};

struct thrift_iter_entry {
  union {
    struct thrift_iter_literal literal;
    struct thrift_iter_chunk chunk;
    struct thrift_iter_content content;
    struct thrift_iter_field field;
    struct thrift_iter_list list;
  } value;
};

enum thrift_iter_state_type {
  THRIFT_ITER_STATE_TYPE_BINARY = 0,
  THRIFT_ITER_STATE_TYPE_STRUCT = 1,
  THRIFT_ITER_STATE_TYPE_LIST = 2,
  THRIFT_ITER_STATE_TYPE_LITERAL = 3,
  THRIFT_ITER_STATE_TYPE_SIZE,
};

struct thrift_iter_state_binary {
  u32 size;
  u32 read;
};

struct thrift_iter_state_struct {
  u32 field;
  u32 type;
};

struct thrift_iter_state_literal {
  u32 type;
  u32 done;
};

struct thrift_iter_state_list {
  u32 size;
  u32 type;
};

struct thrift_iter_state_entry {
  union {
    struct thrift_iter_state_binary binary;
    struct thrift_iter_state_struct fields;
    struct thrift_iter_state_literal literal;
    struct thrift_iter_state_list list;
  } value;
};

struct thrift_iter_state {
  i8 idx;  // current index in the types and entries arrays
  i8 size; // size of the types and entries arrays

  u8 *types;                               // array of types of the entries
  struct thrift_iter_state_entry *entries; // array of state entries
};

struct thrift_iter {
  i16 idx;    // current index in the entries array
  i16 size;   // size of the entries array
  u8 *tokens; // array of tokens in the buffer

  struct malloc_lease *buffer;       // buffer holding both arrays
  struct thrift_iter_entry *entries; // array of entries in the buffer
  struct thrift_iter_state state;    // current state of the iterator
};

/// @brief Initialize a Thrift iterator.
/// @param iter Pointer to the thrift_iter structure to initialize.
/// @param buffer Pointer to the malloc_lease structure holding the buffer for entries.
extern void thrift_iter_init(struct thrift_iter *iter, struct malloc_lease *buffer);

/// @brief Check if the iterator has finished processing all entries.
/// @param iter Pointer to the thrift_iter structure.
/// @return True if the iterator is done, false otherwise.
extern bool thrift_iter_done(struct thrift_iter *iter);

/// @brief Fills the iterator with next entries from the buffer.
/// @param iter Pointer to the thrift_iter structure.
/// @param buffer Pointer to the buffer containing the Thrift data.
/// @param buffer_size The number of bytes available in the buffer.
/// @return Produced/Consumed on success, or a negative error code.
extern i64 thrift_iter_next(struct thrift_iter *iter, const char *buffer, u64 buffer_size);

#if defined(I13C_TESTS)

/// @brief Registers thrift test cases.
/// @param ctx Pointer to the runner_context structure.
extern void thrift_test_cases_iter(struct runner_context *ctx);

#endif
