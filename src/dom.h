#pragma once

#include "error.h"
#include "format.h"
#include "malloc.h"
#include "typing.h"
#include "vargs.h"

#define DOM_TOKENS_MAX 32
#define DOM_ENTRIES_MAX 16

enum dom_error {
  // indicates that the token type is invalid
  DOM_ERROR_INVALID_TYPE = DOM_ERROR_BASE - 0x01,

  // indicates that the token op is not expected
  DOM_ERROR_INVALID_OP = DOM_ERROR_BASE - 0x02,
};

enum dom_type {
  DOM_TYPE_NULL = 0x00,
  DOM_TYPE_I8 = 0x01,
  DOM_TYPE_I16 = 0x02,
  DOM_TYPE_I32 = 0x03,
  DOM_TYPE_I64 = 0x04,
  DOM_TYPE_U8 = 0x05,
  DOM_TYPE_U16 = 0x06,
  DOM_TYPE_U32 = 0x07,
  DOM_TYPE_U64 = 0x08,
  DOM_TYPE_TEXT = 0x09,
  DOM_TYPE_ARRAY = 0x0a,
  DOM_TYPE_STRUCT = 0x0b,
  DOM_TYPE_SIZE = 0x0c,
};

enum dom_op {
  DOM_OP_LITERAL = 0x00,
  DOM_OP_ARRAY_START = 0x01,
  DOM_OP_ARRAY_END = 0x02,
  DOM_OP_INDEX_START = 0x03,
  DOM_OP_INDEX_END = 0x04,
  DOM_OP_STRUCT_START = 0x05,
  DOM_OP_STRUCT_END = 0x06,
  DOM_OP_KEY_START = 0x07,
  DOM_OP_KEY_END = 0x08,
  DOM_OP_VALUE_START = 0x09,
  DOM_OP_VALUE_END = 0x0a,
  DOM_OP_SIZE = 0x0b,
};

struct dom_token {
  u8 op;    // required op
  u8 type;  // required type
  u64 data; // optional value
};

struct dom_state_entry {
  u8 op;     // type of the entry
  u8 type;   // type of the array item
  u16 index; // index within the array
};

struct dom_state {
  i8 entries_indent;      // entries depth level
  void *vargs[VARGS_MAX]; // variable arguments

  struct malloc_lease *buffer;                     // allocated output
  struct format_context format;                    // format context
  struct dom_state_entry entries[DOM_ENTRIES_MAX]; // stack of entries
};

/// @brief Initializes the DOM state.
/// @param state Pointer to the DOM state to initialize.
/// @param buffer Pointer to the buffer to use for writing.
extern void dom_init(struct dom_state *state, struct malloc_lease *buffer);

/// @brief Write a DOM token stream to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @param tokens Pointer to the array of DOM tokens to write.
/// @param count Pointer to the number of tokens to write and written.
/// @return Zero on success, or a negative error code.
extern i64 dom_write(struct dom_state *state, struct dom_token *tokens, u32 *count);

#if defined(I13C_TESTS)

/// @brief Registers dom test cases.
/// @param ctx Pointer to the runner_context structure.
extern void dom_test_cases(struct runner_context *ctx);

#endif
