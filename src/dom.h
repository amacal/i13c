#pragma once

#include "error.h"
#include "typing.h"

enum dom_error {
  // indicates that the token type is invalid
  DOM_ERROR_INVALID_TYPE = DOM_ERROR_BASE | 0x01,
};

enum dom_token_type {
  DOM_TOKEN_TYPE_NULL = 0,
  DOM_TOKEN_TYPE_I8 = 1,
  DOM_TOKEN_TYPE_I16 = 2,
  DOM_TOKEN_TYPE_I32 = 3,
  DOM_TOKEN_TYPE_I64 = 4,
  DOM_TOKEN_TYPE_U8 = 5,
  DOM_TOKEN_TYPE_U16 = 6,
  DOM_TOKEN_TYPE_U32 = 7,
  DOM_TOKEN_TYPE_U64 = 8,
  DOM_TOKEN_TYPE_TEXT_OR_BINARY = 9,
  DOM_TOKEN_TYPE_ARRAY_START = 10,
  DOM_TOKEN_TYPE_ARRAY_END = 11,
  DOM_TOKEN_TYPE_STRUCT_START = 12,
  DOM_TOKEN_TYPE_STRUCT_KEY = 13,
  DOM_TOKEN_TYPE_STRUCT_END = 14,
  DOM_TOKEN_TYPE_SIZE,
};

struct dom_token {
  u32 type; // type of the token

  union {
    struct {
      u64 size;
      u32 item;
    } array;

    struct {
      u64 value;
    } number;

    struct {
      const char *buffer; // null-terminated buffer
    } text_or_binary;

    struct {
      const char *buffer; // null-terminated buffer
    } struct_or_key;
  } value;
};

struct dom_state_entry {
  u32 type;  // type of the token
  u32 item;  // type of the array item
  u32 size;  // size of the array
  u32 index; // index within the array
};

struct dom_state {
  u32 indent;                         // current level
  struct dom_state_entry entries[16]; // stack of entries
};

/// @brief Write a DOM token stream to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @param tokens Null-terminated array of tokens to write.
/// @return The number of written tokens, or a negative error code.
extern i64 dom_write(struct dom_state *state, struct dom_token *tokens);
