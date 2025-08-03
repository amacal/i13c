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
      u32 type;
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
  u8 entries_indent; // entries depth level
  u8 tokens_head;    // head of the ring
  u8 tokens_tail;    // tail of the ring
  u8 tokens_count;   // the count of tokens

  void *vargs[VARGS_MAX];       // variable arguments for formatting
  struct malloc_lease *buffer;  // allocated buffer for writing
  struct format_context format; // format context for writing

  struct dom_token tokens[DOM_TOKENS_MAX];         // ring of tokens tokens
  struct dom_state_entry entries[DOM_ENTRIES_MAX]; // stack of entries
};

/// @brief Initializes the DOM state.
/// @param state Pointer to the DOM state to initialize.
/// @param buffer Pointer to the buffer to use for writing.
extern void dom_init(struct dom_state *state, struct malloc_lease *buffer);

/// @brief Determines the size and the place for new batch of tokens.
/// @param state Pointer to the DOM state.
/// @param ptr Pointer to the returned location of the next token batch.
/// @param size Pointer to the size of the next token batch.
extern void dom_next(struct dom_state *state, struct dom_token **ptr, u8 *size);

/// @brief Advances the head of the tokens ring.
/// @param state Pointer to the DOM state.
/// @param size Number of tokens to advance the head by.
extern void dom_move(struct dom_state *state, u8 size);

/// @brief Write a DOM token stream to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @return The number of written tokens, or a negative error code.
extern i64 dom_write(struct dom_state *state);

#if defined(I13C_TESTS)

/// @brief Registers dom test cases.
/// @param ctx Pointer to the runner_context structure.
extern void dom_test_cases(struct runner_context *ctx);

#endif
