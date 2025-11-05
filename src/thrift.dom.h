#pragma once

#include "dom.h"
#include "malloc.h"
#include "thrift.base.h"
#include "thrift.iter.h"
#include "typing.h"

enum thrift_dom_state_type {
  THRIFT_DOM_STATE_TYPE_INIT = 0,
  THRIFT_DOM_STATE_TYPE_STRUCT = 1,
  THRIFT_DOM_STATE_TYPE_ARRAY = 2,
  THRIFT_DOM_STATE_TYPE_KEY = 3,
  THRIFT_DOM_STATE_TYPE_VALUE = 4,
  THRIFT_DOM_STATE_TYPE_INDEX = 5,
  THRIFT_DOM_STATE_TYPE_LITERAL = 6,
  THRIFT_DOM_STATE_TYPE_BINARY = 7,
  THRIFT_DOM_STATE_TYPE_POINTER = 8,
  THRIFT_DOM_STATE_TYPE_MAYBE = 9,
  THRIFT_DOM_STATE_TYPE_SIZE
};

struct thrift_dom_state_init {
  u32 done;
};

struct thrift_dom_state_struct {
  u32 field;
};

struct thrift_dom_state_value {
  u32 type;
};

struct thrift_dom_state_binary {
  u32 done;
};

struct thrift_dom_state_array {
  u32 size;
};

struct thrift_dom_state_index {
  u32 offset;
};

struct thrift_dom_state_entry {
  union {
    struct thrift_dom_state_init init;
    struct thrift_dom_state_struct fields;
    struct thrift_dom_state_value value;
    struct thrift_dom_state_binary binary;
    struct thrift_dom_state_array array;
    struct thrift_dom_state_index index;
  } value;
};

struct thrift_dom_state {
  i16 idx;  // current index in the tokens array
  i16 size; // the number of tokens in the array

  u8 *types;                              // array of types of the entries
  struct thrift_dom_state_entry *entries; // array of state entries
};

struct thrift_dom {
  i16 idx;  // current index in the tokens array
  i16 size; // the number of tokens in the array

  struct thrift_dom_state state; // current state of the iterator
  struct malloc_lease *buffer;   // buffer holding both arrays
  struct dom_token *tokens;      // array of tokens in the buffer
};

/// @brief Initialize a Thrift DOM iterator.
/// @param iter Pointer to the thrift_dom structure to initialize.
/// @param buffer Pointer to the malloc_lease structure holding the buffer for tokens.
extern void thrift_dom_init(struct thrift_dom *iter, struct malloc_lease *buffer);

/// @brief Check if the iterator has finished processing all tokens.
/// @param iter Pointer to the thrift_dom structure.
/// @return True if the iterator is done, false otherwise.
extern bool thrift_dom_done(struct thrift_dom *iter);

/// @brief Fills the iterator with next tokens from the buffer.
/// @param iter Pointer to the thrift_dom structure.
/// @param tokens Pointer to the array of tokens.
/// @param entries Pointer to the array of entries.
/// @param size Size of the tokens/entries array.
/// @return Produced/Consumed on success, or a negative error code.
extern i64
thrift_dom_next(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size);

#if defined(I13C_TESTS)

/// @brief Registers thrift test cases.
/// @param ctx Pointer to the runner_context structure.
extern void thrift_test_cases_dom(struct runner_context *ctx);

#endif
