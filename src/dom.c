#include "dom.h"
#include "stdout.h"
#include "typing.h"

/// @brief Function type for writing a DOM token to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @param token Pointer to the DOM token to write.
typedef void (*dom_write_fn)(struct dom_state *state, struct dom_token *token);

static inline bool is_primitive(struct dom_token *token) {
  return token->type >= DOM_TOKEN_TYPE_I8 && token->type <= DOM_TOKEN_TYPE_U64;
}

static void write_unsigned(struct dom_state *state, struct dom_token *token) {
  writef("%d", (u64)token->value.number.value);
}

static void write_signed(struct dom_state *state, struct dom_token *token) {
  writef("%d", (i64)token->value.number.value);
}

static void write_text(struct dom_state *state, struct dom_token *token) {
  writef("%s", token->value.text_or_binary.buffer);
}

static void write_array_start(struct dom_state *state, struct dom_token *token) {
  // writef the representation
  writef("%iarray-start", state->indent);

  // decide how to writef each item
  if (is_primitive(token->value.array.item)) {
    writef(": [");
  } else {
    writef(", size=%d\n", token->value.array.size);
  }

  // fillup next entry
  state->entries[state->indent].type = token->type;
  state->entries[state->indent].size = token->value.array.size;
  state->entries[state->indent].index = 0;

  // increase the indent
  state->indent++;
}

static void write_array_end(struct dom_state *state, struct dom_token *token) {
  // decrease the indent
  state->indent--;

  // writef the representation
  writef("%iarray-end", state->indent);
}

static void write_struct_start(struct dom_state *state, struct dom_token *token) {
  // writef the representation
  writef("%i%s-start\n", state->indent, token->value.struct_or_key.buffer);

  // fillup next entry
  state->entries[state->indent].type = token->type;

  // increase the indent
  state->indent++;
}

static void write_struct_key(struct dom_state *state, struct dom_token *token) {
  writef("%s: ", token->value.struct_or_key.buffer);
}

static void write_struct_end(struct dom_state *state, struct dom_token *token) {
  // decrease the indent
  state->indent--;

  // writef the representation
  writef("%i%s-end\n", state->indent, token->value.struct_or_key.buffer);
}

static const dom_write_fn DOM_WRITE_FN[DOM_TOKEN_TYPE_SIZE] = {
  [DOM_TOKEN_TYPE_I8] = write_signed,
  [DOM_TOKEN_TYPE_I16] = write_signed,
  [DOM_TOKEN_TYPE_I32] = write_signed,
  [DOM_TOKEN_TYPE_I64] = write_signed,
  [DOM_TOKEN_TYPE_U8] = write_unsigned,
  [DOM_TOKEN_TYPE_U16] = write_unsigned,
  [DOM_TOKEN_TYPE_U32] = write_unsigned,
  [DOM_TOKEN_TYPE_U64] = write_unsigned,
  [DOM_TOKEN_TYPE_TEXT_OR_BINARY] = write_text,
  [DOM_TOKEN_TYPE_ARRAY_START] = write_array_start,
  [DOM_TOKEN_TYPE_ARRAY_END] = write_array_end,
  [DOM_TOKEN_TYPE_STRUCT_START] = write_struct_start,
  [DOM_TOKEN_TYPE_STRUCT_KEY] = write_struct_key,
  [DOM_TOKEN_TYPE_STRUCT_END] = write_struct_end,
};

i64 dom_write(struct dom_state *state, struct dom_token *tokens) {
  struct dom_token *current;

  // default
  current = tokens;

  // loop through the tokens until we reach the null terminator
  while (current->type != DOM_TOKEN_TYPE_NULL) {
    // check if the token type is valid
    if (current->type >= DOM_TOKEN_TYPE_SIZE) return DOM_ERROR_INVALID_TYPE;

    // delagate the writing to the appropriate function
    DOM_WRITE_FN[current->type](state, current);
    current++;
  }

  // success
  return (i64)(current - tokens + 1);
}
