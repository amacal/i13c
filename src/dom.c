#include "dom.h"
#include "stdout.h"
#include "typing.h"

/// @brief Function type for writing a DOM token to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @param token Pointer to the DOM token to write.
typedef i64 (*dom_write_fn)(struct dom_state *state, struct dom_token *token);

static inline bool is_primitive(enum dom_token_type type) {
  return type >= DOM_TOKEN_TYPE_I8 && type <= DOM_TOKEN_TYPE_U64;
}

static i64 write_unsigned(struct dom_state *state, struct dom_token *token) {
  state->format.fmt = "%d";
  state->format.vargs[0] = (void *)(u64)token->value.number.value;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_signed(struct dom_state *state, struct dom_token *token) {
  state->format.fmt = "%d";
  state->format.vargs[0] = (void *)(i64)token->value.number.value;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_text_or_binary(struct dom_state *state, struct dom_token *token) {
  state->format.fmt = "%s";
  state->format.vargs[0] = (void *)token->value.text_or_binary.buffer;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_array_start(struct dom_state *state, struct dom_token *token) {
  // decide how to format each item
  if (is_primitive(token->value.array.type)) {
    state->format.fmt = "%iarray-start: [";
    state->format.vargs[0] = (void *)(u64)state->entries_indent;
    state->format.vargs_offset = 0;
  } else {
    state->format.fmt = "%iarray-start, size=%d\n";
    state->format.vargs[0] = (void *)(u64)state->entries_indent;
    state->format.vargs[1] = (void *)(u64)token->value.array.size;
    state->format.vargs_offset = 0;
  }

  // fix up the next entry
  state->entries[state->entries_indent].type = token->type;
  state->entries[state->entries_indent].size = token->value.array.size;
  state->entries[state->entries_indent].index = 0;

  // increase the indent
  state->entries_indent++;

  // format the array start
  return format(&state->format);
}

static void write_array_end(struct dom_state *state, struct dom_token *token) {
  // decrease the indent
  state->entries_indent--;

  // decide how to format each item
  if (is_primitive(token->value.array.type)) {
    state->format.fmt = "]";
    state->format.vargs_offset = 0;
  } else {
    state->format.fmt = "%iarray-end\n";
    state->format.vargs[0] = (void *)(u64)state->entries_indent;
    state->format.vargs_offset = 0;
  }

  // format the array end
  return format(&state->format);
}

static void write_struct_start(struct dom_state *state, struct dom_token *token) {
  // writef the representation
  writef("%i%s-start\n", state->entries_indent, token->value.struct_or_key.buffer);

  // fillup next entry
  state->entries[state->entries_indent].type = token->type;

  // increase the indent
  state->entries_indent++;
}

static void write_struct_key(struct dom_state *state, struct dom_token *token) {
  writef("%s: ", token->value.struct_or_key.buffer);
}

static void write_struct_end(struct dom_state *state, struct dom_token *token) {
  // decrease the indent
  state->entries_indent--;

  // writef the representation
  writef("%i%s-end\n", state->entries_indent, token->value.struct_or_key.buffer);
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
  [DOM_TOKEN_TYPE_TEXT_OR_BINARY] = write_text_or_binary,
  [DOM_TOKEN_TYPE_ARRAY_START] = write_array_start,
  [DOM_TOKEN_TYPE_ARRAY_END] = write_array_end,
  [DOM_TOKEN_TYPE_STRUCT_START] = write_struct_start,
  [DOM_TOKEN_TYPE_STRUCT_KEY] = write_struct_key,
  [DOM_TOKEN_TYPE_STRUCT_END] = write_struct_end,
};

void dom_init(struct dom_state *state, struct malloc_lease *buffer) {
  state->entries_indent = 0;
  state->tokens_head = 0;
  state->tokens_tail = 0;
  state->tokens_count = 0;
  state->buffer = buffer;
  state->format.fmt = NULL;
  state->format.buffer = buffer->ptr;
  state->format.buffer_offset = 0;
  state->format.buffer_size = buffer->size - 64;
  state->format.vargs = (void **)&state->vargs;
  state->format.vargs_offset = 0;
}

void dom_next(struct dom_state *state, struct dom_token **ptr, u8 *size) {
  // check if the ring is full
  if (state->tokens_count == DOM_TOKENS_MAX) {
    *ptr = NULL;
    *size = 0;
    return;
  }

  // we know the head points at the next free tokens
  *ptr = state->tokens + state->tokens_head;

  // decide which part of the ring to return
  if (state->tokens_head >= state->tokens_tail) {
    *size = DOM_TOKENS_MAX - state->tokens_head;
  } else {
    *size = state->tokens_tail - state->tokens_head;
  }
}

void dom_move(struct dom_state *state, u8 size) {
  state->tokens_head += size;
  state->tokens_head %= DOM_TOKENS_MAX;
  state->tokens_count += size;
}

i64 dom_write(struct dom_state *state) {
  i64 result, size;
  struct dom_token *current;

  // default
  size = state->tokens_count;
  current = state->tokens + state->tokens_tail;

  // loop through the tokens until we reach the null terminator
  while (state->tokens_count > 0) {

    // check if the token type is valid
    if (current->type >= DOM_TOKEN_TYPE_SIZE) return DOM_ERROR_INVALID_TYPE;

    // delagate the writing to the appropriate function
    result = DOM_WRITE_FN[current->type](state, current);
    if (result < 0) return result;

    // move tokens tail and count
    state->tokens_tail += 1;
    state->tokens_tail %= DOM_TOKENS_MAX;
    state->tokens_count--;

    // get newest current item
    current = state->tokens + state->tokens_tail;
  }

  // success
  return size - state->tokens_count;
}

#if defined(I13C_TESTS)

static void can_return_entire_ring() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  // initialize the state
  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size and the pointer
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens == state.tokens, "should return the entire ring");
}

static void can_return_right_half_ring() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  // initialize the state
  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // fill the first half of the ring
  dom_move(&state, 13);
  dom_next(&state, &tokens, &size);

  // assert the size and the pointer
  assert(size == DOM_TOKENS_MAX - 13, "should return the size of the right half of the ring");
  assert(tokens == state.tokens + 13, "should return the right half of the ring");
}

static void can_return_empty_ring() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  // initialize the state
  dom_init(&state, &lease);

  // fill the entire ring
  dom_move(&state, DOM_TOKENS_MAX);
  dom_next(&state, &tokens, &size);

  // assert the size and the pointer
  assert(size == 0, "should return the size of the empty ring");
  assert(tokens == NULL, "should return NULL for the empty ring");
}

static void can_return_left_half_ring() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  // initialize the state
  dom_init(&state, &lease);

  // fill the entire ring
  dom_move(&state, DOM_TOKENS_MAX);

  // consume 5 tokens
  state.tokens_tail += 5;
  state.tokens_count -= 5;

  // request next batch
  dom_next(&state, &tokens, &size);

  // assert the size and the pointer
  assert(size == 5, "should return the left half of the ring");
  assert(tokens == state.tokens, "should return the left half of the ring");
}

static void can_return_medium_part_ring() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  // initialize the state
  dom_init(&state, &lease);

  // fill the entire ring
  dom_move(&state, DOM_TOKENS_MAX);

  // consume 10 tokens
  state.tokens_tail += 10;
  state.tokens_count -= 10;

  // fill next 5 tokens
  dom_move(&state, 5);

  // request next batch
  dom_next(&state, &tokens, &size);

  // assert the size and the pointer
  assert(size == 5, "should return the medium part of the ring");
  assert(tokens == state.tokens + 5, "should return the medium part of the ring");
}

static void can_write_unsigned_integer() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_U32;
  tokens[0].value.number.value = 42;

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 2, "should write two bytes to the buffer");
  assert_eq_str(buffer, "42", "should write '42'");
}

static void can_postpone_unsigned_integer() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // simulate a buffer overflow
  state.format.buffer_offset = 250;

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_U32;
  tokens[0].value.number.value = 42;

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

static void can_write_text_or_binary() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_TEXT_OR_BINARY;
  tokens[0].value.text_or_binary.buffer = "Hello, world!";

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 13, "should write 13 bytes to the buffer");
  assert_eq_str(buffer, "Hello, world!", "should write 'Hello, world!'");
}

static void can_postpone_text_or_binary() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_TEXT_OR_BINARY;
  tokens[0].value.text_or_binary.buffer = "Hello, world!";

  // move the head
  dom_move(&state, 1);

  // simulate a buffer overflow
  state.format.buffer_offset = 250;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

static void can_write_array_start_primitive() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_START;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_I32;

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 14, "should write 14 bytes to the buffer");
  assert_eq_str(buffer, "array-start: [", "should write 'array-start: ['");
}

static void can_postpone_array_start_primitive() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_START;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_I32;

  // move the head
  dom_move(&state, 1);

  // simulate a buffer overflow
  state.format.buffer_offset = 250;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

static void can_write_array_start_complex() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_START;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_STRUCT_START;

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 20, "should write 20 bytes to the buffer");
  assert_eq_str(buffer, "array-start, size=1\n", "should write 'array-start, size=1\\n'");
}

static void can_postpone_array_start_complex() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_START;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_STRUCT_START;

  // move the head
  dom_move(&state, 1);

  // simulate a buffer overflow
  state.format.buffer_offset = 250;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

static void can_write_array_end_primitive() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_END;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_I32;

  // move the head
  dom_move(&state, 1);

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 1, "should write 1 byte to the buffer");
  assert_eq_str(buffer, "]", "should write 'array-end: ['");
}

static void can_postpone_array_end_primitive() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_END;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_I32;

  // move the head
  dom_move(&state, 1);

  // simulate a buffer overflow
  state.format.buffer_offset = 250;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

static void can_write_array_end_complex() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_END;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_STRUCT_START;

  // move the head
  dom_move(&state, 1);

  // simulate intendation
  state.entries_indent = 1;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == 1, "should write exactly one token");
  assert(state.format.buffer_offset == 10, "should write 10 bytes to the buffer");
  assert_eq_str(buffer, "array-end\n", "should write 'array-end\\n'");
}

static void can_postpone_array_end_complex() {
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;

  u8 size;
  struct dom_token *tokens;

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  dom_init(&state, &lease);
  dom_next(&state, &tokens, &size);

  // assert the size
  assert(size == DOM_TOKENS_MAX, "should return the size of the ring");
  assert(tokens != NULL, "should return a valid pointer to the tokens");

  // set up the tokens
  tokens[0].type = DOM_TOKEN_TYPE_ARRAY_END;
  tokens[0].value.array.size = 1;
  tokens[0].value.array.type = DOM_TOKEN_TYPE_STRUCT_START;

  // move the head
  dom_move(&state, 1);

  // simulate a buffer overflow
  state.entries_indent = 1;
  state.format.buffer_offset = 250;

  // write the tokens
  result = dom_write(&state);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should return buffer too small error");
  assert(state.format.buffer_offset == 250, "should not write to the buffer");
}

void dom_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can return entire ring", can_return_entire_ring);
  test_case(ctx, "can return right half of the ring", can_return_right_half_ring);
  test_case(ctx, "can return empty ring", can_return_empty_ring);
  test_case(ctx, "can return left half the ring", can_return_left_half_ring);
  test_case(ctx, "can return medium part the ring", can_return_medium_part_ring);

  test_case(ctx, "can write unsigned integer", can_write_unsigned_integer);
  test_case(ctx, "can postpone unsigned integer", can_postpone_unsigned_integer);

  test_case(ctx, "can write text or binary", can_write_text_or_binary);
  test_case(ctx, "can postpone text or binary", can_postpone_text_or_binary);

  test_case(ctx, "can write array start primitive", can_write_array_start_primitive);
  test_case(ctx, "can postpone array start primitive", can_postpone_array_start_primitive);
  test_case(ctx, "can write array start complex", can_write_array_start_complex);
  test_case(ctx, "can postpone array start complex", can_postpone_array_start_complex);

  test_case(ctx, "can write array end primitive", can_write_array_end_primitive);
  test_case(ctx, "can postpone array end primitive", can_postpone_array_end_primitive);
  test_case(ctx, "can write array end complex", can_write_array_end_complex);
  test_case(ctx, "can postpone array end complex", can_postpone_array_end_complex);
}

#endif
