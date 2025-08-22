#include "dom.h"
#include "stdout.h"
#include "typing.h"

/// @brief Function type for writing a DOM token to the stdout.
/// @param state Pointer to the DOM state passed between calls.
/// @param token Pointer to the DOM token to write.
typedef i64 (*dom_write_fn)(struct dom_state *state, struct dom_token *token);

// forward declarations
static i64 write_null(struct dom_state *state, struct dom_token *token);
static i64 write_unsigned(struct dom_state *state, struct dom_token *token);
static i64 write_signed(struct dom_state *state, struct dom_token *token);
static i64 write_text(struct dom_state *state, struct dom_token *token);
static i64 write_invalid(struct dom_state *state, struct dom_token *token);

// forward declarations
static i64 write_value(struct dom_state *state, struct dom_token *token);
static i64 write_array_start(struct dom_state *state, struct dom_token *token);
static i64 write_array_end(struct dom_state *state, struct dom_token *token);
static i64 write_index_start(struct dom_state *state, struct dom_token *token);
static i64 write_index_end(struct dom_state *state, struct dom_token *token);
static i64 write_struct_start(struct dom_state *state, struct dom_token *token);
static i64 write_struct_end(struct dom_state *state, struct dom_token *token);
static i64 write_key_start(struct dom_state *state, struct dom_token *token);
static i64 write_key_end(struct dom_state *state, struct dom_token *token);
static i64 write_value_start(struct dom_state *state, struct dom_token *token);
static i64 write_value_end(struct dom_state *state, struct dom_token *token);

// type to function mappings
static const dom_write_fn DOM_WRITE_VALUE_FN[DOM_TYPE_SIZE] = {
  [DOM_TYPE_NULL] = write_null,    [DOM_TYPE_I8] = write_signed,     [DOM_TYPE_I16] = write_signed,
  [DOM_TYPE_I32] = write_signed,   [DOM_TYPE_I64] = write_signed,    [DOM_TYPE_U8] = write_unsigned,
  [DOM_TYPE_U16] = write_unsigned, [DOM_TYPE_U32] = write_unsigned,  [DOM_TYPE_U64] = write_unsigned,
  [DOM_TYPE_TEXT] = write_text,    [DOM_TYPE_ARRAY] = write_invalid, [DOM_TYPE_STRUCT] = write_invalid,
};

// op to function mappings
static const dom_write_fn DOM_WRITE_OP_FN[DOM_OP_SIZE] = {
  [DOM_OP_LITERAL] = write_value,         [DOM_OP_ARRAY_START] = write_array_start,
  [DOM_OP_ARRAY_END] = write_array_end,   [DOM_OP_INDEX_START] = write_index_start,
  [DOM_OP_INDEX_END] = write_index_end,   [DOM_OP_STRUCT_START] = write_struct_start,
  [DOM_OP_STRUCT_END] = write_struct_end, [DOM_OP_KEY_START] = write_key_start,
  [DOM_OP_KEY_END] = write_key_end,       [DOM_OP_VALUE_START] = write_value_start,
  [DOM_OP_VALUE_END] = write_value_end,
};

static i64 write_null(struct dom_state *state, struct dom_token *) {
  // prepare the format string
  state->format.fmt = "%inull\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // format the null value
  return format(&state->format);
}

static i64 write_unsigned(struct dom_state *state, struct dom_token *token) {
  state->format.fmt = "%i%d\n";
  state->format.vargs[0] = (void *)(u64)(state->entries_indent + 1);
  state->format.vargs[1] = (void *)token->data;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_signed(struct dom_state *state, struct dom_token *token) {
  state->format.fmt = "%i%d\n";
  state->format.vargs[0] = (void *)(u64)(state->entries_indent + 1);
  state->format.vargs[1] = (void *)(u64)(i64)token->data;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_text(struct dom_state *state, struct dom_token *token) {
  u64 indent;
  const char *newline;

  // decide if indent it with a newline or to keep it as it is
  if (state->entries[state->entries_indent].op == DOM_OP_KEY_START) {
    indent = 0;
    newline = "";
  } else {
    indent = (u64)(state->entries_indent + 1);
    newline = "\n";
  }

  state->format.fmt = "%i%s%s";
  state->format.vargs[0] = (void *)indent;
  state->format.vargs[1] = (void *)token->data;
  state->format.vargs[2] = (void *)newline;
  state->format.vargs_offset = 0;

  return format(&state->format);
}

static i64 write_invalid(struct dom_state *, struct dom_token *) {
  // writting is not expected
  return DOM_ERROR_INVALID_TYPE;
}

static i64 write_value(struct dom_state *state, struct dom_token *token) {
  // check if the token type is valid
  if (token->type >= DOM_TYPE_ARRAY) {
    return DOM_ERROR_INVALID_TYPE;
  }

  // format the value
  return DOM_WRITE_VALUE_FN[token->type](state, token);
}

static i64 write_array_start(struct dom_state *state, struct dom_token *token) {
  // increase the indent
  state->entries_indent++;

  // prepare the format string
  state->format.fmt = "%iarray-start\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // fix up the next entry
  state->entries[state->entries_indent].op = DOM_OP_ARRAY_START;
  state->entries[state->entries_indent].type = token->type;
  state->entries[state->entries_indent].index = 0;

  // format the array start
  return format(&state->format);
}

static i64 write_array_end(struct dom_state *state, struct dom_token *) {
  // prepare the format string
  state->format.fmt = "%iarray-end\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // decrease the indent
  state->entries_indent--;

  // format the array end
  return format(&state->format);
}

static i64 write_index_start(struct dom_state *state, struct dom_token *token) {
  // increase the indent
  state->entries_indent++;

  // prepare the format string
  state->format.fmt = "%iindex-start, index=%d, type=%s\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs[1] = (void *)(u64)state->entries[state->entries_indent - 1].index;
  state->format.vargs[2] = (void *)(const char *)token->data;
  state->format.vargs_offset = 0;

  // fix up the next entry
  state->entries[state->entries_indent].op = DOM_OP_INDEX_START;
  state->entries[state->entries_indent].type = token->type;

  // update index counter
  state->entries[state->entries_indent - 1].index++;

  // format the array item
  return format(&state->format);
}

static i64 write_index_end(struct dom_state *state, struct dom_token *) {
  // prepare the format string
  state->format.fmt = "%iindex-end\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // decrease the indent
  state->entries_indent--;

  // format the array item
  return format(&state->format);
}

static i64 write_struct_start(struct dom_state *state, struct dom_token *token) {
  // increase the indent
  state->entries_indent++;

  // prepare the format string
  state->format.fmt = "%istruct-start, type=%s\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs[1] = (void *)(u64)token->data;
  state->format.vargs_offset = 0;

  // fix up the next entry
  state->entries[state->entries_indent].op = DOM_OP_STRUCT_START;
  state->entries[state->entries_indent].index = 0;

  // format the struct start
  return format(&state->format);
}

static i64 write_struct_end(struct dom_state *state, struct dom_token *) {
  // prepare the format string
  state->format.fmt = "%istruct-end\n";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // decrease the indent
  state->entries_indent--;

  // format the struct end
  return format(&state->format);
}

static i64 write_key_start(struct dom_state *state, struct dom_token *) {
  // increase the indent
  state->entries_indent++;

  // fix up the next entry
  state->entries[state->entries_indent].op = DOM_OP_KEY_START;

  // prepare the format string
  state->format.fmt = "%i";
  state->format.vargs[0] = (void *)(u64)state->entries_indent;
  state->format.vargs_offset = 0;

  // succeed
  return format(&state->format);
}

static i64 write_key_end(struct dom_state *state, struct dom_token *) {
  // decrease the indent
  state->entries_indent--;

  // succeed
  return 0;
}

static i64 write_value_start(struct dom_state *state, struct dom_token *token) {
  // increase the indent
  state->entries_indent++;

  // fix up the next entry
  state->entries[state->entries_indent].op = DOM_OP_VALUE_START;

  // prepare the format string
  state->format.fmt = ", type=%s\n";
  state->format.vargs[0] = (void *)(u64)token->data;
  state->format.vargs_offset = 0;

  // succeed
  return format(&state->format);
}

static i64 write_value_end(struct dom_state *state, struct dom_token *) {
  // decrease the indent
  state->entries_indent--;

  // succeed
  return 0;
}

void dom_init(struct dom_state *state, struct malloc_lease *buffer) {
  state->entries_indent = -1;
  state->buffer = buffer;
  state->format.fmt = NULL;
  state->format.buffer = buffer->ptr;
  state->format.buffer_offset = 0;
  state->format.buffer_size = buffer->size - 64;
  state->format.vargs = (void **)&state->vargs;
  state->format.vargs_offset = 0;
}

i64 dom_write(struct dom_state *state, struct dom_token *tokens, u32 *count) {
  u32 written;
  i64 result;

  // default
  result = 0;
  written = 0;

  // loop through the tokens until we reach the end
  while (written < *count) {
    // check if the operation is valid
    if (tokens->op >= DOM_OP_SIZE) {
      result = DOM_ERROR_INVALID_OP;
      break;
    }

    // delegate the call
    result = DOM_WRITE_OP_FN[tokens->op](state, tokens);
    if (result == FORMAT_ERROR_BUFFER_TOO_SMALL) {
      written++;
      break;
    }

    // by default fail
    if (result < 0) break;

    // next one
    tokens++;
    written++;
    result = 0;
  }

  // update the count
  *count = written;

  // success
  return result;
}

i64 dom_flush(struct dom_state *state) {
  i64 result;

  // call format to flush remaining formatting
  result = format(&state->format);
  if (result < 0) return result;

  // success
  return 0;
}

#if defined(I13C_TESTS)

static void can_write_array_with_no_items() {
  u32 size;
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[2];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 2;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_ARRAY_START;
  tokens[0].data = (u64)1;
  tokens[0].type = DOM_TYPE_U32;

  tokens[1].op = DOM_OP_ARRAY_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 2, "should consume all tokens");
  assert(state.format.buffer_offset == 22, "should write 22 bytes to the buffer");

  const char *expected = "array-start\n"
                         "array-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_write_array_with_one_item() {
  u32 size;
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[5];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 5;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_ARRAY_START;
  tokens[0].data = (u64)1;
  tokens[0].type = DOM_TYPE_U32;

  tokens[1].op = DOM_OP_INDEX_START;
  tokens[1].data = (u64) "u32";

  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_U32;
  tokens[2].data = (u64)1;

  tokens[3].op = DOM_OP_INDEX_END;
  tokens[4].op = DOM_OP_ARRAY_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 5, "should consume all tokens");
  assert(state.format.buffer_offset == 69, "should write 69 bytes to the buffer");

  const char *expected = "array-start\n"
                         " index-start, index=0, type=u32\n"
                         "  1\n"
                         " index-end\n"
                         "array-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_write_array_with_two_items() {
  u32 size;
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[8];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 8;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_ARRAY_START;
  tokens[0].data = (u64)2;
  tokens[0].type = DOM_TYPE_U32;

  tokens[1].op = DOM_OP_INDEX_START;
  tokens[1].data = (u64) "u16";

  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_U16;
  tokens[2].data = (u64)1;

  tokens[3].op = DOM_OP_INDEX_END;
  tokens[4].op = DOM_OP_INDEX_START;
  tokens[4].data = (u64) "u16";

  tokens[5].op = DOM_OP_LITERAL;
  tokens[5].type = DOM_TYPE_U16;
  tokens[5].data = (u64)2;

  tokens[6].op = DOM_OP_INDEX_END;
  tokens[7].op = DOM_OP_ARRAY_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 8, "should consume all tokens");
  assert(state.format.buffer_offset == 116, "should write 116 bytes to the buffer");

  const char *expected = "array-start\n"
                         " index-start, index=0, type=u16\n"
                         "  1\n"
                         " index-end\n"
                         " index-start, index=1, type=u16\n"
                         "  2\n"
                         " index-end\n"
                         "array-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_write_struct_with_no_fields() {
  u32 size;
  i64 result;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[2];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 2;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_STRUCT_START;
  tokens[0].data = (u64) "abc";

  tokens[1].op = DOM_OP_STRUCT_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 2, "should consume all tokens");
  assert(state.format.buffer_offset == 34, "should write 34 bytes to the buffer");

  const char *expected = "struct-start, type=abc\n"
                         "struct-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_write_struct_with_one_field() {
  i64 result;
  u32 size;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[8];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 8;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_STRUCT_START;
  tokens[0].data = (u64) "abc";

  tokens[1].op = DOM_OP_KEY_START;
  tokens[1].data = (u64) "text";
  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_TEXT;
  tokens[2].data = (u64) "key";
  tokens[3].op = DOM_OP_KEY_END;

  tokens[4].op = DOM_OP_VALUE_START;
  tokens[4].data = (u64) "i64";
  tokens[5].op = DOM_OP_LITERAL;
  tokens[5].type = DOM_TYPE_I64;
  tokens[5].data = (i64)-42;
  tokens[6].op = DOM_OP_VALUE_END;

  tokens[7].op = DOM_OP_STRUCT_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 8, "should consume all tokens");
  assert(state.format.buffer_offset == 55, "should write 55 bytes to the buffer");

  const char *expected = "struct-start, type=abc\n"
                         " key, type=i64\n"
                         "  -42\n"
                         "struct-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_write_struct_with_two_fields() {
  i64 result;
  u32 size;
  char buffer[256];

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[14];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 14;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_STRUCT_START;
  tokens[0].data = (u64) "abc";

  tokens[1].op = DOM_OP_KEY_START;
  tokens[1].data = (u64) "text";
  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_TEXT;
  tokens[2].data = (u64) "key1";
  tokens[3].op = DOM_OP_KEY_END;

  tokens[4].op = DOM_OP_VALUE_START;
  tokens[4].data = (u64) "i64";
  tokens[5].op = DOM_OP_LITERAL;
  tokens[5].type = DOM_TYPE_I64;
  tokens[5].data = (u64)(i64)-42;
  tokens[6].op = DOM_OP_VALUE_END;

  tokens[7].op = DOM_OP_KEY_START;
  tokens[7].data = (u64) "text";
  tokens[8].op = DOM_OP_LITERAL;
  tokens[8].type = DOM_TYPE_TEXT;
  tokens[8].data = (u64) "key2";
  tokens[9].op = DOM_OP_KEY_END;

  tokens[10].op = DOM_OP_VALUE_START;
  tokens[10].data = (u64) "text";
  tokens[11].op = DOM_OP_LITERAL;
  tokens[11].type = DOM_TYPE_TEXT;
  tokens[11].data = (u64) "abc";
  tokens[12].op = DOM_OP_VALUE_END;

  tokens[13].op = DOM_OP_STRUCT_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 14, "should consume all tokens");
  assert(state.format.buffer_offset == 79, "should write 76 bytes to the buffer");

  const char *expected = "struct-start, type=abc\n"
                         " key1, type=i64\n"
                         "  -42\n"
                         " key2, type=text\n"
                         "  abc\n"
                         "struct-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_resume_write_on_u64() {
  u32 size;
  i64 result;
  char buffer[128];
  const char *expected;

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[5];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 5;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_ARRAY_START;
  tokens[0].data = (u64)1;
  tokens[0].type = DOM_TYPE_U32;

  tokens[1].op = DOM_OP_INDEX_START;
  tokens[1].data = (u64) "u64";

  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_U64;
  tokens[2].data = (u64)12345;

  tokens[3].op = DOM_OP_INDEX_END;
  tokens[4].op = DOM_OP_ARRAY_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should fail");
  assert(size == 3, "should consume only three tokens");
  assert(state.format.buffer_offset == 46, "should write 46 bytes to the buffer");

  expected = "array-start\n"
             " index-start, index=0, type=u32\n"
             "  ";

  // reset the state
  size = 2;
  state.format.buffer_offset = 0;

  // flush the buffer
  result = dom_flush(&state);

  // assert the result
  assert(result == 0, "should succeed");
  assert(state.format.buffer_offset == 6, "should write 6 bytes to the buffer");

  // write the tokens
  result = dom_write(&state, tokens + 3, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 2, "should consume remaining two tokens");
  assert(state.format.buffer_offset == 27, "should write 27 bytes to the buffer");

  expected = "12345\n"
             " index-end\n"
             "array-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

static void can_resume_write_on_text() {
  u32 size;
  i64 result;
  char buffer[128];
  const char *expected;

  struct dom_state state;
  struct malloc_lease lease;
  struct dom_token tokens[5];

  // initialize the state
  lease.ptr = buffer;
  lease.size = sizeof(buffer);

  size = 5;
  dom_init(&state, &lease);

  // set up the tokens
  tokens[0].op = DOM_OP_ARRAY_START;
  tokens[0].data = (u64)1;
  tokens[0].type = DOM_TYPE_U32;

  tokens[1].op = DOM_OP_INDEX_START;
  tokens[1].data = (u64) "text";

  tokens[2].op = DOM_OP_LITERAL;
  tokens[2].type = DOM_TYPE_TEXT;
  tokens[2].data = (u64) "1234567890123456789012345678901234567890";

  tokens[3].op = DOM_OP_INDEX_END;
  tokens[4].op = DOM_OP_ARRAY_END;

  // write the tokens
  result = dom_write(&state, tokens, &size);

  // assert the result
  assert(result == FORMAT_ERROR_BUFFER_TOO_SMALL, "should fail");
  assert(size == 3, "should consume only three tokens");
  assert(state.format.buffer_offset == 64, "should write 64 bytes to the buffer");

  expected = "array-start\n"
             " index-start, index=0, type=text\n"
             "  12345678901234567";

  // reset the state
  size = 2;
  state.format.buffer_offset = 0;

  // flush the buffer
  result = dom_flush(&state);

  // assert the result
  assert(result == 0, "should succeed");
  assert(state.format.buffer_offset == 24, "should write 24 bytes to the buffer");

  // write the tokens
  result = dom_write(&state, tokens + 3, &size);

  // assert the result
  assert(result == 0, "should succeed");
  assert(size == 2, "should consume remaining two tokens");
  assert(state.format.buffer_offset == 45, "should write 45 bytes to the buffer");

  expected = "89012345678901234567890\n"
             " index-end\n"
             "array-end\n";

  assert_eq_str(buffer, expected, "should write exact text");
}

void dom_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can write array with no items", can_write_array_with_no_items);
  test_case(ctx, "can write array with one item", can_write_array_with_one_item);
  test_case(ctx, "can write array with two items", can_write_array_with_two_items);

  test_case(ctx, "can write struct with no fields", can_write_struct_with_no_fields);
  test_case(ctx, "can write struct with one field", can_write_struct_with_one_field);
  test_case(ctx, "can write struct with two fields", can_write_struct_with_two_fields);

  test_case(ctx, "can resume write on u64", can_resume_write_on_u64);
  test_case(ctx, "can resume write on text", can_resume_write_on_text);
}

#endif
