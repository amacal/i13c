#include "thrift.dom.h"
#include "malloc.h"
#include "runner.h"
#include "thrift.base.h"

#define THRIFT_DOM_STATE_INITIAL_SIZE 16

#define PRODUCED(res) ((u32)((res) & 0xFFFFFFFFu))
#define CONSUMED(res) ((u32)(((res) >> 32) & 0xFFFFFFFFu))
#define COMBINE(l, r) ((i64)(((u64)(l) << 32) | (u64)(r)))

/// @brief Function type for reading the next element.
/// @param iter Pointer to the Thrift iterator.
/// @param tokens Pointer to the array of tokens.
/// @param entries Pointer to the array of entries.
/// @param size Size of the tokens/entries array.
/// @return The number of tokens read from the stream, or a negative error code.
typedef i64 (*thrift_iter_next_fn)(struct thrift_dom *iter,
                                   const u8 *tokens,
                                   const struct thrift_iter_entry *entries,
                                   u64 size);

/// @brief Function type for folding the iterator state.
/// @param entry Pointer to the state entry to fold.
/// @return True if the state can be folded, false otherwise.
typedef bool (*thrift_iter_fold_fn)(struct thrift_dom_state_entry *entry);

/// @brief Function type for reading a literal value.
/// @param target Pointer to the target Thrift DOM token.
/// @param source Pointer to the Thrift iterator entry.
/// @return Zero on success, or a negative error code.
typedef i64 (*thrift_iter_literal_fn)(struct dom_token *target, const struct thrift_iter_entry *source);

// forward declarations
static i64 thrift_next_init(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_struct(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_array(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_key(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_value(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_index(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_literal(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_binary(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);
static i64 thrift_next_pointer(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64);

// forward declarations
static i64 thrift_literal_bool(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_i8(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_i16(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_i32(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_i64(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_list(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_struct(struct dom_token *, const struct thrift_iter_entry *);
static i64 thrift_literal_fail(struct dom_token *, const struct thrift_iter_entry *);

// forward declarations
static bool thrift_fold_init(struct thrift_dom_state_entry *);
static bool thrift_fold_struct(struct thrift_dom_state_entry *);
static bool thrift_fold_array(struct thrift_dom_state_entry *);
static bool thrift_fold_key(struct thrift_dom_state_entry *);
static bool thrift_fold_value(struct thrift_dom_state_entry *);
static bool thrift_fold_index(struct thrift_dom_state_entry *);
static bool thrift_fold_literal(struct thrift_dom_state_entry *);
static bool thrift_fold_binary(struct thrift_dom_state_entry *);
static bool thrift_fold_pointer(struct thrift_dom_state_entry *);

// fold dispatch table
static const thrift_iter_fold_fn FOLD_FN[THRIFT_DOM_STATE_TYPE_SIZE] = {
  [THRIFT_DOM_STATE_TYPE_INIT] = thrift_fold_init,       [THRIFT_DOM_STATE_TYPE_STRUCT] = thrift_fold_struct,
  [THRIFT_DOM_STATE_TYPE_ARRAY] = thrift_fold_array,     [THRIFT_DOM_STATE_TYPE_KEY] = thrift_fold_key,
  [THRIFT_DOM_STATE_TYPE_VALUE] = thrift_fold_value,     [THRIFT_DOM_STATE_TYPE_INDEX] = thrift_fold_index,
  [THRIFT_DOM_STATE_TYPE_LITERAL] = thrift_fold_literal, [THRIFT_DOM_STATE_TYPE_BINARY] = thrift_fold_binary,
  [THRIFT_DOM_STATE_TYPE_POINTER] = thrift_fold_pointer,
};

// next dispatch table
static const thrift_iter_next_fn NEXT_FN[THRIFT_DOM_STATE_TYPE_SIZE] = {
  [THRIFT_DOM_STATE_TYPE_INIT] = thrift_next_init,       [THRIFT_DOM_STATE_TYPE_STRUCT] = thrift_next_struct,
  [THRIFT_DOM_STATE_TYPE_ARRAY] = thrift_next_array,     [THRIFT_DOM_STATE_TYPE_KEY] = thrift_next_key,
  [THRIFT_DOM_STATE_TYPE_VALUE] = thrift_next_value,     [THRIFT_DOM_STATE_TYPE_INDEX] = thrift_next_index,
  [THRIFT_DOM_STATE_TYPE_LITERAL] = thrift_next_literal, [THRIFT_DOM_STATE_TYPE_BINARY] = thrift_next_binary,
  [THRIFT_DOM_STATE_TYPE_POINTER] = thrift_next_pointer,
};

static const thrift_iter_literal_fn LITERAL_FN[THRIFT_ITER_TOKEN_SIZE] = {
  [THRIFT_ITER_TOKEN_BOOL] = thrift_literal_bool,
  [THRIFT_ITER_TOKEN_I8] = thrift_literal_i8,
  [THRIFT_ITER_TOKEN_I16] = thrift_literal_i16,
  [THRIFT_ITER_TOKEN_I32] = thrift_literal_i32,
  [THRIFT_ITER_TOKEN_I64] = thrift_literal_i64,
  [THRIFT_ITER_TOKEN_BINARY_CHUNK] = thrift_literal_fail,
  [THRIFT_ITER_TOKEN_BINARY_CONTENT] = thrift_literal_fail,
  [THRIFT_ITER_TOKEN_LIST_HEADER] = thrift_literal_list,
  [THRIFT_ITER_TOKEN_STRUCT_FIELD] = thrift_literal_struct,
};

// type mapping
static const u8 TYPE_MAPPING[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = DOM_TYPE_NULL,       [THRIFT_TYPE_BOOL_TRUE] = DOM_TYPE_TEXT,
  [THRIFT_TYPE_BOOL_FALSE] = DOM_TYPE_TEXT, [THRIFT_TYPE_I8] = DOM_TYPE_I8,
  [THRIFT_TYPE_I16] = DOM_TYPE_I16,         [THRIFT_TYPE_I32] = DOM_TYPE_I32,
  [THRIFT_TYPE_I64] = DOM_TYPE_I64,         [THRIFT_TYPE_DOUBLE] = DOM_TYPE_NULL,
  [THRIFT_TYPE_BINARY] = DOM_TYPE_TEXT,     [THRIFT_TYPE_LIST] = DOM_TYPE_STRUCT,
  [THRIFT_TYPE_SET] = DOM_TYPE_NULL,        [THRIFT_TYPE_MAP] = DOM_TYPE_NULL,
  [THRIFT_TYPE_STRUCT] = DOM_TYPE_STRUCT,   [THRIFT_TYPE_UUID] = DOM_TYPE_NULL,
};

// type names
static const char *const TYPE_NAMES[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = "stop",     [THRIFT_TYPE_BOOL_TRUE] = "bool", [THRIFT_TYPE_BOOL_FALSE] = "bool",
  [THRIFT_TYPE_I8] = "i8",         [THRIFT_TYPE_I16] = "i16",        [THRIFT_TYPE_I32] = "i32",
  [THRIFT_TYPE_I64] = "i64",       [THRIFT_TYPE_DOUBLE] = "double",  [THRIFT_TYPE_BINARY] = "binary",
  [THRIFT_TYPE_LIST] = "list",     [THRIFT_TYPE_SET] = NULL,         [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = "struct", [THRIFT_TYPE_UUID] = NULL,
};

// token mapping
static const u8 TOKEN_MAPPING[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = THRIFT_ITER_TOKEN_SIZE,
  [THRIFT_TYPE_BOOL_TRUE] = THRIFT_ITER_TOKEN_BOOL,
  [THRIFT_TYPE_BOOL_FALSE] = THRIFT_ITER_TOKEN_BOOL,
  [THRIFT_TYPE_I8] = THRIFT_ITER_TOKEN_I8,
  [THRIFT_TYPE_I16] = THRIFT_ITER_TOKEN_I16,
  [THRIFT_TYPE_I32] = THRIFT_ITER_TOKEN_I32,
  [THRIFT_TYPE_I64] = THRIFT_ITER_TOKEN_I64,
  [THRIFT_TYPE_DOUBLE] = THRIFT_ITER_TOKEN_SIZE,
  [THRIFT_TYPE_BINARY] = THRIFT_ITER_TOKEN_BINARY_CHUNK,
  [THRIFT_TYPE_LIST] = THRIFT_ITER_TOKEN_LIST_HEADER,
  [THRIFT_TYPE_SET] = THRIFT_ITER_TOKEN_SIZE,
  [THRIFT_TYPE_MAP] = THRIFT_ITER_TOKEN_SIZE,
  [THRIFT_TYPE_STRUCT] = THRIFT_ITER_TOKEN_STRUCT_FIELD,
  [THRIFT_TYPE_UUID] = THRIFT_ITER_TOKEN_SIZE,
};

static i64 thrift_next_init(struct thrift_dom *iter, const u8 *, const struct thrift_iter_entry *, u64) {
  // emit STRUCT_START token
  iter->tokens[0].op = DOM_OP_STRUCT_START;
  iter->tokens[0].data = 0;

  // advance the iterator
  iter->idx++;

  // previous init state is done
  iter->state.entries[iter->state.idx].value.init.done = TRUE;

  // advance the state
  iter->state.idx++;

  // set the new state
  iter->state.types[iter->state.idx] = THRIFT_DOM_STATE_TYPE_STRUCT;
  iter->state.entries[iter->state.idx].value.fields.field = 0xffffffff;

  // success
  return 0;
}

static i64
thrift_next_struct(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  // check for size
  if (size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // check for STRUCT_FIELD token
  if (tokens[0] != THRIFT_ITER_TOKEN_STRUCT_FIELD) {
    return THRIFT_ERROR_INVALID_IMPLEMENTATION;
  }

  // update the state
  iter->state.entries[iter->state.idx].value.fields.field = entries[0].value.field.id;

  // check for STOP field
  if (entries[0].value.field.type == THRIFT_TYPE_STOP) {

    // emit STRUCT_END token
    iter->tokens[iter->idx].op = DOM_OP_STRUCT_END;
    iter->tokens[iter->idx].data = 0;

    // advance the iterator
    iter->idx++;

    return 1;
  }

  // emit KEY_START token
  iter->tokens[iter->idx].op = DOM_OP_KEY_START;
  iter->tokens[iter->idx].data = (u64)TYPE_NAMES[entries[0].value.field.type];
  iter->tokens[iter->idx].type = DOM_TYPE_I32;

  // advance the iterator
  iter->idx++;

  // emit LITERAL token
  iter->tokens[iter->idx].op = DOM_OP_LITERAL;
  iter->tokens[iter->idx].data = entries[0].value.field.id;
  iter->tokens[iter->idx].type = DOM_TYPE_I32;

  // advance the iterator
  iter->idx++;

  // emit KEY_END token
  iter->tokens[iter->idx].op = DOM_OP_KEY_END;
  iter->tokens[iter->idx].data = 0;

  // advance the iterator
  iter->idx++;

  // advance the state
  iter->state.idx++;

  // binary types have to be handled differently
  if (entries[0].value.field.type == THRIFT_TYPE_BINARY) {
    iter->state.types[iter->state.idx] = THRIFT_DOM_STATE_TYPE_BINARY;
    iter->state.entries[iter->state.idx].value.binary.done = FALSE;
  } else {
    iter->state.types[iter->state.idx] = THRIFT_DOM_STATE_TYPE_VALUE;
    iter->state.entries[iter->state.idx].value.value.type = entries[0].value.field.type;
  }

  // success
  return 1;
}

static i64 thrift_next_array(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64) {
  return -1;
}

static i64 thrift_next_key(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64) {
  return -1;
}

static i64
thrift_next_value(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  i64 result;
  u32 type, token;

  // check for size
  if (size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // get the type and the token
  type = iter->state.entries[iter->state.idx].value.value.type;
  token = TOKEN_MAPPING[type];

  // check for the expected token
  if (*tokens != token) {
    return THRIFT_ERROR_INVALID_IMPLEMENTATION;
  }

  // emit VALUE_START token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_START;
  iter->tokens[iter->idx].data = 0;
  iter->tokens[iter->idx].type = TYPE_MAPPING[type];

  // advance the iterator
  iter->idx++;

  // emit LITERAL token
  iter->tokens[iter->idx].op = DOM_OP_LITERAL;
  iter->tokens[iter->idx].type = TYPE_MAPPING[type];

  // fill the literal value
  result = LITERAL_FN[token](iter->tokens + iter->idx, entries);
  if (result < 0) return result;

  // advance the iterator
  iter->idx++;

  // emit VALUE_END token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_END;
  iter->tokens[iter->idx].data = 0;

  // advance the iterator
  iter->idx++;

  // complete the value state
  iter->state.entries[iter->state.idx].value.value.type = 0;

  // success
  return 1;
}

static i64 thrift_next_index(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64) {
  return -1;
}

static i64 thrift_next_literal(struct thrift_dom *, const u8 *, const struct thrift_iter_entry *, u64) {
  return -1;
}

static i64 thrift_next_binary(struct thrift_dom *iter, const u8 *, const struct thrift_iter_entry *entries, u64 size) {
  // check for size
  if (size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // emit VALUE_START token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_START;
  iter->tokens[iter->idx].data = entries->value.chunk.size;
  iter->tokens[iter->idx].type = TYPE_MAPPING[THRIFT_TYPE_BINARY];

  // advance the iterator
  iter->idx++;

  // complete the binary state
  iter->state.entries[iter->state.idx].value.binary.done = TRUE;

  // advance the state
  iter->state.idx++;

  // set the new state
  iter->state.types[iter->state.idx] = THRIFT_DOM_STATE_TYPE_POINTER;
  iter->state.entries[iter->state.idx].value.pointer.done = FALSE;

  // success
  return 1;
}

static i64 thrift_next_pointer(struct thrift_dom *iter, const u8 *, const struct thrift_iter_entry *entries, u64 size) {

  // check for size
  if (size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // emit LITERAL token
  iter->tokens[iter->idx].op = DOM_OP_LITERAL;
  iter->tokens[iter->idx].data = (u64)entries->value.content.ptr;
  iter->tokens[iter->idx].type = TYPE_MAPPING[THRIFT_TYPE_BINARY];

  // advance the iterator
  iter->idx++;

  // emit VALUE_END token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_END;
  iter->tokens[iter->idx].data = 0;

  // advance the iterator
  iter->idx++;

  // complete the value state
  iter->state.entries[iter->state.idx].value.pointer.done = TRUE;

  // success
  return 1;
}

static i64 thrift_literal_bool(struct dom_token *target, const struct thrift_iter_entry *source) {
  target->data = source->value.literal.value.v_bool ? (u64) "true" : (u64) "false";
  return 0;
}

static i64 thrift_literal_i8(struct dom_token *target, const struct thrift_iter_entry *source) {
  target->data = (u64)source->value.literal.value.v_i8;
  return 0;
}

static i64 thrift_literal_i16(struct dom_token *target, const struct thrift_iter_entry *source) {
  target->data = (u64)source->value.literal.value.v_i16;
  return 0;
}

static i64 thrift_literal_i32(struct dom_token *target, const struct thrift_iter_entry *source) {
  target->data = (u64)source->value.literal.value.v_i32;
  return 0;
}

static i64 thrift_literal_i64(struct dom_token *target, const struct thrift_iter_entry *source) {
  target->data = (u64)source->value.literal.value.v_i64;
  return 0;
}

static i64 thrift_literal_list(struct dom_token *, const struct thrift_iter_entry *) {
  return 0;
}

static i64 thrift_literal_struct(struct dom_token *, const struct thrift_iter_entry *) {
  return 0;
}

static i64 thrift_literal_fail(struct dom_token *, const struct thrift_iter_entry *) {
  return THRIFT_ERROR_INVALID_IMPLEMENTATION;
}

static bool thrift_fold_init(struct thrift_dom_state_entry *entry) {
  return entry->value.init.done == TRUE;
}

static bool thrift_fold_struct(struct thrift_dom_state_entry *entry) {
  return entry->value.fields.field == 0;
}

static bool thrift_fold_array(struct thrift_dom_state_entry *) {
  return FALSE;
}

static bool thrift_fold_key(struct thrift_dom_state_entry *) {
  return FALSE;
}

static bool thrift_fold_value(struct thrift_dom_state_entry *entry) {
  return entry->value.value.type == 0;
}

static bool thrift_fold_index(struct thrift_dom_state_entry *) {
  return FALSE;
}

static bool thrift_fold_literal(struct thrift_dom_state_entry *) {
  return FALSE;
}

static bool thrift_fold_binary(struct thrift_dom_state_entry *entry) {
  return entry->value.binary.done == TRUE;
}

static bool thrift_fold_pointer(struct thrift_dom_state_entry *entry) {
  return entry->value.pointer.done == TRUE;
}

void thrift_dom_init(struct thrift_dom *iter, struct malloc_lease *buffer) {
  u16 size1, size2;

  // default values
  iter->idx = 0;
  iter->buffer = buffer;

  iter->state.idx = 0;
  iter->state.size = THRIFT_DOM_STATE_INITIAL_SIZE;

  // calculate the size of all dynamic arrays
  size1 = (sizeof(struct thrift_dom_state_entry) + sizeof(u8)) * THRIFT_DOM_STATE_INITIAL_SIZE;
  size2 = (u16)((buffer->size - size1) / (sizeof(struct dom_token)));

  // first state entries and types
  iter->state.entries = (struct thrift_dom_state_entry *)buffer->ptr;
  iter->state.types = (u8 *)(buffer->ptr + THRIFT_DOM_STATE_INITIAL_SIZE * sizeof(struct thrift_dom_state_entry));

  iter->state.types[0] = THRIFT_DOM_STATE_TYPE_INIT;
  iter->state.entries[0].value.init.done = FALSE;

  // then place the tokens array to keep mod 16 alignment
  iter->size = size2;
  iter->tokens = (struct dom_token *)(buffer->ptr + size1);
}

bool thrift_dom_done(struct thrift_dom *) {
  return FALSE;
}

i64 thrift_dom_next(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  i64 result, consumed, previous;

  // default
  result = 0;
  consumed = 0;
  previous = iter->idx;

  // some states may produce up to three tokens,
  // so we need to ensure we have enough space

  while (iter->state.idx >= 0 && iter->idx < iter->size - 2) {

    // check for too nested structures, it will be valid in the entire loop
    if (iter->state.idx >= iter->state.size) return THRIFT_ERROR_TOO_NESTED;

    // call the appropriate function based on the current state type
    result = NEXT_FN[iter->state.types[iter->state.idx]](iter, tokens, entries, size);
    if (result == THRIFT_ERROR_BUFFER_OVERFLOW) break;
    if (result < 0) return result;

    // update the buffer pointers and sizes
    consumed += result;
    tokens += result;
    entries += result;
    size -= result;

    // try to fold the state till the bottom if possible
    while (iter->state.idx >= 0 && FOLD_FN[iter->state.types[iter->state.idx]](iter->state.entries + iter->state.idx)) {
      iter->state.idx--;
    }
  }

  // check if we produced any tokens
  if (previous == iter->idx && result == THRIFT_ERROR_BUFFER_OVERFLOW) {
    return THRIFT_ERROR_BUFFER_OVERFLOW;
  }

  // return the combined metrics
  return COMBINE(consumed, iter->idx - previous);
}

#if defined(I13C_TESTS)

static void can_init_iterator_single_page() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // assert the initial state of the iterator
  assert(iter.idx == 0, "iterator idx should be 0");
  assert(iter.size > 0, "iterator size should be greater than 0");
  assert(iter.size == 251, "iterator size should be 251 for 4KB buffer");

  assert(iter.tokens != NULL, "tokens should not be NULL");

  assert(iter.state.idx == 0, "state idx should be 0");
  assert(iter.state.size == 16, "state size should be 16");

  assert(iter.state.entries != NULL, "state entries should not be NULL");
  assert(iter.state.types != NULL, "state types should not be NULL");

  assert((void *)iter.state.entries < (void *)iter.state.types, "state entries should be before types");
  assert((void *)iter.state.types < (void *)iter.tokens, "state types should be before tokens");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_init_iterator_double_page() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 8192;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // assert the initial state of the iterator
  assert(iter.idx == 0, "iterator idx should be 0");
  assert(iter.size > 0, "iterator size should be greater than 0");
  assert(iter.size == 507, "iterator size should be 507 for 8KB buffer");

  assert(iter.tokens != NULL, "tokens should not be NULL");

  assert(iter.state.idx == 0, "state idx should be 0");
  assert(iter.state.size == 16, "state size should be 16");

  assert(iter.state.entries != NULL, "state entries should not be NULL");
  assert(iter.state.types != NULL, "state types should not be NULL");

  assert((void *)iter.state.entries < (void *)iter.state.types, "state entries should be before types");
  assert((void *)iter.state.types < (void *)iter.tokens, "state types should be before tokens");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_no_fields() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[1];
  struct thrift_iter_entry entries[1];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 0;
  entries[0].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 1);
  assert(PRODUCED(result) == 2, "should produce two tokens");
  assert(CONSUMED(result) == 1, "should consume one entry");

  assert(iter.idx == 2, "iterator idx should be 2");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_STRUCT_END, "token op should be STRUCT_END");
  assert(iter.tokens[1].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_one_field() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I32;

  tokens[1] = THRIFT_ITER_TOKEN_I32;
  entries[1].value.literal.value.v_i32 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i32", "token data should be NULL");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_two_fields() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[5];
  struct thrift_iter_entry entries[5];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I32;

  tokens[1] = THRIFT_ITER_TOKEN_I32;
  entries[1].value.literal.value.v_i32 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 19;
  entries[2].value.field.type = THRIFT_TYPE_I16;

  tokens[3] = THRIFT_ITER_TOKEN_I16;
  entries[3].value.literal.value.v_i16 = 142;

  tokens[4] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[4].value.field.id = 0;
  entries[4].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 5);
  assert(PRODUCED(result) == 14, "should produce fourteen tokens");
  assert(CONSUMED(result) == 5, "should consume five entries");

  assert(iter.idx == 14, "iterator idx should be 14");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i32", "token data should be i32");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[7].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[7].data == (u64) "i16", "token data should be i16");

  assert(iter.tokens[8].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[8].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[8].data == 19, "token data should be 19");

  assert(iter.tokens[9].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[9].data == 0, "token type should be NULL");

  assert(iter.tokens[10].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[10].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[10].data == 0, "token data should be NULL");

  assert(iter.tokens[11].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[11].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[11].data == 142, "token data should be 142");

  assert(iter.tokens[12].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[12].data == 0, "token type should be NULL");

  assert(iter.tokens[13].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[13].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i8_field_positive() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I8;

  tokens[1] = THRIFT_ITER_TOKEN_I8;
  entries[1].value.literal.value.v_i8 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i8", "token data should be i8");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I8, "token type should be DOM_TYPE_I8");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I8, "token type should be DOM_TYPE_I8");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i8_field_negative() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I8;

  tokens[1] = THRIFT_ITER_TOKEN_I8;
  entries[1].value.literal.value.v_i8 = -42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i8", "token data should be i8");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I8, "token type should be DOM_TYPE_I8");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I8, "token type should be DOM_TYPE_I8");
  assert(iter.tokens[5].data == (u64)(i64)-42, "token data should be -42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i16_field_positive() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I16;

  tokens[1] = THRIFT_ITER_TOKEN_I16;
  entries[1].value.literal.value.v_i16 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i16", "token data should be i16");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i16_field_negative() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I16;

  tokens[1] = THRIFT_ITER_TOKEN_I16;
  entries[1].value.literal.value.v_i16 = -42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i16", "token data should be i16");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I16, "token type should be DOM_TYPE_I16");
  assert(iter.tokens[5].data == (u64)(i64)-42, "token data should be -42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i32_field_positive() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I32;

  tokens[1] = THRIFT_ITER_TOKEN_I32;
  entries[1].value.literal.value.v_i32 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i32", "token data should be i32");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i32_field_negative() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I32;

  tokens[1] = THRIFT_ITER_TOKEN_I32;
  entries[1].value.literal.value.v_i32 = -42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i32", "token data should be i32");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[5].data == (u64)(i64)-42, "token data should be -42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i64_field_positive() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I64;

  tokens[1] = THRIFT_ITER_TOKEN_I64;
  entries[1].value.literal.value.v_i64 = 42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i64", "token data should be i64");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I64, "token type should be DOM_TYPE_I64");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I64, "token type should be DOM_TYPE_I64");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_i64_field_negative() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_I64;

  tokens[1] = THRIFT_ITER_TOKEN_I64;
  entries[1].value.literal.value.v_i64 = -42;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "i64", "token data should be i64");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I64, "token type should be DOM_TYPE_I64");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I64, "token type should be DOM_TYPE_I64");
  assert(iter.tokens[5].data == (u64)(i64)-42, "token data should be -42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_bool_field_true() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_BOOL_TRUE;

  tokens[1] = THRIFT_ITER_TOKEN_BOOL;
  entries[1].value.literal.value.v_bool = TRUE;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "bool", "token data should be bool");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[5].data == (u64) "true", "token data should be 'true'");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_bool_field_false() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[3];
  struct thrift_iter_entry entries[3];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_BOOL_FALSE;

  tokens[1] = THRIFT_ITER_TOKEN_BOOL;
  entries[1].value.literal.value.v_bool = FALSE;

  tokens[2] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[2].value.field.id = 0;
  entries[2].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 3);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 3, "should consume three entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "bool", "token data should be bool");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[4].data == 0, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[5].data == (u64) "false", "token data should be 'false'");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_write_struct_with_binary_field() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_dom iter;

  u8 tokens[4];
  struct thrift_iter_entry entries[4];

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_dom_init(&iter, &lease);

  // data
  tokens[0] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[0].value.field.id = 17;
  entries[0].value.field.type = THRIFT_TYPE_BINARY;

  tokens[1] = THRIFT_ITER_TOKEN_BINARY_CHUNK;
  entries[1].value.chunk.size = 5;
  entries[1].value.chunk.offset = 0;

  tokens[2] = THRIFT_ITER_TOKEN_BINARY_CONTENT;
  entries[2].value.content.ptr = "hello";

  tokens[3] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  entries[3].value.field.id = 0;
  entries[3].value.field.type = THRIFT_TYPE_STOP;

  // iterate over the buffer
  result = thrift_dom_next(&iter, tokens, entries, 4);
  assert(PRODUCED(result) == 8, "should produce eight tokens");
  assert(CONSUMED(result) == 4, "should consume four entries");

  assert(iter.idx == 8, "iterator idx should be 8");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0].op == DOM_OP_STRUCT_START, "token op should be STRUCT_START");
  assert(iter.tokens[0].data == 0, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == (u64) "binary", "token data should be binary");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == 0, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[4].data == 5, "token data should be 5, the size of the binary");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_TEXT, "token type should be DOM_TYPE_TEXT");
  assert(iter.tokens[5].data == (u64) "hello", "token data should be 'hello'");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == 0, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == 0, "token type should be NULL");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

void thrift_test_cases_dom(struct runner_context *ctx) {
  test_case(ctx, "can initialize iterator with single page", can_init_iterator_single_page);
  test_case(ctx, "can initialize iterator with double page", can_init_iterator_double_page);

  test_case(ctx, "can write struct with no fields", can_write_struct_with_no_fields);
  test_case(ctx, "can write struct with one field", can_write_struct_with_one_field);
  test_case(ctx, "can write struct with two fields", can_write_struct_with_two_fields);

  test_case(ctx, "can write struct with i8 field positive", can_write_struct_with_i8_field_positive);
  test_case(ctx, "can write struct with i8 field negative", can_write_struct_with_i8_field_negative);

  test_case(ctx, "can write struct with i16 field positive", can_write_struct_with_i16_field_positive);
  test_case(ctx, "can write struct with i16 field negative", can_write_struct_with_i16_field_negative);

  test_case(ctx, "can write struct with i32 field positive", can_write_struct_with_i32_field_positive);
  test_case(ctx, "can write struct with i32 field negative", can_write_struct_with_i32_field_negative);

  test_case(ctx, "can write struct with i64 field positive", can_write_struct_with_i64_field_positive);
  test_case(ctx, "can write struct with i64 field negative", can_write_struct_with_i64_field_negative);

  test_case(ctx, "can write struct with bool field true", can_write_struct_with_bool_field_true);
  test_case(ctx, "can write struct with bool field false", can_write_struct_with_bool_field_false);

  test_case(ctx, "can write struct with binary field", can_write_struct_with_binary_field);
}

#endif
