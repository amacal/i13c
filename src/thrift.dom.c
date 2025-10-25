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

static i64 thrift_next_init(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *, u64) {
  // emit STRUCT_START token
  iter->tokens[0].op = DOM_OP_STRUCT_START;
  iter->tokens[0].data = NULL;

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
    iter->tokens[iter->idx].data = NULL;

    // advance the iterator
    iter->idx++;

    return 1;
  }

  // emit KEY_START token
  iter->tokens[iter->idx].op = DOM_OP_KEY_START;
  iter->tokens[iter->idx].data = NULL;
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
  iter->tokens[iter->idx].data = NULL;

  // advance the iterator
  iter->idx++;

  // advance the state
  iter->state.idx++;

  // set the new state
  iter->state.types[iter->state.idx] = THRIFT_DOM_STATE_TYPE_VALUE;
  iter->state.entries[iter->state.idx].value.value.type = entries[0].value.field.type;

  // success
  return 1;
}

static i64
thrift_next_array(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  return -1;
}

static i64
thrift_next_key(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  return -1;
}

static i64
thrift_next_value(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  // check for size
  if (size == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // check for VALUE token
  if (tokens[0] != THRIFT_ITER_TOKEN_I32) {
    return THRIFT_ERROR_INVALID_IMPLEMENTATION;
  }

  // emit VALUE_START token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_START;
  iter->tokens[iter->idx].data = NULL;
  iter->tokens[iter->idx].type = DOM_TYPE_I32;

  // advance the iterator
  iter->idx++;

  // emit LITERAL token
  iter->tokens[iter->idx].op = DOM_OP_LITERAL;
  iter->tokens[iter->idx].data = entries[0].value.literal.value.v_i32;
  iter->tokens[iter->idx].type = DOM_TYPE_I32;

  // advance the iterator
  iter->idx++;

  // emit VALUE_END token
  iter->tokens[iter->idx].op = DOM_OP_VALUE_END;
  iter->tokens[iter->idx].data = NULL;

  // advance the iterator
  iter->idx++;

  // complete the value state
  iter->state.entries[iter->state.idx].value.value.type = 0;

  // success
  return 1;
}

static i64
thrift_next_index(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  return -1;
}

static i64
thrift_next_literal(struct thrift_dom *iter, const u8 *tokens, const struct thrift_iter_entry *entries, u64 size) {
  return -1;
}

static bool thrift_fold_init(struct thrift_dom_state_entry *entry) {
  return entry->value.init.done == TRUE;
}

static bool thrift_fold_struct(struct thrift_dom_state_entry *entry) {
  return entry->value.fields.field == 0;
}

static bool thrift_fold_array(struct thrift_dom_state_entry *entry) {
  return FALSE;
}

static bool thrift_fold_key(struct thrift_dom_state_entry *entry) {
  return FALSE;
}

static bool thrift_fold_value(struct thrift_dom_state_entry *entry) {
  return entry->value.value.type == 0;
}

static bool thrift_fold_index(struct thrift_dom_state_entry *entry) {
  return FALSE;
}

static bool thrift_fold_literal(struct thrift_dom_state_entry *entry) {
  return FALSE;
}

// fold dispatch table
static const thrift_iter_fold_fn FOLD_FN[THRIFT_DOM_STATE_TYPE_SIZE] = {
  [THRIFT_DOM_STATE_TYPE_INIT] = thrift_fold_init,       [THRIFT_DOM_STATE_TYPE_STRUCT] = thrift_fold_struct,
  [THRIFT_DOM_STATE_TYPE_ARRAY] = thrift_fold_array,     [THRIFT_DOM_STATE_TYPE_KEY] = thrift_fold_key,
  [THRIFT_DOM_STATE_TYPE_VALUE] = thrift_fold_value,     [THRIFT_DOM_STATE_TYPE_INDEX] = thrift_fold_index,
  [THRIFT_DOM_STATE_TYPE_LITERAL] = thrift_fold_literal,
};

// next dispatch table
static const thrift_iter_next_fn NEXT_FN[THRIFT_DOM_STATE_TYPE_SIZE] = {
  [THRIFT_DOM_STATE_TYPE_INIT] = thrift_next_init,       [THRIFT_DOM_STATE_TYPE_STRUCT] = thrift_next_struct,
  [THRIFT_DOM_STATE_TYPE_ARRAY] = thrift_next_array,     [THRIFT_DOM_STATE_TYPE_KEY] = thrift_next_key,
  [THRIFT_DOM_STATE_TYPE_VALUE] = thrift_next_value,     [THRIFT_DOM_STATE_TYPE_INDEX] = thrift_next_index,
  [THRIFT_DOM_STATE_TYPE_LITERAL] = thrift_next_literal,
};

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

bool thrift_dom_done(struct thrift_dom *iter) {
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
  assert(iter.tokens[0].data == NULL, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_STRUCT_END, "token op should be STRUCT_END");
  assert(iter.tokens[1].data == NULL, "token type should be NULL");

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
  assert(iter.tokens[0].data == NULL, "token type should be NULL");

  assert(iter.tokens[1].op == DOM_OP_KEY_START, "token op should be DOM_OP_KEY_START");
  assert(iter.tokens[1].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[1].data == NULL, "token data should be NULL");

  assert(iter.tokens[2].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[2].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[2].data == 17, "token data should be 17");

  assert(iter.tokens[3].op == DOM_OP_KEY_END, "token op should be DOM_OP_KEY_END");
  assert(iter.tokens[3].data == NULL, "token type should be NULL");

  assert(iter.tokens[4].op == DOM_OP_VALUE_START, "token op should be DOM_OP_VALUE_START");
  assert(iter.tokens[4].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[4].data == NULL, "token data should be NULL");

  assert(iter.tokens[5].op == DOM_OP_LITERAL, "token op should be DOM_OP_LITERAL");
  assert(iter.tokens[5].type == DOM_TYPE_I32, "token type should be DOM_TYPE_I32");
  assert(iter.tokens[5].data == 42, "token data should be 42");

  assert(iter.tokens[6].op == DOM_OP_VALUE_END, "token op should be DOM_OP_VALUE_END");
  assert(iter.tokens[6].data == NULL, "token type should be NULL");

  assert(iter.tokens[7].op == DOM_OP_STRUCT_END, "token op should be DOM_OP_STRUCT_END");
  assert(iter.tokens[7].data == NULL, "token type should be NULL");

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
}

#endif
