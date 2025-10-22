#include "thrift.iter.h"
#include "malloc.h"
#include "thrift.base.h"
#include "typing.h"

#define THRIFT_ITER_STATE_INITIAL_SIZE 16

/// @brief Thrift field type callback function type.
/// @param token Pointer to the token where the result will be stored.
/// @param target Pointer to the target struct.
/// @param buffer Pointer to the buffer containing the data.
/// @param buffer_size The number of bytes available in the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_delegate_fn)(struct thrift_iter *iter, const char *buffer, u64 buffer_size);

typedef i64 (*thrift_fold_fn)(struct thrift_iter_state *state);

static i64 thrift_delegate_bool(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;

  // delegate to thrift_read_bool
  result = thrift_read_bool(&iter->entries[iter->idx].value.literal.value.v_bool, buffer, buffer_size);
  if (result < 0) return result;

  // emit BOOL token
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_BOOL;

  // success
  return result;
}

static i64 thrift_delegate_bool_true(struct thrift_iter *iter, const char *, u64) {
  // emit BOOL token
  iter->entries[iter->idx].value.literal.value.v_bool = TRUE;
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_BOOL;

  // success
  return 0;
}

static i64 thrift_delegate_bool_false(struct thrift_iter *iter, const char *, u64) {
  // emit BOOL token
  iter->entries[iter->idx].value.literal.value.v_bool = FALSE;
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_BOOL;

  // success
  return 0;
}

static i64 thrift_delegate_i8(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;

  // delegate to thrift_read_i8
  result = thrift_read_i8(&iter->entries[iter->idx].value.literal.value.v_i8, buffer, buffer_size);
  if (result < 0) return result;

  // emit I8 token
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_I8;

  // success
  return result;
}

static i64 thrift_delegate_i16(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;

  // delegate to thrift_read_i16
  result = thrift_read_i16(&iter->entries[iter->idx].value.literal.value.v_i16, buffer, buffer_size);
  if (result < 0) return result;

  // emit I16 token
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_I16;

  // success
  return result;
}

static i64 thrift_delegate_i32(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;

  // delegate to thrift_read_i32
  result = thrift_read_i32(&iter->entries[iter->idx].value.literal.value.v_i32, buffer, buffer_size);
  if (result < 0) return result;

  // emit I32 token
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_I32;

  // success
  return result;
}

static i64 thrift_delegate_i64(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;

  // delegate to thrift_read_i64
  result = thrift_read_i64(&iter->entries[iter->idx].value.literal.value.v_i64, buffer, buffer_size);
  if (result < 0) return result;

  // emit I64 token
  iter->tokens[iter->idx++] = THRIFT_ITER_TOKEN_I64;

  // success
  return result;
}

static i64 thrift_delegate_binary(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;
  u32 size;

  // read the binary header containing size
  result = thrift_read_binary_header(&size, buffer, buffer_size);
  if (result < 0) return result;

  // increase the state index
  iter->state.idx++;

  iter->state.types[iter->state.idx] = THRIFT_ITER_STATE_TYPE_BINARY;
  iter->state.entries[iter->state.idx].value.binary.size = size;
  iter->state.entries[iter->state.idx].value.binary.read = 0;

  // success
  return result;
}

static i64 thrift_delegate_list(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result, consumed;
  struct thrift_list_header header;

  // read the list header containing size and type
  result = thrift_read_list_header(&header, buffer, buffer_size);
  if (result < 0) return result;

  // update the consumed bytes
  consumed = result;

  // emit LIST_HEADER token/entry
  iter->tokens[iter->idx] = THRIFT_ITER_TOKEN_LIST_HEADER;
  iter->entries[iter->idx].value.list.size = header.size;
  iter->entries[iter->idx].value.list.type = header.type;

  // move to the next entry
  iter->idx++;

  // the field will be done immediately when list completed
  iter->state.entries[iter->state.idx].value.literal.done = TRUE;

  // increase the state index
  iter->state.idx++;

  // and fill it up based on the item type
  iter->state.types[iter->state.idx] = THRIFT_ITER_STATE_TYPE_LIST;
  iter->state.entries[iter->state.idx].value.list.size = header.size;
  iter->state.entries[iter->state.idx].value.list.type = header.type;

  // success
  return consumed;
}

// forward declaration
static i64 thrift_delegate_struct(struct thrift_iter *iter, const char *buffer, u64 buffer_size);

static const thrift_delegate_fn THRIFT_ITEM_LITERAL_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_delegate_bool,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_delegate_bool,
  [THRIFT_TYPE_I8] = thrift_delegate_i8,
  [THRIFT_TYPE_I16] = thrift_delegate_i16,
  [THRIFT_TYPE_I32] = thrift_delegate_i32,
  [THRIFT_TYPE_I64] = thrift_delegate_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_delegate_binary,
  [THRIFT_TYPE_LIST] = thrift_delegate_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_delegate_struct,
  [THRIFT_TYPE_UUID] = NULL,
};

static const thrift_delegate_fn THRIFT_ITEM_FIELD_FN[THRIFT_TYPE_SIZE] = {
  [THRIFT_TYPE_STOP] = NULL,
  [THRIFT_TYPE_BOOL_TRUE] = thrift_delegate_bool_true,
  [THRIFT_TYPE_BOOL_FALSE] = thrift_delegate_bool_false,
  [THRIFT_TYPE_I8] = thrift_delegate_i8,
  [THRIFT_TYPE_I16] = thrift_delegate_i16,
  [THRIFT_TYPE_I32] = thrift_delegate_i32,
  [THRIFT_TYPE_I64] = thrift_delegate_i64,
  [THRIFT_TYPE_DOUBLE] = NULL,
  [THRIFT_TYPE_BINARY] = thrift_delegate_binary,
  [THRIFT_TYPE_LIST] = thrift_delegate_list,
  [THRIFT_TYPE_SET] = NULL,
  [THRIFT_TYPE_MAP] = NULL,
  [THRIFT_TYPE_STRUCT] = thrift_delegate_struct,
  [THRIFT_TYPE_UUID] = NULL,
};

void thrift_iter_init(struct thrift_iter *iter, struct malloc_lease *buffer) {
  u16 size1, size2;

  // default values
  iter->idx = 0;
  iter->buffer = buffer;

  iter->state.idx = 0;
  iter->state.size = THRIFT_ITER_STATE_INITIAL_SIZE;

  // calculate the size of all dynamic arrays
  size1 = (sizeof(struct thrift_iter_state_entry) + sizeof(u8)) * THRIFT_ITER_STATE_INITIAL_SIZE;
  size2 = (u16)((buffer->size - size1) / (sizeof(struct thrift_iter_entry) + sizeof(u8)));

  // first state entries and types
  iter->state.entries = (struct thrift_iter_state_entry *)buffer->ptr;
  iter->state.types = (u8 *)(buffer->ptr + THRIFT_ITER_STATE_INITIAL_SIZE * sizeof(struct thrift_iter_state_entry));

  iter->state.types[0] = THRIFT_ITER_STATE_TYPE_STRUCT;
  iter->state.entries[0].value.fields.field = 0;
  iter->state.entries[0].value.fields.type = 0xffffffff;

  // then place the entries array to keep mod 16 alignment
  iter->size = size2;
  iter->entries = (struct thrift_iter_entry *)(buffer->ptr + size1);

  // tokens can be placed anywhere because they are u8
  iter->tokens = (u8 *)(buffer->ptr + size1 + size2 * sizeof(struct thrift_iter_entry));
}

extern bool thrift_iter_done(struct thrift_iter *) {
  return FALSE;
}

/// @brief Function type for reading the next element from a Thrift iterator.
/// @param iter Pointer to the Thrift iterator.
/// @param buffer Pointer to the buffer containing the Thrift data.
/// @param buffer_size The number of bytes available in the buffer.
/// @return The number of bytes read from the buffer, or a negative error code.
typedef i64 (*thrift_iter_next_fn)(struct thrift_iter *iter, const char *buffer, u64 buffer_size);

/// @brief Function type for folding the iterator state.
/// @param entry Pointer to the state entry to fold.
/// @return True if the state can be folded, false otherwise.
typedef bool (*thrift_iter_fold_fn)(struct thrift_iter_state_entry *entry);

static bool thrift_iter_fold_struct(struct thrift_iter_state_entry *entry) {
  return entry->value.fields.type == THRIFT_TYPE_STOP;
}

static bool thrift_iter_fold_list(struct thrift_iter_state_entry *entry) {
  return entry->value.list.size == 0;
}

static bool thrift_iter_fold_literal(struct thrift_iter_state_entry *entry) {
  return entry->value.literal.done == TRUE;
}

static bool thrift_iter_fold_binary(struct thrift_iter_state_entry *entry) {
  return entry->value.binary.read == entry->value.binary.size;
}

static const thrift_iter_fold_fn FOLD_FN[THRIFT_ITER_STATE_TYPE_SIZE] = {
  [THRIFT_ITER_STATE_TYPE_BINARY] = thrift_iter_fold_binary,
  [THRIFT_ITER_STATE_TYPE_STRUCT] = thrift_iter_fold_struct,
  [THRIFT_ITER_STATE_TYPE_LIST] = thrift_iter_fold_list,
  [THRIFT_ITER_STATE_TYPE_LITERAL] = thrift_iter_fold_literal,
};

static i64 thrift_iter_next_literal(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  u32 item_type;

  // extract the item type to find the appropriate dispatch function
  item_type = iter->state.entries[iter->state.idx].value.literal.type;

  // primitive types are expected to be done after one read
  if (item_type != THRIFT_TYPE_STRUCT && item_type != THRIFT_TYPE_LIST) {
    iter->state.entries[iter->state.idx].value.literal.done = TRUE;
  }

  // forward the call to the field dispatch function if the owner is a struct
  if (iter->state.types[iter->state.idx - 1] == THRIFT_ITER_STATE_TYPE_STRUCT) {
    return THRIFT_ITEM_FIELD_FN[item_type](iter, buffer, buffer_size);
  }

  // forward the call to the already found dispatch function
  return THRIFT_ITEM_LITERAL_FN[item_type](iter, buffer, buffer_size);
}

static i64 thrift_iter_next_struct(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  i64 result;
  struct thrift_struct_header header;

  // restore the last field id from the state entry
  header.field = iter->state.entries[iter->state.idx].value.fields.field;

  // read the next struct field header
  result = thrift_read_struct_header(&header, buffer, buffer_size);
  if (result < 0) return result;

  // emit next token/entry
  iter->tokens[iter->idx] = THRIFT_ITER_TOKEN_STRUCT_FIELD;
  iter->entries[iter->idx].value.field.id = header.field;
  iter->entries[iter->idx].value.field.type = header.type;

  // move to the next entry
  iter->idx++;

  // update the state entry with the new field type
  iter->state.entries[iter->state.idx].value.fields.type = header.type;

  // the last field is reached, we are done with this struct
  if (header.type == THRIFT_TYPE_STOP) return result;

  // otherwise we need to update the state for the next call
  iter->state.entries[iter->state.idx].value.fields.field = header.field;

  // increase the state index
  iter->state.idx++;

  // and fill it up based on the field type
  iter->state.types[iter->state.idx] = THRIFT_ITER_STATE_TYPE_LITERAL;
  iter->state.entries[iter->state.idx].value.literal.type = header.type;
  iter->state.entries[iter->state.idx].value.literal.done = FALSE;

  // success
  return result;
}

static i64 thrift_delegate_struct(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  // the field will be done immediately when struct completed
  iter->state.entries[iter->state.idx].value.literal.done = TRUE;

  // increase the state index
  iter->state.idx++;

  // initialize the new state entry as a struct
  iter->state.entries[iter->state.idx].value.fields.field = 0;
  iter->state.types[iter->state.idx] = THRIFT_ITER_STATE_TYPE_STRUCT;

  // delegate to the struct next function
  return thrift_iter_next_struct(iter, buffer, buffer_size);
}

static i64 thrift_iter_next_list(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  u32 item_type;
  u32 remaining;

  // extract the item type and remaining count from the current state entry
  item_type = iter->state.entries[iter->state.idx].value.list.type;
  remaining = iter->state.entries[iter->state.idx].value.list.size;

  // the list loose one item
  iter->state.entries[iter->state.idx].value.list.size = remaining - 1;

  // new state entry for the item
  iter->state.idx++;

  // and fill it up based on the item type
  iter->state.types[iter->state.idx] = THRIFT_ITER_STATE_TYPE_LITERAL;
  iter->state.entries[iter->state.idx].value.literal.type = item_type;
  iter->state.entries[iter->state.idx].value.literal.done = FALSE;

  // success
  return thrift_iter_next_literal(iter, buffer, buffer_size);
}

static i64 thrift_iter_next_binary(struct thrift_iter *iter, const char *buffer, u64 buffer_size) {
  u32 size, done, read;

  // extract the remaining bytes from the current state entry
  size = iter->state.entries[iter->state.idx].value.binary.size;
  done = iter->state.entries[iter->state.idx].value.binary.read;

  // calculate how many bytes we can read
  if (size - done > buffer_size) {
    read = buffer_size;
  } else {
    read = size - done;
  }

  // check for buffer overflow
  if (read == 0) return THRIFT_ERROR_BUFFER_OVERFLOW;

  // update the read count
  iter->state.entries[iter->state.idx].value.binary.read += read;

  // emit BINARY_CHUNK token/entry
  iter->tokens[iter->idx] = THRIFT_ITER_TOKEN_BINARY_CHUNK;
  iter->entries[iter->idx].value.chunk.size = read;
  iter->entries[iter->idx++].value.chunk.offset = done;

  // emit BINARY_CONTENT token/entry
  iter->tokens[iter->idx] = THRIFT_ITER_TOKEN_BINARY_CONTENT;
  iter->entries[iter->idx++].value.content.ptr = buffer;

  // success
  return read;
}

static const thrift_iter_next_fn THRIFT_ITER_NEXT_FN[THRIFT_ITER_STATE_TYPE_SIZE] = {
  [THRIFT_ITER_STATE_TYPE_BINARY] = thrift_iter_next_binary,
  [THRIFT_ITER_STATE_TYPE_STRUCT] = thrift_iter_next_struct,
  [THRIFT_ITER_STATE_TYPE_LIST] = thrift_iter_next_list,
  [THRIFT_ITER_STATE_TYPE_LITERAL] = thrift_iter_next_literal,
};

i64 thrift_iter_next(struct thrift_iter *iter, const char *buffer, u64 *buffer_size) {
  i64 result, consumed, previous;

  // default
  consumed = 0;
  previous = iter->idx;

  while (iter->state.idx >= 0) {
    // call the appropriate function based on the current state type
    result = THRIFT_ITER_NEXT_FN[iter->state.types[iter->state.idx]](iter, buffer, *buffer_size);
    if (result == THRIFT_ERROR_BUFFER_OVERFLOW) break;
    if (result < 0) return result;

    // update the buffer pointers and sizes
    consumed += result;
    buffer += result;
    *buffer_size -= result;

    // try to fold the state till the bottom if possible
    while (iter->state.idx >= 0 && FOLD_FN[iter->state.types[iter->state.idx]](iter->state.entries + iter->state.idx)) {
      iter->state.idx--;
    }
  }

  // check if we produced any tokens
  if (previous == iter->idx && result == THRIFT_ERROR_BUFFER_OVERFLOW) {
    return THRIFT_ERROR_BUFFER_OVERFLOW;
  }

  *buffer_size = consumed;
  return iter->idx - previous;
}

#if defined(I13C_TESTS)

static void can_init_iterator_single_page() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // assert the initial state of the iterator
  assert(iter.idx == 0, "iterator idx should be 0");
  assert(iter.size > 0, "iterator size should be greater than 0");
  assert(iter.size == 439, "iterator size should be 439 for 4KB buffer");

  assert(iter.entries != NULL, "entries should not be NULL");
  assert(iter.tokens != NULL, "tokens should not be NULL");

  assert(iter.state.idx == 0, "state idx should be 0");
  assert(iter.state.size == 16, "state size should be 16");

  assert(iter.state.entries != NULL, "state entries should not be NULL");
  assert(iter.state.types != NULL, "state types should not be NULL");

  assert((void *)iter.state.entries < (void *)iter.state.types, "state entries should be before types");
  assert((void *)iter.state.types < (void *)iter.entries, "state types should be before entries");
  assert((void *)iter.entries < (void *)iter.tokens, "entries should be before tokens");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_init_iterator_double_page() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 8192;
  result = malloc_acquire(&pool, &lease);

  assert(result == 0, "should allocate memory");
  assert(lease.ptr != NULL, "lease ptr should be set");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // assert the initial state of the iterator
  assert(iter.idx == 0, "iterator idx should be 0");
  assert(iter.size > 0, "iterator size should be greater than 0");
  assert(iter.size == 894, "iterator size should be 894 for 8KB buffer");

  assert(iter.entries != NULL, "entries should not be NULL");
  assert(iter.tokens != NULL, "tokens should not be NULL");

  assert(iter.state.idx == 0, "state idx should be 0");
  assert(iter.state.size == 16, "state size should be 16");

  assert(iter.state.entries != NULL, "state entries should not be NULL");
  assert(iter.state.types != NULL, "state types should not be NULL");

  assert((void *)iter.state.entries < (void *)iter.state.types, "state entries should be before types");
  assert((void *)iter.state.types < (void *)iter.entries, "state types should be before entries");
  assert((void *)iter.entries < (void *)iter.tokens, "entries should be before tokens");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_empty_struct() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 1, "should produce one token");
  assert(buffer_size == 1, "should consume one byte");

  assert(iter.idx == 1, "iterator idx should be 1");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 0, "field id should be 0");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_one_field_struct() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x35, 0x14, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 3, "should produce three tokens");
  assert(buffer_size == 3, "should consume three bytes");

  assert(iter.idx == 3, "iterator idx should be 3");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 3, "field id should be 3");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[1].value.literal.value.v_i32 == 10, "literal value should be 10");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 0, "field id should be 0");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_two_fields_struct() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x35, 0x14, 0x05, 0x10, 0x12, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 5, "should produce five tokens");
  assert(buffer_size == 6, "should consume six bytes");

  assert(iter.idx == 5, "iterator idx should be 5");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 3, "field id should be 3");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[1].value.literal.value.v_i32 == 10, "literal value should be 10");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 19, "field id should be 19");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[3].value.literal.value.v_i32 == 9, "literal value should be 9");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_nested_struct() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x3c, 0x15, 0x12, 0x00, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 5, "should produce five tokens");
  assert(buffer_size == 5, "should consume five bytes");

  assert(iter.idx == 5, "iterator idx should be 5");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 3, "field id should be 3");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_STRUCT, "field type should be THRIFT_TYPE_STRUCT");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[1].value.field.id == 1, "field id should be 1");
  assert(iter.entries[1].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[2].value.literal.value.v_i32 == 9, "literal value should be 9");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[3].value.field.id == 0, "field id should be 0");
  assert(iter.entries[3].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_empty_list() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x79, 0x03, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 3, "should produce three tokens");
  assert(buffer_size == 3, "should consume three bytes");

  assert(iter.idx == 3, "iterator idx should be 3");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 0, "list size should be 0");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_I8, "list item type should be THRIFT_TYPE_I8");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 0, "field id should be 0");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_two_items_list() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x79, 0x23, 0x44, 0x14, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 5, "should produce five tokens");
  assert(buffer_size == 5, "should consume five bytes");

  assert(iter.idx == 5, "iterator idx should be 5");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 2, "list size should be 2");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_I8, "list item type should be THRIFT_TYPE_I8");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_I8, "token should be I8");
  assert(iter.entries[2].value.literal.value.v_i8 == 0x44, "i8 value should be 0x44");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_I8, "token should be I8");
  assert(iter.entries[3].value.literal.value.v_i8 == 0x14, "i8 value should be 0x14");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_nested_list() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x79, 0x1c, 0x15, 0x12, 0x00, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 6, "should produce six tokens");
  assert(buffer_size == 6, "should consume six bytes");

  assert(iter.idx == 6, "iterator idx should be 6");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 1, "list size should be 1");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_STRUCT, "list item type should be THRIFT_TYPE_STRUCT");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 1, "field id should be 1");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[3].value.literal.value.v_i32 == 9, "literal value should be 9");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[5].value.field.id == 0, "field id should be 0");
  assert(iter.entries[5].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_list_of_structs() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x39, 0x2c, 0x12, 0x00, 0x21, 0x00, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 9, "should produce nine tokens");
  assert(buffer_size == 7, "should consume seven bytes");

  assert(iter.idx == 9, "iterator idx should be 9");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 3, "field id should be 3");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 2, "list size should be 2");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_STRUCT, "list item type should be THRIFT_TYPE_STRUCT");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 1, "field id should be 1");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_BOOL_FALSE, "field type should be THRIFT_TYPE_BOOL_FALSE");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[3].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[5].value.field.id == 2, "field id should be 2");
  assert(iter.entries[5].value.field.type == THRIFT_TYPE_BOOL_TRUE, "field type should be THRIFT_TYPE_BOOL_TRUE");

  assert(iter.tokens[6] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[6].value.literal.value.v_bool == TRUE, "literal value should be true");

  assert(iter.tokens[7] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[7].value.field.id == 0, "field id should be 0");
  assert(iter.entries[7].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  assert(iter.tokens[8] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[8].value.field.id == 0, "field id should be 0");
  assert(iter.entries[8].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_list_of_bools_01() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x79, 0x32, 0x02, 0x01, 0x02, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 6, "should produce six tokens");
  assert(buffer_size == 6, "should consume six bytes");

  assert(iter.idx == 6, "iterator idx should be 6");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 3, "list size should be 3");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_BOOL_FALSE, "list item type should be THRIFT_TYPE_BOOL_FALSE");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[2].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[3].value.literal.value.v_bool == TRUE, "literal value should be true");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[4].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[5].value.field.id == 0, "field id should be 0");
  assert(iter.entries[5].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_list_of_bools_02() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x79, 0x31, 0x02, 0x01, 0x02, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 6, "should produce six tokens");
  assert(buffer_size == 6, "should consume six bytes");

  assert(iter.idx == 6, "iterator idx should be 6");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_LIST, "field type should be THRIFT_TYPE_LIST");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_LIST_HEADER, "token should be LIST_HEADER");
  assert(iter.entries[1].value.list.size == 3, "list size should be 3");
  assert(iter.entries[1].value.list.type == THRIFT_TYPE_BOOL_TRUE, "list item type should be THRIFT_TYPE_BOOL_TRUE");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[2].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[3].value.literal.value.v_bool == TRUE, "literal value should be true");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[4].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[5].value.field.id == 0, "field id should be 0");
  assert(iter.entries[5].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_integers() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x73, 0x14, 0x14, 0x16, 0x25, 0x18, 0x36, 0x1a, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 9, "should produce nine tokens");
  assert(buffer_size == 9, "should consume nine bytes");

  assert(iter.idx == 9, "iterator idx should be 9");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_I8, "field type should be THRIFT_TYPE_I8");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_I8, "token should be I8");
  assert(iter.entries[1].value.literal.value.v_i8 == 20, "literal value should be 20");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 8, "field id should be 8");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_I16, "field type should be THRIFT_TYPE_I16");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_I16, "token should be I16");
  assert(iter.entries[3].value.literal.value.v_i16 == 11, "literal value should be 11");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 10, "field id should be 10");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_I32, "field type should be THRIFT_TYPE_I32");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_I32, "token should be I32");
  assert(iter.entries[5].value.literal.value.v_i32 == 12, "literal value should be 12");

  assert(iter.tokens[6] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[6].value.field.id == 13, "field id should be 13");
  assert(iter.entries[6].value.field.type == THRIFT_TYPE_I64, "field type should be THRIFT_TYPE_I64");

  assert(iter.tokens[7] == THRIFT_ITER_TOKEN_I64, "token should be I64");
  assert(iter.entries[7].value.literal.value.v_i64 == 13, "literal value should be 13");

  assert(iter.tokens[8] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[8].value.field.id == 0, "field id should be 0");
  assert(iter.entries[8].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_bools() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x72, 0x11, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 5, "should produce five tokens");
  assert(buffer_size == 3, "should consume three bytes");

  assert(iter.idx == 5, "iterator idx should be 5");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_BOOL_FALSE, "field type should be THRIFT_TYPE_BOOL_FALSE");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[1].value.literal.value.v_bool == FALSE, "literal value should be false");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[2].value.field.id == 8, "field id should be 8");
  assert(iter.entries[2].value.field.type == THRIFT_TYPE_BOOL_TRUE, "field type should be THRIFT_TYPE_BOOL_TRUE");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_BOOL, "token should be BOOL");
  assert(iter.entries[3].value.literal.value.v_bool == TRUE, "literal value should be true");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[4].value.field.id == 0, "field id should be 0");
  assert(iter.entries[4].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_binary() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer[] = {0x78, 0x02, 0x01, 0x02, 0x00};
  buffer_size = sizeof(buffer);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer, &buffer_size);
  assert(result == 4, "should produce four tokens");
  assert(buffer_size == 5, "should consume five bytes");

  assert(iter.idx == 4, "iterator idx should be 4");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_BINARY, "field type should be THRIFT_TYPE_BINARY");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_BINARY_CHUNK, "token should be BINARY_CHUNK");
  assert(iter.entries[1].value.chunk.offset == 0, "chunk offset should be 0");
  assert(iter.entries[1].value.chunk.size == 2, "chunk size should be 2");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_BINARY_CONTENT, "token should be BINARY_CONTENT");
  assert(iter.entries[2].value.content.ptr == buffer + 2, "content ptr should point to buffer + 2");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[3].value.field.id == 0, "field id should be 0");
  assert(iter.entries[3].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_iterate_over_binary_fragmented() {
  i64 result;
  u64 buffer_size;

  struct malloc_pool pool;
  struct malloc_lease lease;
  struct thrift_iter iter;

  // data
  const char buffer1[] = {0x78, 0x05, 0x01, 0x02};
  const char buffer2[] = {0x03, 0x04, 0x05, 0x00};
  buffer_size = sizeof(buffer1);

  // initialize the pool
  malloc_init(&pool);

  // acquire memory
  lease.size = 4096;
  result = malloc_acquire(&pool, &lease);
  assert(result == 0, "should allocate memory");

  // initialize the iterator with the buffer
  thrift_iter_init(&iter, &lease);

  // iterate over the buffer
  result = thrift_iter_next(&iter, buffer1, &buffer_size);
  assert(result == 3, "should produce three tokens");
  assert(buffer_size == 4, "should consume four bytes");
  assert(iter.idx == 3, "iterator idx should be 3");

  result = thrift_iter_next(&iter, buffer2, &buffer_size);
  assert(result == 3, "should produce three tokens");

  assert(iter.idx == 6, "iterator idx should be 6");
  assert(iter.state.idx == -1, "state idx should be -1");

  assert(iter.tokens[0] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[0].value.field.id == 7, "field id should be 7");
  assert(iter.entries[0].value.field.type == THRIFT_TYPE_BINARY, "field type should be THRIFT_TYPE_BINARY");

  assert(iter.tokens[1] == THRIFT_ITER_TOKEN_BINARY_CHUNK, "token should be BINARY_CHUNK");
  assert(iter.entries[1].value.chunk.offset == 0, "chunk offset should be 0");
  assert(iter.entries[1].value.chunk.size == 2, "chunk size should be 2");

  assert(iter.tokens[2] == THRIFT_ITER_TOKEN_BINARY_CONTENT, "token should be BINARY_CONTENT");
  assert(iter.entries[2].value.content.ptr == buffer1 + 2, "content ptr should point to buffer1 + 2");

  assert(iter.tokens[3] == THRIFT_ITER_TOKEN_BINARY_CHUNK, "token should be BINARY_CHUNK");
  assert(iter.entries[3].value.chunk.offset == 2, "chunk offset should be 2");
  assert(iter.entries[3].value.chunk.size == 3, "chunk size should be 3");

  assert(iter.tokens[4] == THRIFT_ITER_TOKEN_BINARY_CONTENT, "token should be BINARY_CONTENT");
  assert(iter.entries[4].value.content.ptr == buffer2, "content ptr should point to buffer2");

  assert(iter.tokens[5] == THRIFT_ITER_TOKEN_STRUCT_FIELD, "token should be STRUCT_FIELD");
  assert(iter.entries[5].value.field.id == 0, "field id should be 0");
  assert(iter.entries[5].value.field.type == THRIFT_TYPE_STOP, "field type should be THRIFT_TYPE_STOP");

  // release the memory
  malloc_release(&pool, &lease);

  // destroy the pool
  malloc_destroy(&pool);
}

void thrift_test_cases_iter(struct runner_context *ctx) {
  test_case(ctx, "can initialize iterator with single page", can_init_iterator_single_page);
  test_case(ctx, "can initialize iterator with double page", can_init_iterator_double_page);

  test_case(ctx, "can iterate over empty struct", can_iterate_over_empty_struct);
  test_case(ctx, "can iterate over one field struct", can_iterate_over_one_field_struct);
  test_case(ctx, "can iterate over two fields struct", can_iterate_over_two_fields_struct);
  test_case(ctx, "can iterate over nested struct", can_iterate_over_nested_struct);

  test_case(ctx, "can iterate over empty list", can_iterate_over_empty_list);
  test_case(ctx, "can iterate over two items list", can_iterate_over_two_items_list);
  test_case(ctx, "can iterate over nested list", can_iterate_over_nested_list);
  test_case(ctx, "can iterate over list of structs", can_iterate_over_list_of_structs);
  test_case(ctx, "can iterate over list of bools 01", can_iterate_over_list_of_bools_01);
  test_case(ctx, "can iterate over list of bools 02", can_iterate_over_list_of_bools_02);

  test_case(ctx, "can iterate over integers", can_iterate_over_integers);
  test_case(ctx, "can iterate over bools", can_iterate_over_bools);

  test_case(ctx, "can iterate over binary", can_iterate_over_binary);
  test_case(ctx, "can iterate over binary fragmented", can_iterate_over_binary_fragmented);
}

#endif
