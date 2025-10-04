#include "parquet.schema.out.h"
#include "format.h"
#include "malloc.h"
#include "parquet.iter.h"
#include "runner.h"
#include "typing.h"

void parquet_schema_out_init(struct parquet_schema_out_state *state,
                             struct malloc_lease *buffer,
                             struct parquet_schema *schema) {

  // initialize the state
  state->buffer = buffer;

  // null terminate the root array
  state->ctx.root[0] = schema;
  state->ctx.root[1] = NULL;

  // initialize the context
  state->ctx.depth = 0;
  state->ctx.indices[0] = 0;
  state->ctx.stack[0] = state->ctx.root;

  // initialize the format context
  state->fmt.fmt = NULL;
  state->fmt.vargs = state->vargs;
  state->fmt.vargs_offset = 0;
  state->fmt.vargs_max = PARQUET_SCHEMA_OUT_CONTEXT_MAX_VARGS;

  state->fmt.buffer = (char *)buffer->ptr;
  state->fmt.buffer_size = (u32)buffer->size - 64;
  state->fmt.buffer_offset = 0;
}

i64 parquet_schema_out_flush(struct parquet_schema_out_state *state) {
  i64 result;

  // call format to flush remaining formatting
  result = format(&state->fmt);
  if (result < 0) return result;

  // success
  return 0;
}

static const char *const PARQUET_SCHEMA_OUT_FORMATS[8] = {
  [0b000] = "%e%e%s\n",         [0b100] = "%e%e%s, %s\n",
  [0b110] = "%e%e%s, %s, %s\n", [0b111] = "%e%e%s, %s, %s, %s\n",
  [0b101] = "%e%e%s, %s, %s\n", [0b001] = "%e%e%s, %s\n",
  [0b011] = "%e%e%s, %s, %s\n", [0b010] = "%e%e%s, %s\n",
};

static void parquet_schema_out_format(struct format_context *fmt, struct parquet_schema *schema, u32 depth) {
  u32 flags, offset;

  // default
  flags = 0;
  offset = 5;

  // prepare the tree visualization
  fmt->vargs[0] = (void *)" |   ";
  fmt->vargs[1] = (void *)(u64)(depth > 0 ? depth - 1 : 0);
  fmt->vargs[2] = (void *)" |-- ";
  fmt->vargs[3] = (void *)(u64)(depth > 0 ? 1 : 0);
  fmt->vargs[4] = (void *)schema->name;

  // prepare the converted type if available
  if (schema->converted_type > PARQUET_CONVERTED_TYPE_NONE && schema->converted_type < PARQUET_CONVERTED_TYPE_SIZE) {
    flags |= 0b100;
    fmt->vargs[offset++] = (void *)PARQUET_CONVERTED_TYPE_NAMES[schema->converted_type];
  }

  // prepare the data type if available
  if (schema->data_type > PARQUET_DATA_TYPE_NONE && schema->data_type < PARQUET_DATA_TYPE_SIZE) {
    flags |= 0b010;
    fmt->vargs[offset++] = (void *)PARQUET_DATA_TYPE_NAMES[schema->data_type];
  }

  // prepare the repetition type if available
  if (schema->repeated_type != PARQUET_REPETITION_TYPE_NONE && schema->repeated_type < PARQUET_REPETITION_TYPE_SIZE) {
    flags |= 0b001;
    fmt->vargs[offset++] = (void *)PARQUET_REPETITION_TYPE_NAMES[schema->repeated_type];
  }

  // always prepare the format string
  fmt->fmt = PARQUET_SCHEMA_OUT_FORMATS[flags];
  fmt->vargs_offset = 0;
}

i64 parquet_schema_out_next(struct parquet_schema_out_state *state) {
  i64 result;
  struct parquet_schema *schema;

  // fetch the current schema element
  schema = state->ctx.stack[state->ctx.depth][state->ctx.indices[state->ctx.depth]];

  while (schema != NULL) {
    // prepare the format string
    parquet_schema_out_format(&state->fmt, schema, state->ctx.depth);

    // format the output
    result = format(&state->fmt);
    if (result < 0) return result;

    // go to the next element within the current depth
    state->ctx.indices[state->ctx.depth]++;

    // if the schema has children, we need to go deeper
    if (schema->children.count > 0) {
      state->ctx.indices[state->ctx.depth + 1] = 0;
      state->ctx.stack[state->ctx.depth + 1] = schema->children.elements;
      state->ctx.depth++;
    }

    // fetch the next schema element
    schema = state->ctx.stack[state->ctx.depth][state->ctx.indices[state->ctx.depth]];

    // if there is no schema, we need to go up
    while (state->ctx.depth > 0 && schema == NULL) {
      state->ctx.depth--;
      schema = state->ctx.stack[state->ctx.depth][state->ctx.indices[state->ctx.depth]];
    }

    // we are done
    if (state->ctx.depth == 0 && schema == NULL) {
      return 0;
    }
  }

  return 0;
}

#if defined(I13C_TESTS)

void can_output_one_field_schema() {
  i64 result;

  struct malloc_pool pool;
  struct malloc_lease buffer;

  struct parquet_schema_out_state state;
  struct parquet_schema root;
  struct parquet_schema children[1];
  struct parquet_schema *pointers[2];

  // prepare the test
  buffer.size = 4096;

  root.name = "table";
  root.repeated_type = PARQUET_REPETITION_TYPE_REQUIRED;
  root.data_type = PARQUET_DATA_TYPE_NONE;
  root.type_length = PARQUET_UNKNOWN_VALUE;
  root.converted_type = PARQUET_CONVERTED_TYPE_NONE;
  root.children.count = 1;
  root.children.elements = pointers;

  children[0].name = "field";
  children[0].repeated_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  children[0].data_type = PARQUET_DATA_TYPE_INT32;
  children[0].type_length = PARQUET_UNKNOWN_VALUE;
  children[0].converted_type = PARQUET_CONVERTED_TYPE_DATE;
  children[0].children.count = 0;
  children[0].children.elements = NULL;

  pointers[0] = &children[0];
  pointers[1] = NULL;

  // initialize the pool
  malloc_init(&pool);

  // acquire the buffer
  result = malloc_acquire(&pool, &buffer);
  assert(result == 0, "should acquire buffer");

  // initialize the state
  parquet_schema_out_init(&state, &buffer, &root);

  // output the first and only chunk
  result = parquet_schema_out_next(&state);
  assert(result == 0, "should succeed");
  assert(state.fmt.buffer_offset == 50, "should write 50 bytes to the buffer");

  assert_eq_str(state.fmt.buffer,
                "table, REQUIRED\n"
                " |-- field, DATE, INT32, OPTIONAL\n",
                "should write exact text");

  // destroy the buffer
  malloc_release(&pool, &buffer);

  // destroy the pool
  malloc_destroy(&pool);
}

void parquet_test_cases_schema_out(struct runner_context *ctx) {
  test_case(ctx, "can output one field schema", can_output_one_field_schema);
}

#endif
