#include "parquet.schema.h"
#include "arena.h"
#include "parquet.base.h"
#include "parquet.parse.h"
#include "runner.h"
#include "typing.h"

#define SCHEMA_MAX_DEPTH 10

i64 parquet_open_schema(struct arena_allocator *arena,
                        struct parquet_schema_element **metadata,
                        struct parquet_schema *schema) {
  i64 result;
  u32 size, element_index;

  u32 depth_index, depth_queue[SCHEMA_MAX_DEPTH];
  struct parquet_schema *depth_children[SCHEMA_MAX_DEPTH];

  struct parquet_schema **children;
  struct parquet_schema_element *element;

  depth_index = 0;
  depth_queue[depth_index] = 0;
  depth_children[depth_index] = NULL;

  element_index = 0;
  element = metadata[0];

  // the schema must have at least one element
  if (element == NULL) {
    return PARQUET_ERROR_INVALID_SCHEMA;
  }

  // the root element must have children
  if (element->num_children <= 0) {
    return PARQUET_ERROR_INVALID_SCHEMA;
  }

  while (element != NULL) {
    schema->name = element->name;
    schema->children.elements = NULL;
    schema->children.count = 0;

    size = element->num_children + 1;
    size *= sizeof(struct parquet_schema *);

    // the schema can only have one root node
    if (depth_index == 0 && depth_children[0] != NULL) {
      return PARQUET_ERROR_INVALID_SCHEMA;
    }

    if (element->num_children > 0) {
      result = arena_acquire(arena, size, (void **)&children);
      if (result < 0) return result;

      // prepare the children array
      schema->children.elements = children;
      schema->children.count = element->num_children;

      // terminate the array
      children[element->num_children] = NULL;

      // push to depth
      depth_children[depth_index] = schema;
      depth_queue[depth_index] = element->num_children;
    } else {
      // zero out depth
      depth_queue[depth_index] = 0;
      depth_children[depth_index] = NULL;
    }

    schema->repeated_type = element->repetition_type;
    schema->data_type = element->data_type;
    schema->type_length = element->type_length;
    schema->converted_type = element->converted_type;

    while (depth_index > 0 && depth_queue[depth_index] == 0) {
      depth_index--;
    }

    if (depth_queue[depth_index] > 0) {
      result = arena_acquire(arena, sizeof(struct parquet_schema), (void **)&schema);
      if (result < 0) return result;

      // put the schema in the parent's children array
      size = depth_children[depth_index]->children.count;
      depth_children[depth_index]->children.elements[size - depth_queue[depth_index]] = schema;

      // decrease the queue
      depth_queue[depth_index]--;

      // increase the depth
      depth_index++;
    }

    // ensure we do not exceed hard limits
    if (depth_index >= SCHEMA_MAX_DEPTH) {
      return PARQUET_ERROR_LIMITS_REACHED;
    }

    // move to the next element
    element = metadata[++element_index];
  }

  // indicates that the schema is incomplete
  if (depth_index > 0) {
    return PARQUET_ERROR_INVALID_SCHEMA;
  }

  // success
  return 0;
}

#if defined(I13C_TESTS)

static void can_open_schema_data01() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  struct parquet_metadata metadata;
  struct parquet_schema schema;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  result = parquet_open(&file, "data/test01.parquet");
  assert(result == 0, "should open parquet file");

  // parse the metadata
  result = parquet_parse(&file, &metadata);
  assert(result == 0, "should parse metadata");

  // and open the schema
  result = parquet_open_schema(&file.arena, metadata.schemas, &schema);
  assert(result == 0, "should open schema");

  // assert the schema values
  assert_eq_str(schema.name, "table", "should have correct schema name");
  assert(schema.children.count == 5, "should have five children");
  assert(schema.children.elements != NULL, "should have children elements");

  assert(schema.repeated_type == PARQUET_REPETITION_TYPE_NONE, "should have missing repeated type");
  assert(schema.converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.data_type == PARQUET_DATA_TYPE_NONE, "should have missing data type");
  assert(schema.type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the first child
  assert_eq_str(schema.children.elements[0]->name, "date", "should have a name for the first child");
  assert(schema.children.elements[0]->children.count == 0, "should have no children");
  assert(schema.children.elements[0]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[0]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[0]->converted_type == PARQUET_CONVERTED_TYPE_DATE, "should be date");
  assert(schema.children.elements[0]->data_type == PARQUET_DATA_TYPE_INT32, "should have int32 data type");
  assert(schema.children.elements[0]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the second child
  assert_eq_str(schema.children.elements[1]->name, "hour", "should have a name for the second child");
  assert(schema.children.elements[1]->children.count == 0, "should have no children");
  assert(schema.children.elements[1]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[1]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[1]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[1]->data_type == PARQUET_DATA_TYPE_INT32, "should have int32 data type");
  assert(schema.children.elements[1]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the third child
  assert_eq_str(schema.children.elements[2]->name, "ip_country_code", "should have a name for the third child");
  assert(schema.children.elements[2]->children.count == 0, "should have no children");
  assert(schema.children.elements[2]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[2]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[2]->converted_type == PARQUET_CONVERTED_TYPE_UTF8, "should be utf8");
  assert(schema.children.elements[2]->data_type == PARQUET_DATA_TYPE_BYTE_ARRAY, "should have byte array data type");
  assert(schema.children.elements[2]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the fourth child
  assert_eq_str(schema.children.elements[3]->name, "cnt", "should have a name for the fourth child");
  assert(schema.children.elements[3]->children.count == 0, "should have no children");
  assert(schema.children.elements[3]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[3]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[3]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[3]->data_type == PARQUET_DATA_TYPE_INT64, "should have int64 data type");
  assert(schema.children.elements[3]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the fifth child
  assert_eq_str(schema.children.elements[4]->name, "bin", "should have a name for the fifth child");
  assert(schema.children.elements[4]->children.count == 0, "should have no children");
  assert(schema.children.elements[4]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[4]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[4]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[4]->data_type == PARQUET_DATA_TYPE_INT32, "should have int32 data type");
  assert(schema.children.elements[4]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_open_schema_data02() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  struct parquet_metadata metadata;
  struct parquet_schema schema;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  result = parquet_open(&file, "data/test02.parquet");
  assert(result == 0, "should open parquet file");

  // parse the metadata
  result = parquet_parse(&file, &metadata);
  assert(result == 0, "should parse metadata");

  // and open the schema
  result = parquet_open_schema(&file.arena, metadata.schemas, &schema);
  assert(result == 0, "should open schema");

  // assert the schema values
  assert_eq_str(schema.name, "trino_schema", "should have correct schema name");
  assert(schema.children.count == 117, "should have 117 children");
  assert(schema.children.elements != NULL, "should have children elements");

  assert(schema.repeated_type == PARQUET_REPETITION_TYPE_NONE, "should have missing repeated type");
  assert(schema.converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.data_type == PARQUET_DATA_TYPE_NONE, "should have missing data type");
  assert(schema.type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 7th child
  assert_eq_str(schema.children.elements[6]->name, "id5uidchanged", "should have a name for the 7th child");
  assert(schema.children.elements[6]->children.count == 0, "should have no children");
  assert(schema.children.elements[6]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[6]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[6]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[6]->data_type == PARQUET_DATA_TYPE_BOOLEAN, "should have boolean data type");
  assert(schema.children.elements[6]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 113th child
  assert_eq_str(schema.children.elements[112]->name, "cookie_prediction_probability", "should be cookie...");
  assert(schema.children.elements[112]->children.count == 0, "should have no children");
  assert(schema.children.elements[112]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[112]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[112]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[112]->data_type == PARQUET_DATA_TYPE_FLOAT, "should have float data type");
  assert(schema.children.elements[112]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_open_schema_data03() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  struct parquet_metadata metadata;
  struct parquet_schema schema;
  struct parquet_schema *child;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  result = parquet_open(&file, "data/test03.parquet");
  assert(result == 0, "should open parquet file");

  // parse the metadata
  result = parquet_parse(&file, &metadata);
  assert(result == 0, "should parse metadata");

  // and open the schema
  result = parquet_open_schema(&file.arena, metadata.schemas, &schema);
  assert(result == 0, "should open schema");

  // assert the schema values
  assert_eq_str(schema.name, "trino_schema", "should have correct schema name");
  assert(schema.children.count == 13, "should have 13 children");
  assert(schema.children.elements != NULL, "should have children elements");

  assert(schema.repeated_type == PARQUET_REPETITION_TYPE_NONE, "should have missing repeated type");
  assert(schema.converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.data_type == PARQUET_DATA_TYPE_NONE, "should have missing data type");
  assert(schema.type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 8th child
  assert_eq_str(schema.children.elements[7]->name, "bidderrequests", "should have a name for the 8th child");
  assert(schema.children.elements[7]->children.count == 1, "should have 1 child");
  assert(schema.children.elements[7]->children.elements != NULL, "should have children elements");

  assert(schema.children.elements[7]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[7]->converted_type == PARQUET_CONVERTED_TYPE_LIST, "should be list");
  assert(schema.children.elements[7]->data_type == PARQUET_DATA_TYPE_NONE, "should be missing data type");
  assert(schema.children.elements[7]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 8.1th child
  child = schema.children.elements[7];

  assert_eq_str(child->children.elements[0]->name, "list", "should be named 'list'");
  assert(child->children.elements[0]->children.count == 1, "should have 1 child");
  assert(child->children.elements[0]->children.elements != NULL, "should have children elements");

  assert(child->children.elements[0]->repeated_type == PARQUET_REPETITION_TYPE_REPEATED, "should be repeated");
  assert(child->children.elements[0]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(child->children.elements[0]->data_type == PARQUET_DATA_TYPE_NONE, "should be missing data type");
  assert(child->children.elements[0]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 8.1.1th child
  child = child->children.elements[0];

  assert_eq_str(child->children.elements[0]->name, "element", "should be named 'element'");
  assert(child->children.elements[0]->children.count == 7, "should have 7 children");
  assert(child->children.elements[0]->children.elements != NULL, "should have children elements");

  assert(child->children.elements[0]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(child->children.elements[0]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(child->children.elements[0]->data_type == PARQUET_DATA_TYPE_NONE, "should be missing data type");
  assert(child->children.elements[0]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 8.1.1.1th child
  child = child->children.elements[0];

  assert_eq_str(child->children.elements[0]->name, "adunitcode", "should be named 'adunitcode'");
  assert(child->children.elements[0]->children.count == 0, "should have 0 children");
  assert(child->children.elements[0]->children.elements == NULL, "should have no children elements");

  assert(child->children.elements[0]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(child->children.elements[0]->converted_type == PARQUET_CONVERTED_TYPE_UTF8, "should be UTF8");
  assert(child->children.elements[0]->data_type == PARQUET_DATA_TYPE_BYTE_ARRAY, "should be byte array data type");
  assert(child->children.elements[0]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_open_schema_data04() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  struct parquet_metadata metadata;
  struct parquet_schema schema;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  result = parquet_open(&file, "data/test04.parquet");
  assert(result == 0, "should open parquet file");

  // parse the metadata
  result = parquet_parse(&file, &metadata);
  assert(result == 0, "should parse metadata");

  // and open the schema
  result = parquet_open_schema(&file.arena, metadata.schemas, &schema);
  assert(result == 0, "should open schema");

  // assert the schema values
  assert_eq_str(schema.name, "duckdb_schema", "should have correct schema name");
  assert(schema.children.count == 12, "should have 12 children");
  assert(schema.children.elements != NULL, "should have children elements");

  assert(schema.repeated_type == PARQUET_REPETITION_TYPE_REQUIRED, "should be required");
  assert(schema.converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.data_type == PARQUET_DATA_TYPE_NONE, "should have missing data type");
  assert(schema.type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 6th child
  assert_eq_str(schema.children.elements[5]->name, "Age", "should have a name for the 6th child");
  assert(schema.children.elements[5]->children.count == 0, "should have no children");
  assert(schema.children.elements[5]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[5]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[5]->converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.children.elements[5]->data_type == PARQUET_DATA_TYPE_DOUBLE, "should have double data type");
  assert(schema.children.elements[5]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_open_schema_data05() {
  i64 result;

  struct parquet_file file;
  struct malloc_pool pool;

  struct parquet_metadata metadata;
  struct parquet_schema schema;

  // initialize the pool
  malloc_init(&pool);

  // initialize the parquet file
  parquet_init(&file, &pool);

  // open a valid parquet file
  result = parquet_open(&file, "data/test05.parquet");
  assert(result == 0, "should open parquet file");

  // parse the metadata
  result = parquet_parse(&file, &metadata);
  assert(result == 0, "should parse metadata");

  // and open the schema
  result = parquet_open_schema(&file.arena, metadata.schemas, &schema);
  assert(result == 0, "should open schema");

  // assert the schema values
  assert_eq_str(schema.name, "spark_schema", "should have correct schema name");
  assert(schema.children.count == 4, "should have 4 children");
  assert(schema.children.elements != NULL, "should have children elements");

  assert(schema.repeated_type == PARQUET_REPETITION_TYPE_NONE, "should be missing");
  assert(schema.converted_type == PARQUET_CONVERTED_TYPE_NONE, "should be missing");
  assert(schema.data_type == PARQUET_DATA_TYPE_NONE, "should have missing data type");
  assert(schema.type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // assert the 1st child
  assert_eq_str(schema.children.elements[0]->name, "ipCountryCode", "should have a name for the 1st child");
  assert(schema.children.elements[0]->children.count == 0, "should have no children");
  assert(schema.children.elements[0]->children.elements == NULL, "should have no children elements");

  assert(schema.children.elements[0]->repeated_type == PARQUET_REPETITION_TYPE_OPTIONAL, "should be optional");
  assert(schema.children.elements[0]->converted_type == PARQUET_CONVERTED_TYPE_UTF8, "should be UTF8");
  assert(schema.children.elements[0]->data_type == PARQUET_DATA_TYPE_BYTE_ARRAY, "should have byte array data type");
  assert(schema.children.elements[0]->type_length == PARQUET_UNKNOWN_VALUE, "should have unknown type length");

  // close the parquet file
  parquet_close(&file);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_empty_schema() {
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element *metadata[1];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  metadata[0] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == PARQUET_ERROR_INVALID_SCHEMA, "should detect invalid schema");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_only_root_schema() {
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element elements[1];
  struct parquet_schema_element *metadata[2];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  elements[0].name = "table";
  elements[0].num_children = 0;
  elements[0].repetition_type = PARQUET_REPETITION_TYPE_NONE;
  elements[0].data_type = PARQUET_DATA_TYPE_NONE;
  elements[0].type_length = PARQUET_UNKNOWN_VALUE;
  elements[0].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  metadata[0] = &elements[0];
  metadata[1] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == PARQUET_ERROR_INVALID_SCHEMA, "should detect only root schema");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_too_short_schema() {
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element elements[2];
  struct parquet_schema_element *metadata[3];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  elements[0].name = "table";
  elements[0].num_children = 2;
  elements[0].repetition_type = PARQUET_REPETITION_TYPE_NONE;
  elements[0].data_type = PARQUET_DATA_TYPE_NONE;
  elements[0].type_length = PARQUET_UNKNOWN_VALUE;
  elements[0].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  elements[1].name = "field1";
  elements[1].num_children = 0;
  elements[1].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  elements[1].data_type = PARQUET_DATA_TYPE_INT32;
  elements[1].type_length = PARQUET_UNKNOWN_VALUE;
  elements[1].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  metadata[0] = &elements[0];
  metadata[1] = &elements[1];
  metadata[2] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == PARQUET_ERROR_INVALID_SCHEMA, "should detect too short schema");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_too_long_schema() {
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element elements[3];
  struct parquet_schema_element *metadata[4];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  elements[0].name = "table";
  elements[0].num_children = 1;
  elements[0].repetition_type = PARQUET_REPETITION_TYPE_NONE;
  elements[0].data_type = PARQUET_DATA_TYPE_NONE;
  elements[0].type_length = PARQUET_UNKNOWN_VALUE;
  elements[0].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  elements[1].name = "field1";
  elements[1].num_children = 0;
  elements[1].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  elements[1].data_type = PARQUET_DATA_TYPE_INT32;
  elements[1].type_length = PARQUET_UNKNOWN_VALUE;
  elements[1].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  elements[2].name = "field2";
  elements[2].num_children = 0;
  elements[2].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  elements[2].data_type = PARQUET_DATA_TYPE_INT32;
  elements[2].type_length = PARQUET_UNKNOWN_VALUE;
  elements[2].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  metadata[0] = &elements[0];
  metadata[1] = &elements[1];
  metadata[2] = &elements[2];
  metadata[3] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == PARQUET_ERROR_INVALID_SCHEMA, "should detect too long schema");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_handle_nested_only_schema() {
  i64 result;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element elements[3];
  struct parquet_schema_element *metadata[4];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  elements[0].name = "table";
  elements[0].num_children = 1;
  elements[0].repetition_type = PARQUET_REPETITION_TYPE_NONE;
  elements[0].data_type = PARQUET_DATA_TYPE_NONE;
  elements[0].type_length = PARQUET_UNKNOWN_VALUE;
  elements[0].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  elements[1].name = "field1";
  elements[1].num_children = 1;
  elements[1].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  elements[1].data_type = PARQUET_DATA_TYPE_INT32;
  elements[1].type_length = PARQUET_UNKNOWN_VALUE;
  elements[1].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  elements[2].name = "field2";
  elements[2].num_children = 0;
  elements[2].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
  elements[2].data_type = PARQUET_DATA_TYPE_INT32;
  elements[2].type_length = PARQUET_UNKNOWN_VALUE;
  elements[2].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  metadata[0] = &elements[0];
  metadata[1] = &elements[1];
  metadata[2] = &elements[2];
  metadata[3] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == 0, "should return success");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

static void can_detect_nesting_hard_limits() {
  i64 result;
  u32 counter;

  struct malloc_pool pool;
  struct arena_allocator arena;

  struct parquet_schema schema;
  struct parquet_schema_element elements[11];
  struct parquet_schema_element *metadata[12];

  // initialize the pool
  malloc_init(&pool);

  // initialize the arena
  arena_init(&arena, &pool, 4096, 4096);

  // prepare test case
  elements[0].name = "table";
  elements[0].num_children = 1;
  elements[0].repetition_type = PARQUET_REPETITION_TYPE_NONE;
  elements[0].data_type = PARQUET_DATA_TYPE_NONE;
  elements[0].type_length = PARQUET_UNKNOWN_VALUE;
  elements[0].converted_type = PARQUET_CONVERTED_TYPE_NONE;

  for (counter = 1; counter <= 10; counter++) {
    elements[counter].name = "field";
    elements[counter].num_children = (counter < 10) ? 1 : 0;
    elements[counter].repetition_type = PARQUET_REPETITION_TYPE_OPTIONAL;
    elements[counter].data_type = PARQUET_DATA_TYPE_INT32;
    elements[counter].type_length = PARQUET_UNKNOWN_VALUE;
    elements[counter].converted_type = PARQUET_CONVERTED_TYPE_NONE;
  }

  for (counter = 0; counter <= 10; counter++) {
    metadata[counter] = &elements[counter];
  }

  metadata[11] = NULL;

  // and open the schema
  result = parquet_open_schema(&arena, metadata, &schema);
  assert(result == PARQUET_ERROR_LIMITS_REACHED, "should fail with limits reached");

  // destroy the arena
  arena_destroy(&arena);

  // destroy the pool
  malloc_destroy(&pool);
}

void parquet_test_cases_schema(struct runner_context *ctx) {
  test_case(ctx, "can open schema data01", can_open_schema_data01);
  test_case(ctx, "can open schema data02", can_open_schema_data02);
  test_case(ctx, "can open schema data03", can_open_schema_data03);
  test_case(ctx, "can open schema data04", can_open_schema_data04);
  test_case(ctx, "can open schema data05", can_open_schema_data05);

  test_case(ctx, "can detect empty schema", can_detect_empty_schema);
  test_case(ctx, "can detect only root schema", can_detect_only_root_schema);
  test_case(ctx, "can detect too short schema", can_detect_too_short_schema);
  test_case(ctx, "can detect too long schema", can_detect_too_long_schema);
  test_case(ctx, "can handle nested only schema", can_handle_nested_only_schema);
  test_case(ctx, "can detect nesting hard limits", can_detect_nesting_hard_limits);
}

#endif
