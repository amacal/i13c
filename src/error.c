#include "error.h"
#include "runner.h"
#include "typing.h"

// Macro to compute array index from error code
#define ERROR_INDEX(base) (((ERROR_BASE - (base)) / ERROR_BLOCK_SIZE))
#define ERROR_MAX ERROR_BASE_MAX - ERROR_BLOCK_SIZE

static const char *const ERROR_MAP[] = {
  [ERROR_INDEX(THRIFT_ERROR_BASE)] = THRIFT_ERROR_NAME,   [ERROR_INDEX(MALLOC_ERROR_BASE)] = MALLOC_ERROR_NAME,
  [ERROR_INDEX(PARQUET_ERROR_BASE)] = PARQUET_ERROR_NAME, [ERROR_INDEX(DOM_ERROR_BASE)] = DOM_ERROR_NAME,
  [ERROR_INDEX(FORMAT_ERROR_BASE)] = FORMAT_ERROR_NAME,   [ERROR_INDEX(ARENA_ERROR_BASE)] = ARENA_ERROR_NAME,
};

const char *res2str(i64 result) {
  // Check if the result is a valid error code
  if (result > ERROR_BASE || result <= ERROR_MAX) {
    return "unknown";
  }

  // Find the corresponding error name
  return ERROR_MAP[ERROR_INDEX(result)];
}

i64 res2off(i64 result) {
  // Check if the result is a valid error code
  if (result > ERROR_BASE || result <= ERROR_MAX) {
    return 0;
  }

  // Convert the error code to an offset
  return -(result) % ERROR_BLOCK_SIZE;
}

#if defined(I13C_TESTS)

static void can_convert_thrift_major() {
  i64 result;
  const char *str;

  // simulate error
  result = THRIFT_ERROR_BASE - 0x05;
  str = res2str(result);

  // assert that the result is converted to the correct string
  assert_eq_str(str, THRIFT_ERROR_NAME, "should convert THRIFT error to string");
}

static void can_convert_thrift_minor() {
  i64 result, offset;

  // simulate error
  result = THRIFT_ERROR_BASE - 0x05;
  offset = res2off(result);

  // assert that the result is converted to the correct offset
  assert(offset == 0x05, "should convert THRIFT error to 0x05");
}

static void can_detect_system_error() {
  i64 result;
  const char *str;

  // simulate system error
  result = -1;
  str = res2str(result);

  // assert that the result is converted to the correct string
  assert_eq_str(str, "unknown", "should convert system error to 'unknown'");
}

static void can_detect_unknown_error() {
  i64 result;
  const char *str;

  // simulate unknown error
  result = -9999;
  str = res2str(result);

  // assert that the result is converted to the correct string
  assert_eq_str(str, "unknown", "should convert unknown error to 'unknown'");
}

void error_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can convert thrift major to string", can_convert_thrift_major);
  test_case(ctx, "can convert thrift minor to offset", can_convert_thrift_minor);
  test_case(ctx, "can detect system error", can_detect_system_error);
  test_case(ctx, "can detect unknown error", can_detect_unknown_error);
}

#endif
