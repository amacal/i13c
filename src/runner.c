#include "runner.h"
#include "error.h"
#include "format.h"
#include "malloc.h"
#include "parquet.h"
#include "stdout.h"
#include "sys.h"
#include "thrift.h"
#include "typing.h"

#if defined(I13C_TESTS)

void test_case(struct runner_context *ctx, const char *name, void (*execute)(struct runner_context *ctx)) {
  ctx->entries[ctx->offset].name = name;
  ctx->entries[ctx->offset].execute = execute;
  ctx->offset++;
}

void assert(bool condition, const char *msg) {
  if (!condition) {
    writef(" FAILED: %s\n", msg);
    sys_exit(1);
  }
}

static i64 strcmp(const char *actual, const char *expected) {
  // loop until any string reaches the end
  while (*actual != EOS && *expected != EOS) {
    if (*actual != *expected) {
      return -1;
    }

    // move to the next character
    actual++;
    expected++;
  }

  // both strings ended at the same time
  return *actual == *expected ? 0 : -1;
}

void assert_eq_str(const char *actual, const char *expected, const char *msg) {
  assert(strcmp(actual, expected) == 0, msg);
}

static void can_compare_strings() {
  const char *str1 = "Hello, World!";
  const char *str2 = "Hello, World!";
  const char *str3 = "Goodbye, World!";
  const char *str4 = "Hello, World!!";

  // assert that str1 and str2 are equal
  assert(strcmp(str1, str2) == 0, "should be equal");

  // assert that str1 and str3 are not equal
  assert(strcmp(str1, str3) != 0, "should not be equal");

  // assert that strings with common prefix but different length are not equal
  assert(strcmp(str1, str4) != 0, "should not ignore termination");
}

void runner_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can compare strings", can_compare_strings);
}

i32 runner_execute() {
  u64 index;
  struct runner_context ctx;

  // register test cases
  ctx.offset = 0;
  error_test_cases(&ctx);
  malloc_test_cases(&ctx);
  parquet_test_cases(&ctx);
  runner_test_cases(&ctx);
  format_test_cases(&ctx);
  thrift_test_cases(&ctx);

  // execute all registered test cases
  for (index = 0; index < ctx.offset; index++) {
    writef("Executing '%s' ...", ctx.entries[index].name);
    ctx.entries[index].execute(&ctx);
    writef(" OK\n");
  }

  writef("\nAll %d test cases passed.\n", index);
  return 0;
}

#endif
