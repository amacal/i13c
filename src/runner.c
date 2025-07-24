#include "runner.h"
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
  while (*actual && *expected) {
    if (*actual++ != *expected++) {
      return -1;
    }

    // even if actual may not be terminated
    if (*expected == EOS) {
      break;
    }
  }

  return 0;
}

void assert_eq_str(const char *actual, const char *expected, const char *msg) {
  assert(strcmp(actual, expected) == 0, msg);
}

static void can_compare_strings() {
  const char *str1 = "Hello, World!";
  const char *str2 = "Hello, World!";
  const char *str3 = "Goodbye, World!";

  // assert that str1 and str2 are equal
  assert(strcmp(str1, str2) == 0, "should be equal");

  // assert that str1 and str3 are not equal
  assert(strcmp(str1, str3) != 0, "should not be equal");
}

void runner_test_cases(struct runner_context *ctx) {
  test_case(ctx, "can compare strings", can_compare_strings);
}

i32 runner_execute() {
  u64 index;
  struct runner_context ctx;

  // register test cases
  ctx.offset = 0;
  runner_test_cases(&ctx);
  malloc_test_cases(&ctx);
  parquet_test_cases(&ctx);
  stdout_test_cases(&ctx);
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
