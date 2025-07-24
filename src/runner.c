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

void assert_eq_str(const char *actual, const char *expected, const char *msg) {
  while (*actual && *expected) {
    if (*actual++ != *expected++) {
      assert(*actual != *expected, msg);
    }
  }
}

i32 runner_execute() {
  u64 index;
  struct runner_context ctx;

  // register test cases
  ctx.offset = 0;
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
