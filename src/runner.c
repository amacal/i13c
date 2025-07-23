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

i32 runner_execute() {
  struct runner_context ctx;

  // register test cases
  ctx.offset = 0;
  malloc_test_cases(&ctx);
  parquet_test_cases(&ctx);
  thrift_test_cases(&ctx);

  // execute all registered test cases
  for (u64 i = 0; i < ctx.offset; i++) {
    writef("Executing '%s' ...", ctx.entries[i].name);
    ctx.entries[i].execute(&ctx);
    writef(" OK\n");
  }

  return 0;
}

#endif
