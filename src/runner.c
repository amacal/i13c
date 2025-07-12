#include "runner.h"
#include "hello.h"
#include "stdout.h"
#include "sys.h"
#include "typing.h"

void test_case(runner_context *ctx, const char *name, void (*execute)(runner_context *ctx)) {
  ctx->entries[ctx->offset].name = name;
  ctx->entries[ctx->offset].execute = execute;
  ctx->offset++;
}

void assert(bool condition, const char *msg) {
  if (!condition) {
    printf(" FAILED: %s\n", msg);
    sys_exit(1);
  }
}

i32 runner_execute() {
  runner_context ctx;

  // register test cases
  ctx.offset = 0;
  hello_test_cases(&ctx);

  // execute all registered test cases
  for (u64 i = 0; i < ctx.offset; i++) {
    printf("Executing '%s' ...", ctx.entries[i].name);
    ctx.entries[i].execute(&ctx);
    printf(" OK\n");
  }

  return 0;
}
