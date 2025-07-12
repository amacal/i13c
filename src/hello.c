#include "runner.h"
#include "typing.h"

static void hello_can_world() {
  assert(TRUE, "should not fail");
}

void hello_test_cases(runner_context *ctx) {
  test_case(ctx, "Hello World", hello_can_world);
}
