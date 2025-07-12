#include "runner.h"
#include "typing.h"

static void hello_can_world() {
  assert(TRUE, "Should not be called");
}

void hello_test_cases(runner_context *ctx) {
  test_case(ctx, "Hello World", hello_can_world);
}
