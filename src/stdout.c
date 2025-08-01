#include "stdout.h"
#include "format.h"
#include "sys.h"
#include "typing.h"
#include "vargs.h"

#define BUFFER_SIZE 256
#define BUFFER_TRIGGER 192

static i64 flush_buffer(struct stdout_context *ctx) {
  i64 result;

  while (ctx->buffer_offset > 0) {
    // write the buffer to stdout
    result = sys_write(1, ctx->buffer, ctx->buffer_offset);
    if (result < 0) return result;

    // adjust the length and buffer pointer
    ctx->buffer_offset -= result;
    ctx->buffer += result;
  }

  // success
  return 0;
}

void writef(const char *fmt, ...) {
  void *vargs[VARGS_MAX];
  char buffer[BUFFER_SIZE];

  i64 result;
  struct stdout_context ctx;

  // collect argument list
  vargs_init(vargs);

  // initialize the context
  ctx.fmt = fmt;
  ctx.vargs = vargs;
  ctx.vargs_offset = 0;
  ctx.buffer = buffer;
  ctx.buffer_offset = 0;
  ctx.buffer_size = BUFFER_TRIGGER;

  do {
    // format the string
    result = format(&ctx);

    // ignore the result
    flush_buffer(&ctx);
  } while (result == FORMAT_ERROR_BUFFER_TOO_SMALL);
}
